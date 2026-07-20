#!/usr/bin/env python3
"""Plot figures for a scripts/compare_engines.py cross-engine comparison run:
TPC-C and YCSB A-F throughput, and peak memory, overlaid across
cMVBT/LeanStore/WiredTiger/PostgreSQL.

Reads <run_dir>/manifest.csv (one row per engine/workload combo, written
incrementally by compare_engines.py) and writes every figure as both PDF and
SVG into <run_dir>/plots/.

    python3 scripts/plot_compare.py
    python3 scripts/plot_compare.py --run-dir comparison_results/run_20260101_120000

Note: PostgreSQL's peak_rss_mb is always 0 (see engines/postgres_benchbase.py's
docstring for why) - the memory plot below excludes it rather than showing a
misleading zero bar.

Requires: pandas, matplotlib (see requirements.txt).
"""
import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ENGINE_ORDER = ["cmvbt", "leanstore", "wiredtiger", "postgres"]
ENGINE_LABELS = {"cmvbt": "cMVBT", "leanstore": "LeanStore", "wiredtiger": "WiredTiger", "postgres": "PostgreSQL"}
ENGINE_COLORS = {"cmvbt": "tab:green", "leanstore": "tab:blue", "wiredtiger": "tab:orange", "postgres": "tab:red"}
YCSB_WORKLOADS = [f"ycsb_{w}" for w in "abcdef"]


def _engine_sort_key(name: str):
    return ENGINE_ORDER.index(name) if name in ENGINE_ORDER else len(ENGINE_ORDER)


def _save(fig, out_dir: Path, name: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    for ext in ("pdf", "svg"):
        path = out_dir / f"{name}.{ext}"
        fig.savefig(path)
        print(f"Wrote {path}")
    plt.close(fig)


def find_latest_run_dir(search_root: Path) -> Path:
    if not search_root.exists():
        raise SystemExit(f"{search_root} does not exist — pass --run-dir explicitly")
    candidates = sorted(search_root.glob("run_*"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise SystemExit(f"No run_* directories found under {search_root}")
    return candidates[-1]


def load_manifest(run_dir: Path) -> pd.DataFrame:
    path = run_dir / "manifest.csv"
    if not path.exists():
        raise SystemExit(f"{path} not found — is {run_dir} a compare_engines.py run directory?")
    df = pd.read_csv(path)
    df["notes"] = df["notes"].fillna("")
    df["failed"] = df["notes"].str.startswith("FAILED") | df["notes"].str.startswith("EXCEPTION")
    return df


def _bar_by_engine(ax, df: pd.DataFrame, value_col: str):
    engines_present = sorted(df["engine"].unique(), key=_engine_sort_key)
    values = [df.loc[df["engine"] == e, value_col].iloc[0] if e in df["engine"].values else 0 for e in engines_present]
    colors = [ENGINE_COLORS.get(e, "tab:gray") for e in engines_present]
    bars = ax.bar(range(len(engines_present)), values, color=colors)
    ax.set_xticks(range(len(engines_present)))
    ax.set_xticklabels([ENGINE_LABELS.get(e, e) for e in engines_present])
    for bar, e in zip(bars, engines_present):
        failed = df.loc[df["engine"] == e, "failed"]
        if not failed.empty and failed.iloc[0]:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), "FAILED",
                    ha="center", va="bottom", color="red", fontsize=8)


def plot_tpcc_throughput(manifest: pd.DataFrame, out_dir: Path):
    df = manifest[manifest["workload"] == "tpcc"]
    if df.empty:
        print("No tpcc rows in manifest.csv — skipping TPC-C throughput plot.")
        return
    fig, ax = plt.subplots(figsize=(7, 5))
    _bar_by_engine(ax, df, "primary_metric_value")
    ax.set_ylabel("New-Order transactions / sec")
    ax.set_title("TPC-C throughput by engine")
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, "tpcc_throughput_by_engine")


def plot_ycsb_throughput(manifest: pd.DataFrame, out_dir: Path):
    df = manifest[manifest["workload"].isin(YCSB_WORKLOADS)].copy()
    if df.empty:
        print("No YCSB rows in manifest.csv — skipping YCSB throughput plot.")
        return
    df["workload_label"] = df["workload"].str.replace("ycsb_", "", regex=False).str.upper()
    engines_present = sorted(df["engine"].unique(), key=_engine_sort_key)
    workloads_present = sorted(df["workload_label"].unique())

    fig, ax = plt.subplots(figsize=(11, 5))
    n = len(engines_present)
    width = 0.8 / max(n, 1)
    x = range(len(workloads_present))
    for i, engine in enumerate(engines_present):
        sub = df[df["engine"] == engine].set_index("workload_label")
        values = [sub["primary_metric_value"].get(w, 0) for w in workloads_present]
        offsets = [xi + (i - (n - 1) / 2) * width for xi in x]
        ax.bar(offsets, values, width, label=ENGINE_LABELS.get(engine, engine), color=ENGINE_COLORS.get(engine, "tab:gray"))

    ax.set_xticks(list(x))
    ax.set_xticklabels(workloads_present)
    ax.set_xlabel("YCSB workload")
    ax.set_ylabel("Operations / sec")
    ax.set_title("YCSB throughput by workload and engine")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, "ycsb_throughput_by_engine")


def plot_memory_usage(manifest: pd.DataFrame, out_dir: Path):
    """Peak RSS by engine, per workload — PostgreSQL excluded (see module docstring)."""
    df = manifest[manifest["engine"] != "postgres"].copy()
    if df.empty:
        print("No non-Postgres rows in manifest.csv — skipping memory plot.")
        return
    workloads = sorted(df["workload"].unique(), key=lambda w: (w != "tpcc", w))

    cols = 3
    rows = (len(workloads) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(4.5 * cols, 3.5 * rows), squeeze=False)
    for idx, workload in enumerate(workloads):
        ax = axes[idx // cols][idx % cols]
        _bar_by_engine(ax, df[df["workload"] == workload], "peak_rss_mb")
        ax.set_title(workload, fontsize=10)
        ax.set_ylabel("Peak RSS (MB)")
        ax.grid(alpha=0.3, axis="y")
    for idx in range(len(workloads), rows * cols):
        axes[idx // cols][idx % cols].axis("off")

    fig.suptitle("Peak memory usage by engine (PostgreSQL not tracked, see docstring)")
    _save(fig, out_dir, "memory_by_engine")


def plot_summary_all(manifest: pd.DataFrame, out_dir: Path):
    """Every workload x engine combo's primary metric, log-scaled purely so
    TPC-C and YCSB (different units/magnitudes) fit on one chart."""
    df = manifest.copy()
    workloads = sorted(df["workload"].unique(), key=lambda w: (w != "tpcc", w))
    engines_present = sorted(df["engine"].unique(), key=_engine_sort_key)

    fig, ax = plt.subplots(figsize=(12, 5))
    n = len(engines_present)
    width = 0.8 / max(n, 1)
    x = range(len(workloads))
    for i, engine in enumerate(engines_present):
        sub = df[df["engine"] == engine].set_index("workload")
        values = [max(sub["primary_metric_value"].get(w, 0), 0.01) for w in workloads]
        offsets = [xi + (i - (n - 1) / 2) * width for xi in x]
        ax.bar(offsets, values, width, label=ENGINE_LABELS.get(engine, engine), color=ENGINE_COLORS.get(engine, "tab:gray"))

    ax.set_xticks(list(x))
    ax.set_xticklabels(workloads, rotation=30, ha="right")
    ax.set_ylabel("Primary throughput metric (New-Order/sec or ops/sec)")
    ax.set_yscale("log")
    ax.set_title("All workloads: primary throughput by engine (log scale)")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, "summary_all_workloads")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-dir", help="A specific run_YYYYmmdd_HHMMSS directory (default: auto-detect the most recently modified one under --results-root)")
    parser.add_argument("--results-root", default="comparison_results", help="Where to look for run_* directories when --run-dir isn't given")
    args = parser.parse_args()

    run_dir = Path(args.run_dir) if args.run_dir else find_latest_run_dir(Path(args.results_root))
    if args.run_dir and not run_dir.exists():
        raise SystemExit(f"{run_dir} does not exist")

    print(f"Plotting cross-engine comparison results from {run_dir}")
    manifest = load_manifest(run_dir)
    out_dir = run_dir / "plots"

    plot_tpcc_throughput(manifest, out_dir)
    plot_ycsb_throughput(manifest, out_dir)
    plot_memory_usage(manifest, out_dir)
    plot_summary_all(manifest, out_dir)

    print(f"\nAll figures written to {out_dir}")


if __name__ == "__main__":
    main()
