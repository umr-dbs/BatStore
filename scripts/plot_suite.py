#!/usr/bin/env python3
"""Plot figures for a full BatStore `benchmark` suite run (see `benchmark` at the
repo root / `src/bat_bench/suite.rs`): TPC-C OLTP-only, CH-benCHmark ("TPC-H"),
HTAP, and YCSB A-F, each run once with GC on and once with GC off, with
per-run memory-usage logging.

Reads `<run_dir>/manifest.csv` (one row per experiment/GC variant, written
incrementally by the suite) plus each experiment subdirectory's raw CSVs
(`tpcc_oltp_timeseries.csv`, `tpcc_scan.csv`, `ycsb_timeseries.csv`,
`mem_stats.csv`), and writes every figure as both PDF and SVG into
`<run_dir>/plots/`.

Quickest start — auto-detects the most recently modified `run_*` directory
under `benchmark_results/` (or wherever `./benchmark <output_root>` was
pointed via `--results-root`):

    python3 scripts/plot_suite.py

Or target a specific run directly:

    python3 scripts/plot_suite.py --run-dir benchmark_results/run_20260101_120000

For plotting a single ad-hoc `tpcc`/`tpch`/`htap`/`ycsb` CLI invocation (not
a full `benchmark` suite run), see `scripts/plot_results.py` instead.

Requires: pandas, matplotlib (see requirements.txt).
"""
import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# The 4 CH-benCHmark queries implemented in bat_bench::tpch_queries, and the
# order they're always run in (bat_bench::olap_scan::ch_benchmark_queries_once)
# — mirrors scripts/plot_results.py's CH_QUERY_ORDER/LABELS.
CH_QUERY_ORDER = [
    "ch_q1_pricing_summary",
    "ch_q6_forecast_revenue",
    "ch_q4_order_priority",
    "ch_q5_revenue_by_nation",
]
CH_QUERY_LABELS = {
    "ch_q1_pricing_summary": "Q1 Pricing Summary",
    "ch_q6_forecast_revenue": "Q6 Forecast Revenue",
    "ch_q4_order_priority": "Q4 Order Priority",
    "ch_q5_revenue_by_nation": "Q5 Revenue by Nation",
}

# Sensible left-to-right/top-to-bottom experiment order for bar charts &
# memory-usage grids — anything not listed here (shouldn't happen) sorts last.
EXPERIMENT_ORDER = ["tpcc_oltp_only", "ch_benchmark", "htap"] + [f"ycsb_{w}" for w in "abcdef"]


def gc_label(gc_enabled) -> str:
    return "GC on" if bool(gc_enabled) else "GC off"


def _experiment_sort_key(name: str):
    return EXPERIMENT_ORDER.index(name) if name in EXPERIMENT_ORDER else len(EXPERIMENT_ORDER)


def _save(fig, out_dir: Path, name: str):
    svg_dir = out_dir / "svg"
    svg_dir.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    path = svg_dir / f"{name}.svg"
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
        raise SystemExit(f"{path} not found — is {run_dir} a `benchmark` suite run directory?")
    df = pd.read_csv(path)
    # Rust's `{}"` on a bool prints lowercase "true"/"false", which pandas's
    # bool-literal inference doesn't always recognize — normalize explicitly
    # rather than relying on dtype inference.
    df["gc_enabled"] = df["gc_enabled"].astype(str).str.lower() == "true"
    return df


def plot_oltp_throughput(run_dir: Path, out_dir: Path):
    """tpcc_oltp_only_gc_{on,off}/tpcc_oltp_timeseries.csv: New-Order
    throughput over time, GC on vs. off overlay."""
    fig, ax = plt.subplots(figsize=(9, 5))
    any_data = False
    for gc in (True, False):
        csv = run_dir / f"tpcc_oltp_only_gc_{'on' if gc else 'off'}" / "tpcc_oltp_timeseries.csv"
        if not csv.exists():
            continue
        df = pd.read_csv(csv)
        ax.plot(df["elapsed_sec"], df["new_order_committed"], marker="o", markersize=3, label=gc_label(gc))
        any_data = True

    if not any_data:
        print("No tpcc_oltp_only data found — skipping OLTP throughput plot.")
        plt.close(fig)
        return

    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel("New-Order transactions / sec")
    ax.set_title("TPC-C OLTP-only throughput: GC on vs. off")
    ax.legend()
    ax.grid(alpha=0.3)
    _save(fig, out_dir, "oltp_throughput_gc_on_vs_off")


def plot_htap_interference(manifest: pd.DataFrame, out_dir: Path):
    """manifest.csv's `htap` rows: mixed (OLTP+OLAP) tpmC vs. the same run's
    OLTP-only baseline tpmC (DriverConfig::htap_baseline, captured via
    TpccRunSummary::baseline_tpm_c), grouped by GC variant."""
    df = manifest[(manifest["experiment"] == "htap") & manifest["baseline_metric_value"].notna()].copy()
    if df.empty:
        print("No htap baseline data in manifest.csv — skipping HTAP interference plot.")
        return
    df = df.sort_values("gc_enabled", ascending=False)  # GC on first
    df["gc_label"] = df["gc_enabled"].map(gc_label)

    fig, ax = plt.subplots(figsize=(7, 5))
    x = range(len(df))
    width = 0.35
    ax.bar([i - width / 2 for i in x], df["baseline_metric_value"], width, label="OLTP only (baseline)", color="tab:blue")
    ax.bar([i + width / 2 for i in x], df["primary_metric_value"], width, label="OLTP + concurrent OLAP", color="tab:orange")
    ax.set_xticks(list(x))
    ax.set_xticklabels(df["gc_label"])
    ax.set_ylabel("tpmC (New-Order / min)")
    ax.set_title("HTAP interference: OLTP throughput with vs. without concurrent OLAP")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, "htap_interference")


def plot_ch_benchmark(run_dir: Path, out_dir: Path):
    """ch_benchmark_gc_{on,off}/tpcc_scan.csv: per-CH-benCHmark-query latency
    and HTAP staleness over time, GC on vs. off overlaid on each panel."""
    colors = {True: "tab:blue", False: "tab:orange"}
    per_gc = {}
    for gc in (True, False):
        csv = run_dir / f"ch_benchmark_gc_{'on' if gc else 'off'}" / "tpcc_scan.csv"
        if not csv.exists():
            continue
        df = pd.read_csv(csv)
        df = df[df["mode"].str.startswith("ch_")]
        if not df.empty:
            per_gc[gc] = df

    if not per_gc:
        print("No CH-benCHmark (ch_*) scan data found — skipping ch_benchmark plot.")
        return

    modes_present = sorted(
        {m for df in per_gc.values() for m in df["mode"].unique()},
        key=lambda m: CH_QUERY_ORDER.index(m) if m in CH_QUERY_ORDER else len(CH_QUERY_ORDER),
    )

    fig, axes = plt.subplots(len(modes_present), 2, figsize=(12, 3.2 * len(modes_present)), squeeze=False)
    for row, mode in enumerate(modes_present):
        label = CH_QUERY_LABELS.get(mode, mode)
        ax_lat, ax_stale = axes[row][0], axes[row][1]
        for gc, df in per_gc.items():
            group = df[df["mode"] == mode].sort_values("elapsed_secs")
            if group.empty:
                continue
            ax_lat.plot(group["elapsed_secs"], group["latency_ns"] / 1e6, marker="o", markersize=3, color=colors[gc], label=gc_label(gc))
            ax_stale.plot(group["elapsed_secs"], group["staleness_versions"], marker="o", markersize=3, color=colors[gc], label=gc_label(gc))
        ax_lat.set_ylabel(f"{label}\nlatency (ms)")
        ax_lat.grid(alpha=0.3)
        ax_stale.set_ylabel("staleness (versions)")
        ax_stale.grid(alpha=0.3)
        if ax_lat.get_legend_handles_labels()[0]:
            ax_lat.legend(fontsize=8)
        if ax_stale.get_legend_handles_labels()[0]:
            ax_stale.legend(fontsize=8)

    for ax in axes[-1]:
        ax.set_xlabel("Elapsed time (s)")
    fig.suptitle("CH-benCHmark query latency & HTAP staleness: GC on vs. off")
    _save(fig, out_dir, "ch_benchmark_latency_staleness")


def plot_ycsb_by_workload(manifest: pd.DataFrame, out_dir: Path):
    """manifest.csv's `ycsb_*` rows: grouped bar chart of throughput per
    workload (A-F), GC on vs. off."""
    df = manifest[manifest["experiment"].str.startswith("ycsb_")].copy()
    if df.empty:
        print("No YCSB rows in manifest.csv — skipping YCSB-by-workload plot.")
        return
    df["workload"] = df["experiment"].str.replace("ycsb_", "", regex=False).str.upper()
    pivot = df.pivot(index="workload", columns="gc_enabled", values="primary_metric_value").sort_index()

    fig, ax = plt.subplots(figsize=(9, 5))
    x = range(len(pivot))
    width = 0.35
    if True in pivot.columns:
        ax.bar([i - width / 2 for i in x], pivot[True], width, label="GC on", color="tab:blue")
    if False in pivot.columns:
        ax.bar([i + width / 2 for i in x], pivot[False], width, label="GC off", color="tab:orange")
    ax.set_xticks(list(x))
    ax.set_xticklabels(pivot.index)
    ax.set_xlabel("YCSB workload")
    ax.set_ylabel("Operations / sec")
    ax.set_title("YCSB throughput by workload: GC on vs. off")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, "ycsb_throughput_by_workload")


def plot_memory_usage(run_dir: Path, manifest: pd.DataFrame, out_dir: Path):
    """Every experiment's own mem_stats.csv: RSS over elapsed time, GC on vs.
    off overlaid per panel — the plot that should show GC actually reclaiming
    memory (a flattening/bounded RSS curve) vs. unbounded growth with GC off."""
    experiments = sorted(manifest["experiment"].unique().tolist(), key=_experiment_sort_key)
    if not experiments:
        print("Empty manifest.csv — skipping memory usage plot.")
        return

    cols = 3
    rows = (len(experiments) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 3.5 * rows), squeeze=False)
    colors = {True: "tab:blue", False: "tab:orange"}

    for idx, experiment in enumerate(experiments):
        ax = axes[idx // cols][idx % cols]
        any_data = False
        for gc in (True, False):
            csv = run_dir / f"{experiment}_gc_{'on' if gc else 'off'}" / "mem_stats.csv"
            if not csv.exists():
                continue
            df = pd.read_csv(csv)
            ax.plot(df["elapsed_sec"], df["rss_kb"] / 1024.0, color=colors[gc], label=gc_label(gc))
            any_data = True
        ax.set_title(experiment, fontsize=10)
        ax.set_xlabel("Elapsed (s)")
        ax.set_ylabel("RSS (MB)")
        ax.grid(alpha=0.3)
        if any_data:
            ax.legend(fontsize=8)

    for idx in range(len(experiments), rows * cols):
        axes[idx // cols][idx % cols].axis("off")

    fig.suptitle("Memory usage (RSS) over time: GC on vs. off, per experiment")
    _save(fig, out_dir, "memory_usage_gc_on_vs_off")


def plot_summary_all(manifest: pd.DataFrame, out_dir: Path):
    """manifest.csv, every experiment: one consolidated bar chart of each
    experiment's own primary throughput metric (tpmC for TPC-C/CH/HTAP,
    ops/sec for YCSB — different units, log-scaled y-axis purely so every
    experiment is visible on one chart; not meant for cross-experiment
    magnitude comparison)."""
    df = manifest.copy()
    pivot = df.pivot(index="experiment", columns="gc_enabled", values="primary_metric_value")
    pivot = pivot.reindex(sorted(pivot.index, key=_experiment_sort_key))

    fig, ax = plt.subplots(figsize=(10, 5))
    x = range(len(pivot))
    width = 0.35
    if True in pivot.columns:
        ax.bar([i - width / 2 for i in x], pivot[True], width, label="GC on", color="tab:blue")
    if False in pivot.columns:
        ax.bar([i + width / 2 for i in x], pivot[False], width, label="GC off", color="tab:orange")
    ax.set_xticks(list(x))
    ax.set_xticklabels(pivot.index, rotation=30, ha="right")
    ax.set_ylabel("Primary throughput metric (tpmC or ops/sec — see manifest.csv)")
    ax.set_yscale("log")
    ax.set_title("All experiments: primary throughput, GC on vs. off (log scale)")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, "summary_all_experiments")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-dir", help="A specific run_YYYYmmdd_HHMMSS directory (default: auto-detect the most recently modified one under --results-root)")
    parser.add_argument("--results-root", default="benchmark_results", help="Where to look for run_* directories when --run-dir isn't given (default: benchmark_results)")
    args = parser.parse_args()

    if args.run_dir:
        run_dir = Path(args.run_dir)
        if not run_dir.exists():
            raise SystemExit(f"{run_dir} does not exist")
    else:
        run_dir = find_latest_run_dir(Path(args.results_root))

    print(f"Plotting benchmark suite results from {run_dir}")
    manifest = load_manifest(run_dir)
    out_dir = run_dir / "plots"

    plot_oltp_throughput(run_dir, out_dir)
    plot_htap_interference(manifest, out_dir)
    plot_ch_benchmark(run_dir, out_dir)
    plot_ycsb_by_workload(manifest, out_dir)
    plot_memory_usage(run_dir, manifest, out_dir)
    plot_summary_all(manifest, out_dir)

    print(f"\nAll figures written to {out_dir}")


if __name__ == "__main__":
    main()
