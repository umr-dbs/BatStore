#!/usr/bin/env python3
"""Plot figures for a scripts/compare_engines.py cross-engine comparison run:
TPC-C and YCSB A-F throughput, peak memory, throughput-vs-threads, GC on/off
comparison, and scan/OLAP latency, overlaid across
cMVBT/LeanStore/WiredTiger/PostgreSQL.

Reads <run_dir>/manifest.csv (one row per engine/workload/threads/gc combo,
written incrementally by compare_engines.py) and writes every figure as both
PDF and SVG into <run_dir>/plots/.

    python3 scripts/plot_compare.py
    python3 scripts/plot_compare.py --run-dir comparison_results/run_20260101_120000

Several plots (TPC-C/YCSB throughput bars, memory, summary-all) need exactly
one row per engine/workload to draw a bar chart, but the manifest now has
many rows per engine/workload (one per threads x gc combo). Those plots use
a "reference slice": the largest threads value present, and gc="on" for
engines that support toggling GC, "n/a" for the ones that don't (never
"off") - see pick_reference_slice(). Every plot's title/filename says which
thread count it's pinned to, since it's no longer the only data for that
engine/workload in the manifest.

Note: PostgreSQL's peak_rss_mb is always 0 (see engines/postgres_benchbase.py's
docstring for why) - the memory plot excludes it rather than showing a
misleading zero bar.

Requires: pandas, matplotlib (see requirements.txt).
"""
import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ENGINE_ORDER = ["cmvbt", "leanstore", "wiredtiger", "postgres", "vweaver_ermia", "libmdbx"]
ENGINE_LABELS = {
    "cmvbt": "cMVBT", "leanstore": "LeanStore", "wiredtiger": "WiredTiger", "postgres": "PostgreSQL",
    "vweaver_ermia": "vWeaver/ERMIA", "libmdbx": "libmdbx",
}
ENGINE_COLORS = {
    "cmvbt": "tab:green", "leanstore": "tab:blue", "wiredtiger": "tab:orange", "postgres": "tab:red",
    "vweaver_ermia": "tab:purple", "libmdbx": "tab:brown",
}
YCSB_WORKLOADS = [f"ycsb_{w}" for w in "abcdef"]
# Engines with a real, working GC on/off toggle (see engines/*.py's SUPPORTS_GC_TOGGLE) -
# leanstore/wiredtiger only ever report gc_enabled="n/a" (no working toggle in this
# checkout, see the plan's Context section), so they're excluded from GC-comparison plots.
GC_TOGGLE_ENGINES = ["cmvbt", "postgres"]


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
    df["gc_enabled"] = df["gc_enabled"].fillna("n/a")
    return df


def pick_reference_slice(manifest: pd.DataFrame) -> tuple:
    """One row per engine/workload: the largest threads value present, and gc="on" for
    engines that support toggling GC, "n/a" for the ones that don't - never "off", so a
    bar chart never silently shows the GC-disabled number as if it were the default.
    """
    if manifest.empty:
        return manifest, 0
    ref_threads = int(manifest["threads"].max())
    slice_df = manifest[(manifest["threads"] == ref_threads) & (manifest["gc_enabled"] != "off")]
    return slice_df, ref_threads


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


def plot_tpcc_throughput(ref_slice: pd.DataFrame, ref_threads: int, out_dir: Path):
    df = ref_slice[ref_slice["workload"] == "tpcc"]
    if df.empty:
        print("No tpcc rows in manifest.csv — skipping TPC-C throughput plot.")
        return
    fig, ax = plt.subplots(figsize=(7, 5))
    _bar_by_engine(ax, df, "primary_metric_value")
    ax.set_ylabel("New-Order transactions / sec")
    ax.set_title(f"TPC-C throughput by engine (threads={ref_threads}, gc=on/n/a)")
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, "tpcc_throughput_by_engine")


def plot_ycsb_throughput(ref_slice: pd.DataFrame, ref_threads: int, out_dir: Path):
    df = ref_slice[ref_slice["workload"].isin(YCSB_WORKLOADS)].copy()
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
    ax.set_title(f"YCSB throughput by workload and engine (threads={ref_threads}, gc=on/n/a)")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, "ycsb_throughput_by_engine")


def plot_memory_usage(ref_slice: pd.DataFrame, ref_threads: int, out_dir: Path):
    """Peak RSS by engine, per workload — PostgreSQL excluded (see module docstring)."""
    df = ref_slice[ref_slice["engine"] != "postgres"].copy()
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

    fig.suptitle(f"Peak memory usage by engine (threads={ref_threads}, gc=on/n/a; PostgreSQL not tracked)")
    _save(fig, out_dir, "memory_by_engine")


def plot_summary_all(ref_slice: pd.DataFrame, ref_threads: int, out_dir: Path):
    """Every workload x engine combo's primary metric, log-scaled purely so
    TPC-C and YCSB (different units/magnitudes) fit on one chart."""
    df = ref_slice.copy()
    if df.empty:
        print("No rows in manifest.csv — skipping summary-all plot.")
        return
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
    ax.set_title(f"All workloads: primary throughput by engine (log scale, threads={ref_threads}, gc=on/n/a)")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, "summary_all_workloads")


def plot_throughput_vs_threads(manifest: pd.DataFrame, out_dir: Path):
    """One figure per workload: x=threads, y=primary_metric_value, one line per
    engine/gc_enabled combo (solid for gc=on/n/a, dashed for gc=off) - the whole point of
    the thread sweep, so results can be read directly off an x-axis of thread count."""
    for workload in sorted(manifest["workload"].unique(), key=lambda w: (w != "tpcc", w)):
        df = manifest[manifest["workload"] == workload]
        if df.empty:
            continue
        fig, ax = plt.subplots(figsize=(8, 5.5))
        for engine in sorted(df["engine"].unique(), key=_engine_sort_key):
            edf = df[df["engine"] == engine]
            for gc_variant, linestyle in (("on", "-"), ("n/a", "-"), ("off", "--")):
                gdf = edf[edf["gc_enabled"] == gc_variant].sort_values("threads")
                if gdf.empty:
                    continue
                label = ENGINE_LABELS.get(engine, engine)
                if gc_variant == "off":
                    label += " (gc off)"
                ax.plot(gdf["threads"], gdf["primary_metric_value"], marker="o", linestyle=linestyle,
                        label=label, color=ENGINE_COLORS.get(engine, "tab:gray"))
        ax.set_xscale("log", base=2)
        ax.set_xlabel("Threads / terminals")
        ax.set_ylabel(df["primary_metric_name"].iloc[0])
        ax.set_title(f"{workload}: throughput vs. thread count")
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)
        _save(fig, out_dir, f"threads_sweep_{workload}")


def plot_gc_comparison(manifest: pd.DataFrame, ref_threads: int, out_dir: Path):
    """Grouped gc=on vs gc=off bars, one subplot per workload - only cmvbt/postgres have a
    real toggle (see GC_TOGGLE_ENGINES), at the reference thread count."""
    df = manifest[
        (manifest["engine"].isin(GC_TOGGLE_ENGINES))
        & (manifest["threads"] == ref_threads)
        & (manifest["gc_enabled"].isin(["on", "off"]))
    ]
    if df.empty:
        print("No gc=on/off rows for cmvbt/postgres — skipping GC comparison plot.")
        return
    workloads = sorted(df["workload"].unique(), key=lambda w: (w != "tpcc", w))
    cols = 3
    rows = (len(workloads) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(4.5 * cols, 3.5 * rows), squeeze=False)
    for idx, workload in enumerate(workloads):
        ax = axes[idx // cols][idx % cols]
        wdf = df[df["workload"] == workload]
        engines_present = sorted(wdf["engine"].unique(), key=_engine_sort_key)
        width = 0.35
        x = range(len(engines_present))
        on_vals = [wdf[(wdf["engine"] == e) & (wdf["gc_enabled"] == "on")]["primary_metric_value"].sum() for e in engines_present]
        off_vals = [wdf[(wdf["engine"] == e) & (wdf["gc_enabled"] == "off")]["primary_metric_value"].sum() for e in engines_present]
        ax.bar([xi - width / 2 for xi in x], on_vals, width, label="gc=on", color="tab:blue")
        ax.bar([xi + width / 2 for xi in x], off_vals, width, label="gc=off", color="tab:red")
        ax.set_xticks(list(x))
        ax.set_xticklabels([ENGINE_LABELS.get(e, e) for e in engines_present])
        ax.set_title(workload, fontsize=10)
        ax.grid(alpha=0.3, axis="y")
        if idx == 0:
            ax.legend(fontsize=8)
    for idx in range(len(workloads), rows * cols):
        axes[idx // cols][idx % cols].axis("off")
    fig.suptitle(f"GC on vs. off throughput (threads={ref_threads}; leanstore/wiredtiger have no working "
                 f"GC toggle in this checkout, not shown)")
    _save(fig, out_dir, "gc_on_vs_off")


def plot_scan_latency(manifest: pd.DataFrame, ref_threads: int, out_dir: Path):
    """YCSB-E scan-op latency (p50/p95/p99), one group of bars per engine, at the
    reference thread count - the one workload instrumented for real scan latency across
    all four engines (see the plan's Context section)."""
    df = manifest[
        (manifest["workload"] == "ycsb_e") & (manifest["threads"] == ref_threads)
        & (manifest["gc_enabled"] != "off") & (manifest["scan_count"] > 0)
    ]
    if df.empty:
        print("No ycsb_e scan-latency rows — skipping scan-latency plot.")
        return
    engines_present = sorted(df["engine"].unique(), key=_engine_sort_key)
    fig, ax = plt.subplots(figsize=(8, 5))
    width = 0.25
    x = range(len(engines_present))
    for i, (col, label) in enumerate([("scan_p50_us", "p50"), ("scan_p95_us", "p95"), ("scan_p99_us", "p99")]):
        values = [df[df["engine"] == e][col].iloc[0] if e in df["engine"].values else 0 for e in engines_present]
        offsets = [xi + (i - 1) * width for xi in x]
        ax.bar(offsets, values, width, label=label)
    ax.set_xticks(list(x))
    ax.set_xticklabels([ENGINE_LABELS.get(e, e) for e in engines_present])
    ax.set_ylabel("Scan-op latency (microseconds)")
    ax.set_title(f"YCSB-E scan latency by engine (threads={ref_threads})")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, "ycsb_e_scan_latency")


def plot_htap_interference(manifest: pd.DataFrame, ref_threads: int, out_dir: Path):
    """HTAP interference%: how much running one CH-benCHmark analytical query
    concurrently (htap_q1/htap_q6) drops OLTP throughput vs. plain "tpcc" at the same
    threads/gc - derived purely from already-collected rows, no separate baseline
    sub-phase needed. Positive % = OLTP got slower under concurrent analytics."""
    df = manifest[(manifest["threads"] == ref_threads) & (manifest["gc_enabled"] != "off")]
    tpcc = df[df["workload"] == "tpcc"].set_index("engine")["primary_metric_value"]
    if tpcc.empty:
        print("No tpcc rows at the reference thread count — skipping HTAP interference plot.")
        return
    engines_present = sorted(tpcc.index.unique(), key=_engine_sort_key)
    fig, ax = plt.subplots(figsize=(8, 5))
    width = 0.35
    x = range(len(engines_present))
    for i, workload in enumerate(("htap_q1", "htap_q6")):
        wdf = df[df["workload"] == workload].set_index("engine")["primary_metric_value"]
        pct = []
        for e in engines_present:
            baseline = tpcc.get(e, 0.0)
            htap = wdf.get(e, None)
            pct.append(0.0 if htap is None or not baseline else (baseline - htap) / baseline * 100.0)
        offsets = [xi + (i - 0.5) * width for xi in x]
        ax.bar(offsets, pct, width, label=workload)
    ax.set_xticks(list(x))
    ax.set_xticklabels([ENGINE_LABELS.get(e, e) for e in engines_present])
    ax.set_ylabel("OLTP throughput drop vs. plain TPC-C (%)")
    ax.set_title(f"HTAP interference: OLTP slowdown under concurrent CH-benCHmark analytics (threads={ref_threads})")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, "htap_interference")


def plot_ch_query_latency(manifest: pd.DataFrame, ref_threads: int, out_dir: Path):
    """CH-benCHmark Q1/Q6 analytical-query latency (p50/p95/p99), one subplot per query,
    across all 4 engines - at the reference thread count."""
    df = manifest[
        (manifest["workload"].isin(["htap_q1", "htap_q6"])) & (manifest["threads"] == ref_threads)
        & (manifest["gc_enabled"] != "off") & (manifest["scan_count"] > 0)
    ]
    if df.empty:
        print("No htap_q1/htap_q6 latency rows — skipping CH query latency plot.")
        return
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), squeeze=False)
    for idx, (workload, title) in enumerate([("htap_q1", "Q1 (Pricing Summary)"), ("htap_q6", "Q6 (Forecast Revenue)")]):
        ax = axes[0][idx]
        wdf = df[df["workload"] == workload]
        if wdf.empty:
            ax.axis("off")
            continue
        engines_present = sorted(wdf["engine"].unique(), key=_engine_sort_key)
        width = 0.25
        x = range(len(engines_present))
        for i, (col, label) in enumerate([("scan_p50_us", "p50"), ("scan_p95_us", "p95"), ("scan_p99_us", "p99")]):
            values = [wdf[wdf["engine"] == e][col].iloc[0] if e in wdf["engine"].values else 0 for e in engines_present]
            offsets = [xi + (i - 1) * width for xi in x]
            ax.bar(offsets, values, width, label=label)
        ax.set_xticks(list(x))
        ax.set_xticklabels([ENGINE_LABELS.get(e, e) for e in engines_present])
        ax.set_ylabel("Query latency (microseconds)")
        ax.set_title(title)
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3, axis="y")
    fig.suptitle(f"CH-benCHmark analytical query latency by engine (threads={ref_threads})")
    _save(fig, out_dir, "ch_query_latency")


def plot_cmvbt_olap_scan_latency(run_dir: Path, ref_threads: int, out_dir: Path):
    """cMVBT-specific: its TPC-C run always includes a concurrent HTAP scan-sweep OLAP
    thread (see tpcc_driver.rs's olap_mode_str default), writing raw per-scan latency vs.
    the scan's staleness/delay to tpcc_scan.csv. Not comparable to the other 3 engines'
    plain-OLTP TPC-C in this harness, so it's its own plot rather than a 4-way bar."""
    candidates = sorted(run_dir.glob(f"tpcc/cmvbt/threads_{ref_threads}/gc_*/tpcc_scan.csv"))
    if not candidates:
        print(f"No tpcc_scan.csv found for cmvbt at threads={ref_threads} — skipping cMVBT OLAP-scan-latency plot.")
        return
    # Prefer gc=on (the default/representative variant) if present.
    scan_csv = next((c for c in candidates if "gc_on" in c.parts), candidates[0])
    df = pd.read_csv(scan_csv)
    if df.empty:
        print(f"{scan_csv} is empty — skipping cMVBT OLAP-scan-latency plot.")
        return
    df["latency_us"] = df["latency_ns"] / 1000.0

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(df["delay_secs"], df["latency_us"], marker="o", color="tab:green")
    ax.set_xlabel("OLAP scan delay / staleness target (seconds)")
    ax.set_ylabel("Scan latency (microseconds)")
    ax.set_title(f"cMVBT: TPC-C concurrent OLAP-scan latency vs. staleness (threads={ref_threads})")
    ax.grid(alpha=0.3)
    _save(fig, out_dir, "cmvbt_tpcc_olap_scan_latency")


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
    ref_slice, ref_threads = pick_reference_slice(manifest)
    out_dir = run_dir / "plots"

    plot_tpcc_throughput(ref_slice, ref_threads, out_dir)
    plot_ycsb_throughput(ref_slice, ref_threads, out_dir)
    plot_memory_usage(ref_slice, ref_threads, out_dir)
    plot_summary_all(ref_slice, ref_threads, out_dir)
    plot_throughput_vs_threads(manifest, out_dir)
    plot_gc_comparison(manifest, ref_threads, out_dir)
    plot_scan_latency(manifest, ref_threads, out_dir)
    plot_cmvbt_olap_scan_latency(run_dir, ref_threads, out_dir)
    plot_htap_interference(manifest, ref_threads, out_dir)
    plot_ch_query_latency(manifest, ref_threads, out_dir)

    print(f"\nAll figures written to {out_dir}")


if __name__ == "__main__":
    main()
