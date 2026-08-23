#!/usr/bin/env python3
"""Plot figures for a scripts/compare_engines.py benchmark run:
TPC-C and YCSB A-F throughput, peak memory, throughput-vs-threads, GC on/off
comparison, and scan/OLAP latency, overlaid across
BatStore/LeanStore/WiredTiger/PostgreSQL/vWeaver/libmdbx variants.

A run containing only one engine gets a workload-by-metric overview instead of
cross-engine comparison charts.

Reads <run_dir>/manifest.csv (one row per engine/workload/threads/gc combo,
written incrementally by compare_engines.py) and writes every figure as both
PDF and SVG into <run_dir>/plots/.

    python3 scripts/plot_compare.py
    python3 scripts/plot_compare.py --run-dir comparison_results/run_20260101_120000

Several plots (TPC-C/YCSB throughput bars, memory, summary-all) need exactly
one row per engine/workload to draw a bar chart, but the manifest now has
many rows per engine/workload (one per threads x gc combo). Those plots use
a "reference slice": the largest threads value present, and gc=<choice> for
engines that support toggling GC, "n/a" for the ones that don't - see
pick_reference_slice(). Each of these 4 plots is generated ONCE PER gc
choice ("on" and "off"), as a separate file (name/title suffixed
"_gc_on"/"_gc_off"), so a single chart never mixes one engine's gc=on bar
with another engine's gc=off bar - the two files are directly comparable
side by side instead. Engines without a real toggle (gc_enabled="n/a")
show the same bar in both files, since there's no separate on/off state
for them. Every plot's title/filename also says which thread count it's
pinned to, since it's no longer the only data for that engine/workload in
the manifest.

The threads-sweep plots (x=threads, y=throughput, one line per engine - the whole point
of compare_engines.py's thread sweep) are split by workload group rather than one giant
figure or one-file-per-workload: threads_sweep_tpcc.svg (TPC-C, its own figure),
threads_sweep_ycsb.svg (ONE figure, all loaded YCSB A-F workloads as subplots, so the
whole YCSB sweep reads off a single file), and threads_sweep_htap_q1.svg /
threads_sweep_htap_q6.svg. Each HTAP figure has separate OLTP-throughput and
OLAP-throughput panels; see plot_throughput_vs_threads_htap's docstring.

Requires: pandas, matplotlib (see requirements.txt).
"""
import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D

ENGINE_ORDER = [
    "batstore", "leanstore", "wiredtiger", "postgres", "vweaver_ermia",
    "vweaver_ermia_frugal", "libmdbx",
]
ENGINE_LABELS = {
    "batstore": "BatStore", "leanstore": "LeanStore", "wiredtiger": "WiredTiger", "postgres": "PostgreSQL",
    "vweaver_ermia": "vWeaver/ERMIA", "vweaver_ermia_frugal": "Frugal/ERMIA",
    "libmdbx": "libmdbx",
}
ENGINE_COLORS = {
    "batstore": "tab:green", "leanstore": "tab:blue", "wiredtiger": "tab:orange", "postgres": "tab:red",
    "vweaver_ermia": "tab:purple", "vweaver_ermia_frugal": "tab:pink", "libmdbx": "tab:brown",
}
YCSB_WORKLOADS = [f"ycsb_{w}" for w in "abcdef"]
HTAP_WORKLOADS = ["htap_q1", "htap_q6"]
# Engines with a real, working GC on/off toggle (see engines/*.py's SUPPORTS_GC_TOGGLE) -
# leanstore/wiredtiger only ever report gc_enabled="n/a" (no working toggle in this
# checkout, see the plan's Context section), so they're excluded from GC-comparison plots.
GC_TOGGLE_ENGINES = ["batstore", "postgres", "vweaver_ermia", "vweaver_ermia_frugal"]


def _engine_sort_key(name: str):
    return ENGINE_ORDER.index(name) if name in ENGINE_ORDER else len(ENGINE_ORDER)


def _save(fig, out_dir: Path, name: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    for suffix in ("svg", "pdf"):
        path = out_dir / f"{name}.{suffix}"
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
    # Older manifests used the pre-rename engine key. Normalize them at the
    # reader boundary so historical comparison runs remain plottable.
    df["engine"] = df["engine"].replace({"cmvbt": "batstore"})
    df["notes"] = df["notes"].fillna("")
    df["failed"] = df["notes"].str.startswith(("FAILED", "TIMEOUT", "EXCEPTION"))
    df["gc_enabled"] = df["gc_enabled"].fillna("n/a")
    return df


def pick_reference_slice(manifest: pd.DataFrame, gc_choice: str) -> tuple:
    """One row per engine/workload: the largest threads value present, and gc=gc_choice
    for engines that support toggling GC, "n/a" for the ones that don't (they have no
    separate on/off state, so the same row appears in both the "on" and "off" slices).
    Called once per gc_choice ("on"/"off") so each resulting chart is entirely one GC
    state, never a mix of one engine's "on" bar next to another engine's "off" bar.
    """
    if manifest.empty:
        return manifest, 0
    ref_threads = int(manifest["threads"].max())
    slice_df = manifest[(manifest["threads"] == ref_threads) & (manifest["gc_enabled"].isin([gc_choice, "n/a"]))]
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


def plot_tpcc_throughput(ref_slice: pd.DataFrame, ref_threads: int, gc_choice: str, out_dir: Path):
    df = ref_slice[ref_slice["workload"] == "tpcc"]
    if df.empty:
        print(f"No tpcc rows in manifest.csv for gc={gc_choice} — skipping TPC-C throughput plot.")
        return
    fig, ax = plt.subplots(figsize=(7, 5))
    _bar_by_engine(ax, df, "primary_metric_value")
    ax.set_ylabel("New-Order transactions / sec")
    ax.set_title(f"TPC-C throughput by engine (threads={ref_threads}, gc={gc_choice})")
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, f"tpcc_throughput_by_engine_gc_{gc_choice}")


def plot_ycsb_throughput(ref_slice: pd.DataFrame, ref_threads: int, gc_choice: str, out_dir: Path):
    df = ref_slice[ref_slice["workload"].isin(YCSB_WORKLOADS)].copy()
    if df.empty:
        print(f"No YCSB rows in manifest.csv for gc={gc_choice} — skipping YCSB throughput plot.")
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
    ax.set_title(f"YCSB throughput by workload and engine (threads={ref_threads}, gc={gc_choice})")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, f"ycsb_throughput_by_engine_gc_{gc_choice}")


def plot_memory_usage(ref_slice: pd.DataFrame, ref_threads: int, gc_choice: str, out_dir: Path):
    """Peak RSS by engine, per workload."""
    df = ref_slice.copy()
    if df.empty:
        print(f"No rows in manifest.csv for gc={gc_choice} — skipping memory plot.")
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

    fig.suptitle(f"Peak memory usage by engine (threads={ref_threads}, gc={gc_choice})")
    _save(fig, out_dir, f"memory_by_engine_gc_{gc_choice}")


def plot_summary_all(ref_slice: pd.DataFrame, ref_threads: int, gc_choice: str, out_dir: Path):
    """Every workload x engine combo's primary metric, log-scaled purely so
    TPC-C and YCSB (different units/magnitudes) fit on one chart."""
    df = ref_slice.copy()
    if df.empty:
        print(f"No rows in manifest.csv for gc={gc_choice} — skipping summary-all plot.")
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
    ax.set_title(f"All workloads: primary throughput by engine (log scale, threads={ref_threads}, gc={gc_choice})")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    _save(fig, out_dir, f"summary_all_workloads_gc_{gc_choice}")


def _plot_engine_lines(ax, df: pd.DataFrame, value_col: str = "primary_metric_value"):
    """x=threads, y=value_col, one line per engine/gc_enabled combo (solid for
    gc=on/n/a, dashed for gc=off) drawn onto `ax` - the shared building block behind every
    threads_sweep_* figure below, so a single workload's worth of lines can be placed
    either on its own figure (TPC-C, HTAP) or as one subplot among several (YCSB)."""
    for engine in sorted(df["engine"].unique(), key=_engine_sort_key):
        edf = df[df["engine"] == engine]
        for gc_variant, linestyle in (("on", "-"), ("n/a", "-"), ("off", "--")):
            gdf = edf[edf["gc_enabled"] == gc_variant].sort_values("threads")
            if gdf.empty:
                continue
            label = ENGINE_LABELS.get(engine, engine)
            if gc_variant == "off":
                label += " (gc off)"
            ax.plot(gdf["threads"], gdf[value_col], marker="o", linestyle=linestyle,
                    label=label, color=ENGINE_COLORS.get(engine, "tab:gray"))
    ax.set_xscale("log", base=2)
    # Tick locations/labels pinned to the actual thread counts in `df` (e.g. 2,4,8,...128)
    # rather than matplotlib's default log-scale formatter, which would otherwise render
    # them as "2^1", "2^2", ... - plain integers read directly as the thread counts they are.
    thread_values = sorted(df["threads"].unique())
    if thread_values:
        ax.set_xticks(thread_values)
        ax.set_xticklabels([str(int(t)) for t in thread_values])
        ax.minorticks_off()
    ax.set_xlabel("Threads / terminals")
    ax.grid(alpha=0.3)


def plot_throughput_vs_threads_tpcc(manifest: pd.DataFrame, out_dir: Path):
    """TPC-C: one figure, x=threads, y=new_order_per_sec, one line per engine."""
    df = manifest[manifest["workload"] == "tpcc"]
    if df.empty:
        print("No tpcc rows in manifest.csv — skipping TPC-C threads-sweep plot.")
        return
    fig, ax = plt.subplots(figsize=(8, 5.5))
    _plot_engine_lines(ax, df)
    ax.set_ylabel(df["primary_metric_name"].iloc[0])
    ax.set_title("TPC-C: throughput vs. thread count")
    ax.legend(fontsize=8)
    _save(fig, out_dir, "threads_sweep_tpcc")


def plot_throughput_vs_threads_ycsb(manifest: pd.DataFrame, out_dir: Path):
    """YCSB: ONE figure covering every loaded workload (A-F), each as its own subplot -
    x=threads, y=ops_per_sec, one line per engine - so the whole YCSB sweep reads off a
    single figure instead of six separate files."""
    workloads = [w for w in YCSB_WORKLOADS if w in manifest["workload"].unique()]
    if not workloads:
        print("No YCSB rows in manifest.csv — skipping YCSB threads-sweep plot.")
        return
    # cols scales down with however many YCSB workloads actually ran (e.g. 2 if only A/E
    # were requested) rather than always reserving 3, which left empty, wasted subplot
    # slots for any subset smaller than the full A-F sweep.
    cols = min(3, len(workloads))
    rows = (len(workloads) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 4.2 * rows), squeeze=False)
    for idx, workload in enumerate(workloads):
        ax = axes[idx // cols][idx % cols]
        df = manifest[manifest["workload"] == workload]
        _plot_engine_lines(ax, df)
        ax.set_ylabel(df["primary_metric_name"].iloc[0])
        ax.set_title(workload.replace("ycsb_", "").upper())
        # Legend on the first subplot only (same convention as plot_gc_comparison below) -
        # every subplot shares the same engine->color mapping, so one legend identifies
        # all of them without a fragile figure-level legend fighting the bottom row's own
        # x-axis labels for space.
        if idx == 0:
            ax.legend(fontsize=8)
    for idx in range(len(workloads), rows * cols):
        axes[idx // cols][idx % cols].axis("off")

    fig.suptitle("YCSB: throughput vs. thread count")
    _save(fig, out_dir, "threads_sweep_ycsb")


def plot_throughput_vs_threads_htap(manifest: pd.DataFrame, out_dir: Path):
    """HTAP: one figure per query with OLTP and OLAP throughput in separate panels.

    The comparison harness sweeps the number of OLTP terminals while keeping
    ``htap_olap_threads`` fixed. Consequently, x is the manifest's ``threads`` value,
    OLTP throughput is the normalized primary metric (New-Order/sec), and aggregate OLAP
    throughput is the number of completed Q1/Q6 scans divided by measured run duration.
    Separate panels avoid forcing these differently-scaled metrics onto a misleading
    shared y-axis.
    """
    for workload in HTAP_WORKLOADS:
        df = manifest[manifest["workload"] == workload].copy()
        if df.empty:
            print(f"No {workload} rows in manifest.csv — skipping HTAP threads-sweep plot.")
            continue
        duration = pd.to_numeric(df["duration_secs"], errors="coerce").replace(0, float("nan"))
        df["olap_queries_per_sec"] = pd.to_numeric(df["scan_count"], errors="coerce") / duration

        fig, (ax_oltp, ax_olap) = plt.subplots(1, 2, figsize=(12, 5.5))
        _plot_engine_lines(ax_oltp, df)
        _plot_engine_lines(ax_olap, df, "olap_queries_per_sec")

        ax_oltp.set_ylabel("New-Order transactions / sec")
        ax_oltp.set_title("OLTP throughput")
        ax_olap.set_ylabel("Completed analytical queries / sec")
        ax_olap.set_title("OLAP throughput")
        ax_oltp.legend(fontsize=8)
        ax_olap.legend(fontsize=8)
        fig.suptitle(f"{workload}: HTAP throughput vs. number of OLTP threads")
        _save(fig, out_dir, f"threads_sweep_{workload}")


def plot_gc_comparison(manifest: pd.DataFrame, ref_threads: int, out_dir: Path):
    """Grouped gc=on vs gc=off bars for engines with a real toggle, one subplot per
    workload at the reference thread count."""
    df = manifest[
        (manifest["engine"].isin(GC_TOGGLE_ENGINES))
        & (manifest["threads"] == ref_threads)
        & (manifest["gc_enabled"].isin(["on", "off"]))
    ]
    if df.empty:
        print("No rows for engines with a GC toggle — skipping GC comparison plot.")
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
    fig.suptitle(f"GC on vs. off throughput (threads={ref_threads}; engines without a working "
                 f"GC toggle are not shown)")
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


def plot_batstore_olap_scan_latency(run_dir: Path, ref_threads: int, out_dir: Path):
    """BatStore-specific: its TPC-C run always includes a concurrent HTAP scan-sweep OLAP
    thread (see tpcc_driver.rs's olap_mode_str default), writing raw per-scan latency vs.
    the scan's staleness/delay to tpcc_scan.csv. Not comparable to the other 3 engines'
    plain-OLTP TPC-C in this harness, so it's its own plot rather than a 4-way bar."""
    candidates = sorted(run_dir.glob(f"tpcc/batstore/threads_{ref_threads}/gc_*/tpcc_scan.csv"))
    if not candidates:
        print(f"No tpcc_scan.csv found for batstore at threads={ref_threads} — skipping BatStore OLAP-scan-latency plot.")
        return
    # Prefer gc=on (the default/representative variant) if present.
    scan_csv = next((c for c in candidates if "gc_on" in c.parts), candidates[0])
    df = pd.read_csv(scan_csv)
    if df.empty:
        print(f"{scan_csv} is empty — skipping BatStore OLAP-scan-latency plot.")
        return
    df["latency_us"] = df["latency_ns"] / 1000.0

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(df["delay_secs"], df["latency_us"], marker="o", color="tab:green")
    ax.set_xlabel("OLAP scan delay / staleness target (seconds)")
    ax.set_ylabel("Scan latency (microseconds)")
    ax.set_title(f"BatStore: TPC-C concurrent OLAP-scan latency vs. staleness (threads={ref_threads})")
    ax.grid(alpha=0.3)
    _save(fig, out_dir, "batstore_tpcc_olap_scan_latency")


def _workload_sort_key(workload: str):
    if workload == "tpcc":
        return (0, 0, workload)
    if workload.startswith("htap_q"):
        try:
            return (1, 0, int(workload.removeprefix("htap_q")))
        except ValueError:
            return (1, 1, workload)
    if workload == "s_htap":
        return (2, 0, workload)
    if workload.startswith("ycsb_"):
        return (3, 0, workload)
    return (4, 0, workload)


def _set_measurement_thread_axis(ax, thread_values) -> None:
    thread_values = sorted(set(thread_values))
    if not thread_values:
        return
    if all(value > 0 for value in thread_values):
        ax.set_xscale("log", base=2)
    ax.set_xticks(thread_values)
    ax.set_xticklabels([str(int(value)) for value in thread_values])
    ax.minorticks_off()
    ax.set_xlabel("Threads / terminals")
    ax.grid(alpha=0.3)


def plot_single_engine_overview(
    manifest: pd.DataFrame, out_dir: Path, output_name: str = "single_engine_overview",
) -> bool:
    """Plot workload rows by throughput, latency, and memory for one engine.

    HTAP rows get distinct OLTP and aggregate OLAP throughput panels. Non-HTAP
    workloads leave the OLAP-only column unused because ``scan_count / duration`` is not
    their primary throughput measurement.
    """
    engines = list(manifest["engine"].dropna().unique())
    if len(engines) != 1:
        return False

    engine = engines[0]
    valid = manifest[~manifest["failed"]].copy()
    workloads = sorted(valid["workload"].dropna().unique(), key=_workload_sort_key)
    if not workloads:
        print(f"No successful {engine} measurements — skipping single-engine overview.")
        return True

    fig, axes = plt.subplots(
        len(workloads), 4, figsize=(20, max(4.2, 3.8 * len(workloads))), squeeze=False,
    )
    gc_styles = {
        "on": ("-", "gc=on"), "n/a": ("-", "gc=n/a"), "off": ("--", "gc=off"),
    }

    for row, workload in enumerate(workloads):
        wdf = valid[valid["workload"] == workload]
        thread_values = sorted(wdf["threads"].unique())

        throughput_ax = axes[row][0]
        for gc_variant, (linestyle, label) in gc_styles.items():
            gdf = wdf[wdf["gc_enabled"] == gc_variant].sort_values("threads")
            if not gdf.empty:
                throughput_ax.plot(
                    gdf["threads"], gdf["primary_metric_value"], marker="o",
                    linestyle=linestyle, label=label,
                )
        throughput_ax.set_ylabel(wdf["primary_metric_name"].iloc[0])
        throughput_ax.set_title(f"{workload}: throughput")
        _set_measurement_thread_axis(throughput_ax, thread_values)
        throughput_ax.legend(fontsize=8)

        olap_throughput_ax = axes[row][1]
        if workload in HTAP_WORKLOADS:
            duration = pd.to_numeric(wdf["duration_secs"], errors="coerce").replace(
                0, float("nan"),
            )
            wdf = wdf.copy()
            wdf["olap_queries_per_sec"] = (
                pd.to_numeric(wdf["scan_count"], errors="coerce") / duration
            )
            for gc_variant, (linestyle, label) in gc_styles.items():
                gdf = wdf[wdf["gc_enabled"] == gc_variant].sort_values("threads")
                if not gdf.empty:
                    olap_throughput_ax.plot(
                        gdf["threads"], gdf["olap_queries_per_sec"], marker="o",
                        linestyle=linestyle, label=label,
                    )
            olap_throughput_ax.set_ylabel("Completed analytical queries / sec")
            olap_throughput_ax.set_title(f"{workload}: OLAP throughput")
            _set_measurement_thread_axis(olap_throughput_ax, thread_values)
            olap_throughput_ax.legend(fontsize=8)
            throughput_ax.set_ylabel("New-Order transactions / sec")
            throughput_ax.set_title(f"{workload}: OLTP throughput")
        else:
            olap_throughput_ax.set_axis_off()

        latency_ax = axes[row][2]
        latency_df = wdf[wdf["scan_count"] > 0]
        latency_gc = next(
            (gc for gc in ("on", "n/a", "off") if gc in set(latency_df["gc_enabled"])), None,
        )
        if latency_gc is None:
            latency_ax.text(
                0.5, 0.5, "No latency measurements", ha="center", va="center",
                transform=latency_ax.transAxes,
            )
            latency_ax.set_axis_off()
        else:
            latency_df = latency_df[latency_df["gc_enabled"] == latency_gc].sort_values("threads")
            for col, label, linestyle in (
                ("scan_p50_us", "p50", ":"),
                ("scan_p95_us", "p95", "--"),
                ("scan_p99_us", "p99", "-"),
            ):
                latency_ax.plot(
                    latency_df["threads"], latency_df[col], marker="o",
                    linestyle=linestyle, label=label,
                )
            latency_ax.set_ylabel("Latency (microseconds)")
            latency_ax.set_title(f"{workload}: latency (gc={latency_gc})")
            _set_measurement_thread_axis(latency_ax, latency_df["threads"].unique())
            latency_ax.legend(fontsize=8)

        memory_ax = axes[row][3]
        for gc_variant, (linestyle, label) in gc_styles.items():
            gdf = wdf[wdf["gc_enabled"] == gc_variant].sort_values("threads")
            if not gdf.empty:
                memory_ax.plot(
                    gdf["threads"], gdf["peak_rss_mb"], marker="o",
                    linestyle=linestyle, label=label,
                )
        memory_ax.set_ylabel("Peak RSS (MB)")
        memory_ax.set_title(f"{workload}: memory")
        _set_measurement_thread_axis(memory_ax, thread_values)
        memory_ax.legend(fontsize=8)

    fig.suptitle(f"{ENGINE_LABELS.get(engine, engine)}: single-engine workload overview")
    _save(fig, out_dir, output_name)
    return True


def plot_all_engines_workload_overview(
    manifest: pd.DataFrame, out_dir: Path, output_name: str = "all_engines_workload_overview",
) -> bool:
    """Plot every engine in one workload-by-metric overview.

    A line's color identifies its engine and its marker identifies the GC state. HTAP rows
    contain separate OLTP and aggregate OLAP throughput panels; the remaining panels show
    p99 scan/query latency and peak RSS. Showing p99 alone keeps the multi-engine latency
    panel readable instead of multiplying every engine/GC line by three percentiles.
    """
    engines = sorted(manifest["engine"].dropna().unique(), key=_engine_sort_key)
    if len(engines) < 2:
        return False

    valid = manifest[~manifest["failed"]].copy()
    workloads = sorted(valid["workload"].dropna().unique(), key=_workload_sort_key)
    if not workloads:
        print("No successful measurements — skipping all-engines workload overview.")
        return True

    gc_markers = {"on": "o", "off": "s", "n/a": "D"}
    gc_labels = {"on": "GC on", "off": "GC off", "n/a": "GC n/a"}
    fig, axes = plt.subplots(
        len(workloads), 4, figsize=(20, max(4.2, 3.8 * len(workloads))), squeeze=False,
    )

    def plot_metric(ax, wdf: pd.DataFrame, value_col: str) -> None:
        for engine in engines:
            edf = wdf[wdf["engine"] == engine]
            for gc_variant in ("on", "off", "n/a"):
                gdf = edf[edf["gc_enabled"] == gc_variant].sort_values("threads")
                if gdf.empty:
                    continue
                ax.plot(
                    gdf["threads"], gdf[value_col],
                    color=ENGINE_COLORS.get(engine, "tab:gray"),
                    marker=gc_markers[gc_variant], linestyle="-", markersize=5,
                )

    for row, workload in enumerate(workloads):
        wdf = valid[valid["workload"] == workload].copy()
        thread_values = sorted(wdf["threads"].unique())

        throughput_ax = axes[row][0]
        plot_metric(throughput_ax, wdf, "primary_metric_value")
        throughput_ax.set_ylabel(wdf["primary_metric_name"].iloc[0])
        throughput_ax.set_title(f"{workload}: throughput")
        _set_measurement_thread_axis(throughput_ax, thread_values)

        olap_throughput_ax = axes[row][1]
        if workload in HTAP_WORKLOADS:
            duration = pd.to_numeric(wdf["duration_secs"], errors="coerce").replace(
                0, float("nan"),
            )
            wdf["olap_queries_per_sec"] = (
                pd.to_numeric(wdf["scan_count"], errors="coerce") / duration
            )
            plot_metric(olap_throughput_ax, wdf, "olap_queries_per_sec")
            olap_throughput_ax.set_ylabel("Completed analytical queries / sec")
            olap_throughput_ax.set_title(f"{workload}: OLAP throughput")
            _set_measurement_thread_axis(olap_throughput_ax, thread_values)
            throughput_ax.set_ylabel("New-Order transactions / sec")
            throughput_ax.set_title(f"{workload}: OLTP throughput")
        else:
            olap_throughput_ax.set_axis_off()

        latency_ax = axes[row][2]
        latency_df = wdf[wdf["scan_count"] > 0]
        if latency_df.empty:
            latency_ax.text(
                0.5, 0.5, "No latency measurements", ha="center", va="center",
                transform=latency_ax.transAxes,
            )
            latency_ax.set_axis_off()
        else:
            plot_metric(latency_ax, latency_df, "scan_p99_us")
            latency_ax.set_ylabel("p99 latency (microseconds)")
            latency_ax.set_title(f"{workload}: p99 latency")
            _set_measurement_thread_axis(latency_ax, latency_df["threads"].unique())

        memory_ax = axes[row][3]
        plot_metric(memory_ax, wdf, "peak_rss_mb")
        memory_ax.set_ylabel("Peak RSS (MB)")
        memory_ax.set_title(f"{workload}: memory")
        _set_measurement_thread_axis(memory_ax, thread_values)

    engine_handles = [
        Line2D([0], [0], color=ENGINE_COLORS.get(engine, "tab:gray"), label=ENGINE_LABELS.get(engine, engine))
        for engine in engines
    ]
    gc_values = [gc for gc in ("on", "off", "n/a") if gc in set(valid["gc_enabled"])]
    gc_handles = [
        Line2D([0], [0], color="black", marker=gc_markers[gc], linestyle="-", label=gc_labels[gc])
        for gc in gc_values
    ]
    axes[0][0].legend(
        handles=engine_handles + gc_handles, fontsize=7, ncol=2,
        title="Color = engine; marker = GC", title_fontsize=8,
    )

    fig.suptitle("All engines: workload overview")
    _save(fig, out_dir, output_name)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-dir", help="A specific run_YYYYmmdd_HHMMSS directory (default: auto-detect the most recently modified one under --results-root)")
    parser.add_argument("--results-root", default="comparison_results", help="Where to look for run_* directories when --run-dir isn't given")
    parser.add_argument(
        "--engine",
        help=("Plot only this engine with the single-engine overview, even when the "
              "manifest contains multiple engines (for example: --engine batstore)"),
    )
    args = parser.parse_args()

    run_dir = Path(args.run_dir) if args.run_dir else find_latest_run_dir(Path(args.results_root))
    if args.run_dir and not run_dir.exists():
        raise SystemExit(f"{run_dir} does not exist")

    print(f"Plotting benchmark results from {run_dir}")
    manifest = load_manifest(run_dir)
    if args.engine:
        available_engines = sorted(manifest["engine"].dropna().unique())
        if args.engine not in available_engines:
            available = ", ".join(available_engines) or "none"
            raise SystemExit(
                f"Engine '{args.engine}' is not present in {run_dir / 'manifest.csv'} "
                f"(available: {available})"
            )
        manifest = manifest[manifest["engine"] == args.engine].copy()
        print(f"Selected single-engine view: {args.engine}")
    out_dir = run_dir / "plots"

    overview_name = f"single_engine_overview_{args.engine}" if args.engine else "single_engine_overview"
    if plot_single_engine_overview(manifest, out_dir, overview_name):
        print(f"\nSingle-engine overview written to {out_dir}")
        return

    plot_all_engines_workload_overview(manifest, out_dir)

    ref_threads = 0
    for gc_choice in ("on", "off"):
        ref_slice, ref_threads = pick_reference_slice(manifest, gc_choice)
        plot_tpcc_throughput(ref_slice, ref_threads, gc_choice, out_dir)
        plot_ycsb_throughput(ref_slice, ref_threads, gc_choice, out_dir)
        plot_memory_usage(ref_slice, ref_threads, gc_choice, out_dir)
        plot_summary_all(ref_slice, ref_threads, gc_choice, out_dir)
    plot_throughput_vs_threads_ycsb(manifest, out_dir)
    plot_throughput_vs_threads_tpcc(manifest, out_dir)
    plot_throughput_vs_threads_htap(manifest, out_dir)
    plot_gc_comparison(manifest, ref_threads, out_dir)
    plot_scan_latency(manifest, ref_threads, out_dir)
    plot_batstore_olap_scan_latency(run_dir, ref_threads, out_dir)
    plot_htap_interference(manifest, ref_threads, out_dir)
    plot_ch_query_latency(manifest, ref_threads, out_dir)

    print(f"\nAll figures written to {out_dir}")


if __name__ == "__main__":
    main()
