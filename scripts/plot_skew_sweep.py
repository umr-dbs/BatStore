#!/usr/bin/env python3
"""Plot YCSB A-F or S-YCSB skew-sweep throughput, memory, and scan latency.

For each workload, the primary figures use skew on the x-axis and throughput on the
y-axis. YCSB-E and S-YCSB also produce p50/p95/p99 scan-latency figures whenever the
manifest contains latency samples.

For regular YCSB, one figure is produced per YCSB workload (a-f),
x-axis = skew factor (uniform, then Zipfian theta 0.1/0.4/0.8/0.99/1.4), y-axis =
throughput (ops/sec), with one line per thread count. One figure is produced per engine
(so BatStore/libmdbx/PostgreSQL/WiredTiger each get their own file and can also be
overlaid manually) plus an all-engines-overlaid variant per workload, at a single
reference thread count, for a quick cross-engine skew comparison. GC-on and GC-off
measurements are written to separate figures.

Reads <run_dir>/manifest.csv, written by run_skew_sweep.py - same schema as
compare_engines.py's manifest, with the skew value stamped into config_label as
"<scale-label> skew=<label>" (see run_skew_sweep.py::main).

    python3 scripts/plot_skew_sweep.py --run-dir skew_sweep_results/run_20260101_120000

Requires: pandas, matplotlib.
"""
import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from plot_styles import (
    ENGINE_LABELS, apply_compact_layout, compact_enabled, engine_line_style,
    engine_sort_key,
)
YCSB_WORKLOADS = [f"ycsb_{w}" for w in "abcdef"]
SKEW_WORKLOADS = YCSB_WORKLOADS + ["s_htap"]

_SKEW_RE = re.compile(r"skew=(\S+)")


def skew_label(config_label: str) -> str:
    m = _SKEW_RE.search(config_label)
    return m.group(1) if m else "?"


def skew_sort_key(skew: str):
    return (-1.0, "uniform") if skew == "uniform" else (float(skew), skew)


def workload_label(workload: str) -> str:
    return "S-YCSB" if workload == "s_htap" else f"YCSB {workload.split('_')[1].upper()}"


def load_manifest(run_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(run_dir / "manifest.csv")
    df = df[df["notes"].fillna("") == ""]
    df["gc_enabled"] = df["gc_enabled"].fillna("n/a")
    if "memory_source" not in df:
        df["memory_source"] = "process_rss"
    else:
        df["memory_source"] = df["memory_source"].fillna("process_rss")
    df["skew"] = df["config_label"].map(skew_label)
    return df


def gc_slices(df: pd.DataFrame):
    """Yield (label, rows) without putting GC-on and GC-off in one figure."""
    gc_values = set(df["gc_enabled"])
    choices = [gc for gc in ("on", "off") if gc in gc_values]
    if choices:
        for gc in choices:
            yield gc, df[df["gc_enabled"].isin([gc, "n/a"])]
    elif "n/a" in gc_values:
        yield "na", df[df["gc_enabled"] == "n/a"]


def prepare_output_dir(out_dir: Path) -> None:
    """Create the output layout and relocate PDFs from the legacy flat layout."""
    out_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir = out_dir / "pdf"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "svg").mkdir(parents=True, exist_ok=True)
    for path in out_dir.glob("*.pdf"):
        if "overview" not in path.stem:
            path.replace(pdf_dir / path.name)


def _save(fig, out_dir: Path, name: str, *, overview: bool = False) -> None:
    """Keep overview PDFs in plots/; put detailed PDFs/SVGs in format folders."""
    prepare_output_dir(out_dir)
    apply_compact_layout(fig)
    for ext in ("pdf", "svg"):
        if ext == "svg":
            destination = out_dir / "svg"
        else:
            destination = out_dir if overview else out_dir / "pdf"
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / f"{name}.{ext}"
        fig.savefig(path)
        print(f"Wrote {path}")


def plot_workload_per_engine(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path,
) -> None:
    sub = df[df["workload"] == workload]
    if sub.empty:
        return
    for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
        esub = sub[sub["engine"] == engine]
        skews = sorted(esub["skew"].unique(), key=skew_sort_key)
        fig, ax = plt.subplots(figsize=(7, 5))
        for threads in sorted(esub["threads"].unique()):
            tsub = esub[esub["threads"] == threads].set_index("skew").reindex(skews)
            ax.plot(skews, tsub["primary_metric_value"], marker="o", label=f"{threads} threads")
        ax.set_xlabel("Skew factor (Zipfian theta; 'uniform' = theta 0.0)")
        ax.set_ylabel("Throughput (ops/sec)")
        ax.set_title(
            f"{ENGINE_LABELS.get(engine, engine)} - "
            f"{workload_label(workload)} vs. skew - GC {gc_choice}"
        )
        ax.legend(title="Threads", fontsize="small")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        _save(fig, out_dir, f"skew_{workload}_{engine}_gc_{gc_choice}")
        plt.close(fig)


def plot_workload_all_engines(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path, ref_threads: int,
) -> None:
    sub = df[(df["workload"] == workload) & (df["threads"] == ref_threads)]
    if sub.empty:
        return
    fig, ax = plt.subplots(figsize=(7, 5))
    for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
        esub = sub[sub["engine"] == engine]
        skews = sorted(esub["skew"].unique(), key=skew_sort_key)
        esub = esub.set_index("skew").reindex(skews)
        ax.plot(
            skews, esub["primary_metric_value"],
            label=ENGINE_LABELS.get(engine, engine), **engine_line_style(engine),
        )
    ax.set_xlabel("Skew factor (Zipfian theta; 'uniform' = theta 0.0)")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_title(
        f"{workload_label(workload)} vs. skew "
        f"(threads={ref_threads}) - GC {gc_choice}"
    )
    ax.legend(fontsize="small")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    _save(
        fig, out_dir,
        f"skew_{workload}_all_engines_threads{ref_threads}_gc_{gc_choice}",
    )
    plt.close(fig)


def _latency_rows(df: pd.DataFrame, workload: str) -> pd.DataFrame:
    """Successful rows with an actual scan-latency sample population."""
    return df[
        (df["workload"] == workload)
        & (pd.to_numeric(df["scan_count"], errors="coerce").fillna(0) > 0)
    ].copy()


def plot_latency_per_engine(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path,
) -> None:
    """Skew-to-latency curves per engine, with one panel per percentile."""
    sub = _latency_rows(df, workload)
    if sub.empty:
        return
    metrics = (("scan_p50_us", "p50"), ("scan_p95_us", "p95"), ("scan_p99_us", "p99"))
    for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
        esub = sub[sub["engine"] == engine]
        skews = sorted(esub["skew"].unique(), key=skew_sort_key)
        fig, axes = plt.subplots(1, 3, figsize=(15, 4.6), squeeze=False, sharex=True)
        for idx, (column, percentile) in enumerate(metrics):
            ax = axes[0][idx]
            for threads in sorted(esub["threads"].unique()):
                tsub = esub[esub["threads"] == threads].set_index("skew").reindex(skews)
                ax.plot(skews, tsub[column], marker="o", label=f"{threads} threads")
            ax.set_xlabel("Skew factor")
            ax.set_ylabel(f"{percentile} scan latency (µs)")
            ax.set_title(percentile)
            ax.grid(True, alpha=0.3)
            if idx == 0:
                ax.legend(title="Total threads", fontsize="small")
        fig.suptitle(
            f"{ENGINE_LABELS.get(engine, engine)} - {workload_label(workload)} "
            f"scan latency vs. skew - GC {gc_choice}"
        )
        _save(fig, out_dir, f"skew_{workload}_latency_{engine}_gc_{gc_choice}")
        plt.close(fig)


def plot_latency_all_engines(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path, ref_threads: int,
) -> None:
    """Cross-engine skew-to-latency curves at one reference thread count."""
    sub = _latency_rows(df, workload)
    sub = sub[sub["threads"] == ref_threads]
    if sub.empty:
        return
    metrics = (("scan_p50_us", "p50"), ("scan_p95_us", "p95"), ("scan_p99_us", "p99"))
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.6), squeeze=False, sharex=True)
    for idx, (column, percentile) in enumerate(metrics):
        ax = axes[0][idx]
        for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
            esub = sub[sub["engine"] == engine]
            skews = sorted(esub["skew"].unique(), key=skew_sort_key)
            esub = esub.set_index("skew").reindex(skews)
            ax.plot(
                skews, esub[column], label=ENGINE_LABELS.get(engine, engine),
                **engine_line_style(engine),
            )
        ax.set_xlabel("Skew factor")
        ax.set_ylabel(f"{percentile} scan latency (µs)")
        ax.set_title(percentile)
        ax.grid(True, alpha=0.3)
        if idx == 0:
            ax.legend(fontsize="small")
    fig.suptitle(
        f"{workload_label(workload)} scan latency vs. skew "
        f"(threads={ref_threads}) - GC {gc_choice}"
    )
    _save(
        fig, out_dir,
        f"skew_{workload}_latency_all_engines_threads{ref_threads}_gc_{gc_choice}",
    )
    plt.close(fig)


def plot_workloads_overview(
    df: pd.DataFrame, gc_choice: str, out_dir: Path, ref_threads: int,
    value_column: str, ylabel: str, metric_name: str,
) -> None:
    """Plot all available YCSB/S-YCSB workloads as an engine-comparison grid."""
    sub = df[
        df["workload"].isin(SKEW_WORKLOADS) & (df["threads"] == ref_threads)
    ]
    workloads = [workload for workload in SKEW_WORKLOADS if workload in set(sub["workload"])]
    if not workloads:
        return

    skews = sorted(sub["skew"].unique(), key=skew_sort_key)
    cols = min(3, len(workloads))
    rows = (len(workloads) + cols - 1) // cols
    fig, axes = plt.subplots(
        rows, cols, figsize=(5 * cols, 4.1 * rows), squeeze=False, sharex=True,
    )
    last_active_row = {
        col: max(idx // cols for idx in range(len(workloads)) if idx % cols == col)
        for col in range(min(cols, len(workloads)))
    }
    legend_handles = {}
    for idx, workload in enumerate(workloads):
        row, col = divmod(idx, cols)
        ax = axes[row][col]
        workload_df = sub[sub["workload"] == workload]
        for engine in sorted(workload_df["engine"].unique(), key=engine_sort_key):
            engine_df = (
                workload_df[workload_df["engine"] == engine]
                .set_index("skew")
                .reindex(skews)
            )
            label = ENGINE_LABELS.get(engine, engine)
            line, = ax.plot(
                skews, engine_df[value_column], label=label,
                **engine_line_style(engine),
            )
            legend_handles.setdefault(label, line)
        ax.set_title(workload_label(workload))
        if compact_enabled():
            ax.set_xlabel("Skew factor" if row == last_active_row[col] else "")
        else:
            ax.set_xlabel("Skew factor")
            ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.3)
        # ``sharex=True`` otherwise lets Matplotlib hide tick values on every
        # row except the last, which makes individual overview panels harder
        # to read when cropped or embedded.
        ax.tick_params(axis="x", which="both", labelbottom=True)

    for idx in range(len(workloads), rows * cols):
        axes[idx // cols][idx % cols].axis("off")

    source_note = ""
    if value_column == "peak_rss_mb" and "memory_source" in sub:
        sources = set(sub["memory_source"])
        if "cgroup_v2_memory.current" in sources and len(sources) > 1:
            source_note = " — cgroup total where available; otherwise process RSS"
    if compact_enabled():
        fig.supylabel(ylabel)
    fig.suptitle(
        f"YCSB/S-YCSB workload {metric_name} overview "
        f"(threads={ref_threads}, GC {gc_choice}){source_note}"
    )
    if legend_handles:
        fig.legend(
            legend_handles.values(), legend_handles.keys(),
            loc="lower center", ncol=min(4, len(legend_handles)), fontsize="small",
        )
    fig.tight_layout(rect=(0, 0.08, 1, 0.95))
    _save(
        fig, out_dir,
        f"skew_workloads_{metric_name}_overview_threads{ref_threads}_gc_{gc_choice}",
        overview=True,
    )
    plt.close(fig)


def plot_overviews(df: pd.DataFrame, out_dir: Path) -> None:
    """Write throughput and peak-memory grids for every GC mode and thread count."""
    for gc_choice, gc_df in gc_slices(df):
        for threads in sorted(int(value) for value in gc_df["threads"].unique()):
            plot_workloads_overview(
                gc_df, gc_choice, out_dir, threads,
                "primary_metric_value", "Throughput (ops/sec)", "throughput",
            )
            plot_workloads_overview(
                gc_df, gc_choice, out_dir, threads,
                "peak_rss_mb", "Peak measured memory (MB)", "memory",
            )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", required=True, type=Path)
    p.add_argument("--ref-threads", type=int, default=None,
                   help="thread count used for the all-engines-overlay plot (default: max present)")
    args = p.parse_args()

    df = load_manifest(args.run_dir)
    out_dir = args.run_dir / "plots"
    prepare_output_dir(out_dir)

    for workload in SKEW_WORKLOADS:
        workload_df = df[df["workload"] == workload]
        if workload_df.empty:
            continue
        ref_threads = args.ref_threads or int(workload_df["threads"].max())
        for gc_choice, gc_df in gc_slices(workload_df):
            plot_workload_per_engine(gc_df, workload, gc_choice, out_dir)
            plot_workload_all_engines(
                gc_df, workload, gc_choice, out_dir, ref_threads,
            )
            plot_latency_per_engine(gc_df, workload, gc_choice, out_dir)
            plot_latency_all_engines(
                gc_df, workload, gc_choice, out_dir, ref_threads,
            )

    if not df.empty:
        plot_overviews(df, out_dir)

    print(f"Wrote plots to {out_dir}")


if __name__ == "__main__":
    main()
