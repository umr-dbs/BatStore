#!/usr/bin/env python3
"""Plot YCSB A-F or S-YCSB skew-sweep throughput, memory, and latency.

Regular YCSB figures split the aggregate throughput into read-class and write-class
operations and plot the matching p50/p95/p99 operation latency. Reads and scans are
read-class operations; updates, inserts, and read-modify-write operations are
write-class operations. S-YCSB retains its write-throughput and scan-latency plots.

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

# The standard YCSB mixes used by every engine.  Multiplying the normalized aggregate
# throughput by these ratios gives a comparable read/write split without relying on
# sampled latency counts (which are deliberately sparse and can differ by one sample).
YCSB_OPERATION_MIX = {
    "ycsb_a": (0.50, 0.50),
    "ycsb_b": (0.95, 0.05),
    "ycsb_c": (1.00, 0.00),
    "ycsb_d": (0.95, 0.05),
    "ycsb_e": (0.95, 0.05),
    "ycsb_f": (0.50, 0.50),
}
YCSB_OPERATION_BY_CLASS = {
    "ycsb_a": ("read", "update"),
    "ycsb_b": ("read", "update"),
    "ycsb_c": ("read", None),
    "ycsb_d": ("read", "insert"),
    "ycsb_e": ("scan", "insert"),
    "ycsb_f": ("read", "read_modify_write"),
}

_SKEW_RE = re.compile(r"skew=(\S+)")


def skew_label(config_label: str) -> str:
    m = _SKEW_RE.search(config_label)
    return m.group(1) if m else "?"


def skew_sort_key(skew: str):
    return (-1.0, "uniform") if skew == "uniform" else (float(skew), skew)


def workload_label(workload: str) -> str:
    return "S-YCSB" if workload == "s_htap" else f"YCSB {workload.split('_')[1].upper()}"


def _operation_summary_path(run_dir: Path, row: pd.Series) -> Path:
    gc_dir = f"gc_{row['gc_enabled']}".replace("/", "_")
    base = (
        run_dir / row["workload"] / row["engine"] / f"skew_{row['skew']}"
        / f"threads_{int(row['threads'])}"
    )
    preferred = base / gc_dir / "ycsb_operation_latency_summary.csv"
    # Skew sweeps duplicate a non-toggle engine's single gc_n_a measurement into
    # manifest rows labelled on/off so the comparison slices remain symmetrical.
    fallback = base / "gc_n_a" / "ycsb_operation_latency_summary.csv"
    return preferred if preferred.exists() else fallback


def _add_ycsb_operation_metrics(run_dir: Path, df: pd.DataFrame) -> pd.DataFrame:
    """Add read/write throughput and operation-latency columns to manifest rows."""
    result = df.copy()
    for operation_class in ("read", "write"):
        result[f"{operation_class}_throughput"] = float("nan")
        result[f"{operation_class}_operation"] = None
        for percentile in ("p50", "p95", "p99"):
            result[f"{operation_class}_{percentile}_us"] = float("nan")

    for index, row in result.iterrows():
        workload = row["workload"]
        if workload == "s_htap":
            result.at[index, "write_throughput"] = pd.to_numeric(
                row["primary_metric_value"], errors="coerce",
            )
            if pd.to_numeric(row.get("scan_count"), errors="coerce") > 0:
                result.at[index, "read_operation"] = "scan"
                for percentile in ("p50", "p95", "p99"):
                    result.at[index, f"read_{percentile}_us"] = pd.to_numeric(
                        row.get(f"scan_{percentile}_us"), errors="coerce",
                    )
            continue
        if workload not in YCSB_OPERATION_MIX:
            continue
        read_ratio, write_ratio = YCSB_OPERATION_MIX[workload]
        aggregate = pd.to_numeric(row["primary_metric_value"], errors="coerce")
        result.at[index, "read_throughput"] = aggregate * read_ratio
        result.at[index, "write_throughput"] = aggregate * write_ratio

        summary_path = _operation_summary_path(run_dir, row)
        summary = None
        if summary_path.exists():
            try:
                summary = pd.read_csv(summary_path).set_index("operation")
            except (OSError, ValueError, KeyError):
                pass
        if summary is None:
            # Older YCSB-E runs only have the backward-compatible scan summary in
            # manifest.csv. Keep those runs plottable as read-class latency.
            if (
                workload == "ycsb_e"
                and pd.to_numeric(row.get("scan_count"), errors="coerce") > 0
            ):
                result.at[index, "read_operation"] = "scan"
                for percentile in ("p50", "p95", "p99"):
                    result.at[index, f"read_{percentile}_us"] = pd.to_numeric(
                        row.get(f"scan_{percentile}_us"), errors="coerce",
                    )
            continue
        for operation_class, operation in zip(
            ("read", "write"), YCSB_OPERATION_BY_CLASS[workload],
        ):
            if operation is None or operation not in summary.index:
                continue
            operation_row = summary.loc[operation]
            if pd.to_numeric(operation_row.get("count"), errors="coerce") <= 0:
                continue
            result.at[index, f"{operation_class}_operation"] = operation
            for percentile in ("p50", "p95", "p99"):
                result.at[index, f"{operation_class}_{percentile}_us"] = pd.to_numeric(
                    operation_row.get(f"{percentile}_us"), errors="coerce",
                )
    return result


def load_manifest(run_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(run_dir / "manifest.csv")
    df = df[df["notes"].fillna("") == ""]
    df["gc_enabled"] = df["gc_enabled"].fillna("n/a")
    if "memory_source" not in df:
        df["memory_source"] = "process_rss"
    else:
        df["memory_source"] = df["memory_source"].fillna("process_rss")
    df["skew"] = df["config_label"].map(skew_label)
    return _add_ycsb_operation_metrics(run_dir, df)


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
        metrics = (
            (("read_throughput", "Read throughput"), ("write_throughput", "Write throughput"))
            if workload in YCSB_WORKLOADS
            else (("primary_metric_value", "Write throughput"),)
        )
        fig, axes = plt.subplots(1, len(metrics), figsize=(7 * len(metrics), 5), squeeze=False)
        for metric_index, (column, title) in enumerate(metrics):
            ax = axes[0][metric_index]
            for threads in sorted(esub["threads"].unique()):
                tsub = esub[esub["threads"] == threads].set_index("skew").reindex(skews)
                ax.plot(skews, tsub[column], marker="o", label=f"{threads} threads")
            ax.set_xlabel("Skew factor (Zipfian theta; 'uniform' = theta 0.0)")
            ax.set_ylabel(f"{title} (ops/sec)")
            ax.set_title(title)
            ax.grid(True, alpha=0.3)
            if metric_index == 0:
                ax.legend(title="Threads", fontsize="small")
        fig.suptitle(
            f"{ENGINE_LABELS.get(engine, engine)} - "
            f"{workload_label(workload)} vs. skew - GC {gc_choice}"
        )
        fig.tight_layout()
        _save(fig, out_dir, f"skew_{workload}_read_write_throughput_{engine}_gc_{gc_choice}")
        plt.close(fig)


def plot_workload_all_engines(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path, ref_threads: int,
) -> None:
    sub = df[(df["workload"] == workload) & (df["threads"] == ref_threads)]
    if sub.empty:
        return
    metrics = (
        (("read_throughput", "Read throughput"), ("write_throughput", "Write throughput"))
        if workload in YCSB_WORKLOADS
        else (("primary_metric_value", "Write throughput"),)
    )
    fig, axes = plt.subplots(1, len(metrics), figsize=(7 * len(metrics), 5), squeeze=False)
    for metric_index, (column, title) in enumerate(metrics):
        ax = axes[0][metric_index]
        for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
            esub = sub[sub["engine"] == engine]
            skews = sorted(esub["skew"].unique(), key=skew_sort_key)
            esub = esub.set_index("skew").reindex(skews)
            ax.plot(
                skews, esub[column],
                label=ENGINE_LABELS.get(engine, engine), **engine_line_style(engine),
            )
        ax.set_xlabel("Skew factor (Zipfian theta; 'uniform' = theta 0.0)")
        ax.set_ylabel(f"{title} (ops/sec)")
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        if metric_index == 0:
            ax.legend(fontsize="small")
    fig.suptitle(
        f"{workload_label(workload)} vs. skew "
        f"(threads={ref_threads}) - GC {gc_choice}"
    )
    fig.tight_layout()
    _save(
        fig, out_dir,
        f"skew_{workload}_read_write_throughput_all_engines_threads{ref_threads}_gc_{gc_choice}",
    )
    plt.close(fig)


def _latency_rows(df: pd.DataFrame, workload: str) -> pd.DataFrame:
    """Successful rows with an actual latency sample population."""
    if workload in YCSB_WORKLOADS:
        latency_columns = [
            f"{operation_class}_{percentile}_us"
            for operation_class in ("read", "write")
            for percentile in ("p50", "p95", "p99")
        ]
        return df[
            (df["workload"] == workload) & df[latency_columns].notna().any(axis=1)
        ].copy()
    return df[
        (df["workload"] == workload)
        & (pd.to_numeric(df["scan_count"], errors="coerce").fillna(0) > 0)
    ].copy()


def _latency_metrics(workload: str):
    if workload in YCSB_WORKLOADS:
        return [
            (operation_class, percentile, f"{operation_class}_{percentile}_us")
            for operation_class in ("read", "write")
            for percentile in ("p50", "p95", "p99")
        ]
    return [
        ("read", percentile, column)
        for column, percentile in (
            ("scan_p50_us", "p50"),
            ("scan_p95_us", "p95"),
            ("scan_p99_us", "p99"),
        )
    ]


def plot_latency_per_engine(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path,
) -> None:
    """Skew-to-latency curves per engine, split by read/write and percentile."""
    sub = _latency_rows(df, workload)
    if sub.empty:
        return
    metrics = _latency_metrics(workload)
    row_classes = ["read", "write"] if workload in YCSB_WORKLOADS else ["read"]
    for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
        esub = sub[sub["engine"] == engine]
        skews = sorted(esub["skew"].unique(), key=skew_sort_key)
        fig, axes = plt.subplots(
            len(row_classes), 3, figsize=(15, 4.4 * len(row_classes)),
            squeeze=False, sharex=True, sharey="row",
        )
        for operation_class, percentile, column in metrics:
            row = row_classes.index(operation_class)
            col = ("p50", "p95", "p99").index(percentile)
            ax = axes[row][col]
            if esub[column].notna().sum() == 0:
                ax.text(0.5, 0.5, "No measurements", ha="center", va="center",
                        transform=ax.transAxes)
                ax.set_axis_off()
                continue
            for threads in sorted(esub["threads"].unique()):
                tsub = esub[esub["threads"] == threads].set_index("skew").reindex(skews)
                ax.plot(skews, tsub[column], marker="o", label=f"{threads} threads")
            ax.set_xlabel("Skew factor")
            ax.set_yscale("log")
            ax.set_ylabel(f"{operation_class.title()} {percentile} latency (µs, log)")
            ax.set_title(percentile)
            ax.grid(True, alpha=0.3)
            if col == 0:
                ax.legend(title="Total threads", fontsize="small")
        fig.suptitle(
            f"{ENGINE_LABELS.get(engine, engine)} - {workload_label(workload)} "
            f"read/write latency vs. skew - GC {gc_choice}"
        )
        _save(fig, out_dir, f"skew_{workload}_read_write_latency_{engine}_gc_{gc_choice}")
        plt.close(fig)


def plot_latency_all_engines(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path, ref_threads: int,
) -> None:
    """Cross-engine latency curves at one thread count, split by read/write."""
    sub = _latency_rows(df, workload)
    sub = sub[sub["threads"] == ref_threads]
    if sub.empty:
        return
    metrics = _latency_metrics(workload)
    row_classes = ["read", "write"] if workload in YCSB_WORKLOADS else ["read"]
    fig, axes = plt.subplots(
        len(row_classes), 3, figsize=(15, 4.4 * len(row_classes)),
        squeeze=False, sharex=True, sharey="row",
    )
    for operation_class, percentile, column in metrics:
        row = row_classes.index(operation_class)
        col = ("p50", "p95", "p99").index(percentile)
        ax = axes[row][col]
        if sub[column].notna().sum() == 0:
            ax.text(0.5, 0.5, "No measurements", ha="center", va="center",
                    transform=ax.transAxes)
            ax.set_axis_off()
            continue
        for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
            esub = sub[sub["engine"] == engine]
            skews = sorted(esub["skew"].unique(), key=skew_sort_key)
            esub = esub.set_index("skew").reindex(skews)
            ax.plot(
                skews, esub[column], label=ENGINE_LABELS.get(engine, engine),
                **engine_line_style(engine),
            )
        ax.set_xlabel("Skew factor")
        ax.set_yscale("log")
        ax.set_ylabel(f"{operation_class.title()} {percentile} latency (µs, log)")
        ax.set_title(percentile)
        ax.grid(True, alpha=0.3)
        if col == 0:
            ax.legend(fontsize="small")
    fig.suptitle(
        f"{workload_label(workload)} read/write latency vs. skew "
        f"(threads={ref_threads}) - GC {gc_choice}"
    )
    _save(
        fig, out_dir,
        f"skew_{workload}_read_write_latency_all_engines_threads{ref_threads}_gc_{gc_choice}",
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
    numeric_values = pd.to_numeric(sub[value_column], errors="coerce")
    workloads = [
        workload for workload in SKEW_WORKLOADS
        if numeric_values[sub["workload"] == workload].notna().any()
    ]
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
    """Write read/write throughput, latency, and memory overview grids."""
    for gc_choice, gc_df in gc_slices(df):
        for threads in sorted(int(value) for value in gc_df["threads"].unique()):
            plot_workloads_overview(
                gc_df, gc_choice, out_dir, threads,
                "read_throughput", "Read throughput (ops/sec)", "read_throughput",
            )
            plot_workloads_overview(
                gc_df, gc_choice, out_dir, threads,
                "write_throughput", "Write throughput (ops/sec)", "write_throughput",
            )
            plot_workloads_overview(
                gc_df, gc_choice, out_dir, threads,
                "read_p99_us", "Read p99 latency (µs)", "read_p99_latency",
            )
            plot_workloads_overview(
                gc_df, gc_choice, out_dir, threads,
                "write_p99_us", "Write p99 latency (µs)", "write_p99_latency",
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
