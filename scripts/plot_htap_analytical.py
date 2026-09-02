#!/usr/bin/env python3
"""Plot figures for a scripts/run_htap_analytical_sweep.py run: x-axis = number of
analytical (OLAP) threads, with two separate y-axis series per engine - OLTP throughput
(new_order_per_sec, from the fixed-size TPC-C population/terminal pool) and OLAP
throughput (CH-benCHmark Q1/Q6 queries/sec, summed across all analytical threads) - drawn
as two side-by-side subplots (OLTP left, OLAP right) so the OLTP-interference and the
OLAP-scaling stories are each readable on their own axis. It also plots analytical-query
p50/p95/p99 latency over the thread sweep. Each workload gets per-engine figures and an
all-engines overlay. GC-on and GC-off measurements are written to separate figures;
engines without a GC toggle (``gc_enabled=n/a``) are shown in both comparison sets.
Dedicated OLTP-throughput figures are also saved per engine and across engines.
The throughput_overview figures collect all measured workloads into rows, with
OLTP throughput on the left and OLAP throughput on the right.

Reads <run_dir>/manifest.csv, written by run_htap_analytical_sweep.py - same schema as
compare_engines.py's manifest, with the analytical thread count stamped into
config_label as "... olap_threads=<n>" (see that script's main()). OLAP throughput is
derived here as scan_count / duration_secs (not stored directly in the manifest).

    python3 scripts/plot_htap_analytical.py --run-dir htap_analytical_results/run_20260101_120000

Requires: pandas, matplotlib.
"""
import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from plot_styles import (
    ENGINE_LABELS,
    apply_compact_layout,
    compact_enabled,
    engine_line_style,
    engine_sort_key,
    latency_line_style,
    measurement_positions,
    measurement_values,
    set_compact,
    set_measurement_axis,
)
HTAP_WORKLOADS = ["htap_q1", "htap_q6", "htap_q1_variant", "htap_q6_variant"]
HTAP_LABELS = {
    "htap_q1": "CH-benCHmark Q1 (Pricing Summary Report)",
    "htap_q6": "CH-benCHmark Q6 (Forecasting Revenue Change)",
    "htap_q1_variant": "Q1 predicate variant",
    "htap_q6_variant": "Q6 predicate variant",
}
HTAP_PANEL_LABELS = {
    "htap_q1": "Q1 Pricing Summary",
    "htap_q6": "Q6 Revenue Change",
    "htap_q1_variant": "Q1 predicate variant",
    "htap_q6_variant": "Q6 predicate variant",
}
HTAP_COMPACT_PANEL_LABELS = {
    "htap_q1": "Q1",
    "htap_q6": "Q6",
    "htap_q1_variant": "Q1 variant",
    "htap_q6_variant": "Q6 variant",
}
COMPACT_OLAP_THREADS = {1, 2, 4, 8, 16, 32, 64, 120}

_OLAP_RE = re.compile(r"olap_threads=(\d+)")


def olap_threads_of(config_label: str) -> int:
    m = _OLAP_RE.search(config_label)
    return int(m.group(1)) if m else -1


def load_manifest(run_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(run_dir / "manifest.csv")
    # A note is not necessarily a failed measurement.  In particular, PostgreSQL
    # records a useful throughput/latency row with a peak-RSS warning when the
    # postmaster PID cannot be located.  The old blanket filter silently removed
    # those points from every HTAP plot.
    notes = df["notes"].fillna("")
    harmless_note = notes.str.startswith("peak_rss unavailable")
    usable = (notes == "") | harmless_note
    for _, row in df[~usable].iterrows():
        print(
            "Skipping unsuccessful HTAP point: "
            f"engine={row.get('engine', '?')}, workload={row.get('workload', '?')}, "
            f"config={row.get('config_label', '?')} - {row.get('notes', '')}"
        )
    df = df[usable].copy()
    df["gc_enabled"] = df["gc_enabled"].fillna("n/a")
    df["olap_threads"] = df["config_label"].map(olap_threads_of)
    df["olap_qps"] = df["scan_count"] / df["duration_secs"].replace(0, float("nan"))
    return df


def gc_slices(df: pd.DataFrame):
    """Yield (label, rows) without ever putting GC-on and GC-off in one figure."""
    gc_values = set(df["gc_enabled"])
    choices = [gc for gc in ("on", "off") if gc in gc_values]
    if choices:
        for gc in choices:
            yield gc, df[df["gc_enabled"].isin([gc, "n/a"])]
    elif "n/a" in gc_values:
        yield "na", df[df["gc_enabled"] == "n/a"]


def _save(fig, out_dir: Path, name: str) -> None:
    """Save PDFs in plots/ and SVGs in plots/svg/."""
    out_dir.mkdir(parents=True, exist_ok=True)
    apply_compact_layout(fig)
    for ext in ("pdf", "svg"):
        destination = out_dir / "svg" if ext == "svg" else out_dir
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / f"{name}.{ext}"
        fig.savefig(path)
        print(f"Wrote {path}")


def _set_olap_thread_axis(ax, thread_values) -> None:
    """Label evenly spaced positions with the measured OLAP thread counts."""
    set_measurement_axis(ax, thread_values, "Number of analytical (OLAP) threads")


def _olap_thread_values(thread_values) -> list[int]:
    return measurement_values(thread_values)


def _olap_thread_positions(thread_values, axis_values) -> list[int]:
    """Map real thread counts to equally spaced categorical positions."""
    return measurement_positions(thread_values, axis_values)


def _plot_pair(
    ax_oltp, ax_olap, thread_values, axis_values,
    oltp_series, olap_series, engine,
):
    label = ENGINE_LABELS.get(engine, engine)
    positions = _olap_thread_positions(thread_values, axis_values)
    ax_oltp.plot(positions, oltp_series, label=label, **engine_line_style(engine))
    ax_olap.plot(positions, olap_series, label=label, **engine_line_style(engine))


def plot_workload_per_engine(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path,
) -> None:
    sub = df[df["workload"] == workload]
    if sub.empty:
        return
    for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
        esub = sub[sub["engine"] == engine].sort_values("olap_threads")
        thread_values = _olap_thread_values(esub["olap_threads"])
        fig, (ax_oltp, ax_olap) = plt.subplots(1, 2, figsize=(12, 5))
        _plot_pair(
            ax_oltp, ax_olap, esub["olap_threads"], thread_values,
            esub["primary_metric_value"], esub["olap_qps"], engine,
        )
        ax_oltp.set_title("OLTP throughput (fixed OLTP terminals)")
        ax_oltp.set_ylabel("new_order/sec")
        ax_olap.set_title("OLAP throughput (all analytical threads)")
        ax_olap.set_ylabel("queries/sec")
        for ax in (ax_oltp, ax_olap):
            _set_olap_thread_axis(ax, thread_values)
        fig.suptitle(
            f"{ENGINE_LABELS.get(engine, engine)} - "
            f"{HTAP_LABELS.get(workload, workload)} - GC {gc_choice}"
        )
        fig.tight_layout()
        _save(fig, out_dir, f"htap_analytical_{workload}_{engine}_gc_{gc_choice}")
        plt.close(fig)


def plot_workload_all_engines(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path,
) -> None:
    sub = df[df["workload"] == workload]
    if sub.empty:
        return
    thread_values = _olap_thread_values(sub["olap_threads"])
    fig, (ax_oltp, ax_olap) = plt.subplots(1, 2, figsize=(12, 5))
    for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
        esub = sub[sub["engine"] == engine].sort_values("olap_threads")
        _plot_pair(
            ax_oltp, ax_olap, esub["olap_threads"], thread_values,
            esub["primary_metric_value"], esub["olap_qps"], engine,
        )
    ax_oltp.set_title("OLTP throughput (fixed OLTP terminals)")
    ax_oltp.set_ylabel("new_order/sec")
    ax_olap.set_title("OLAP throughput (all analytical threads)")
    ax_olap.set_ylabel("queries/sec")
    for ax in (ax_oltp, ax_olap):
        _set_olap_thread_axis(ax, thread_values)
        ax.legend(fontsize="small")
    fig.suptitle(f"{HTAP_LABELS.get(workload, workload)} - GC {gc_choice}")
    fig.tight_layout()
    _save(fig, out_dir, f"htap_analytical_{workload}_all_engines_gc_{gc_choice}")
    plt.close(fig)


def plot_workload_oltp(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path,
) -> None:
    """Save standalone OLTP throughput plots per engine and across engines."""
    sub = df[df["workload"] == workload]
    if sub.empty:
        return
    engines = sorted(sub["engine"].unique(), key=engine_sort_key)
    groups = [(engine, sub[sub["engine"] == engine]) for engine in engines]
    groups.append(("all_engines", sub))
    for name, rows in groups:
        thread_values = _olap_thread_values(rows["olap_threads"])
        fig, ax = plt.subplots(figsize=(8, 5))
        for engine in sorted(rows["engine"].unique(), key=engine_sort_key):
            esub = rows[rows["engine"] == engine].sort_values("olap_threads")
            ax.plot(
                _olap_thread_positions(esub["olap_threads"], thread_values),
                esub["primary_metric_value"],
                label=ENGINE_LABELS.get(engine, engine), **engine_line_style(engine),
            )
        _set_olap_thread_axis(ax, thread_values)
        ax.set_ylabel("new_order/sec")
        ax.set_ylim(bottom=0)
        ax.set_title("OLTP throughput (fixed OLTP terminals)")
        ax.legend(fontsize="small")
        title = "All engines" if name == "all_engines" else ENGINE_LABELS.get(name, name)
        fig.suptitle(f"{title} - {HTAP_LABELS.get(workload, workload)} - GC {gc_choice}")
        fig.tight_layout()
        _save(fig, out_dir, f"htap_analytical_{workload}_{name}_oltp_gc_{gc_choice}")
        plt.close(fig)


def plot_throughput_overviews(df: pd.DataFrame, out_dir: Path) -> None:
    """Compare all HTAP workloads in OLTP-left/OLAP-right rows, split by GC."""
    sub = df[df["workload"].isin(HTAP_WORKLOADS)]
    for gc_choice, gc_df in gc_slices(sub):
        workloads = [w for w in HTAP_WORKLOADS if w in set(gc_df["workload"])]
        row_height = 3.8 if compact_enabled() else 2.3
        fig, axes = plt.subplots(
            len(workloads), 2, figsize=(10, row_height * len(workloads)), squeeze=False,
        )
        for row, ((ax_oltp, ax_olap), workload) in enumerate(zip(axes, workloads)):
            wdf = gc_df[gc_df["workload"] == workload]
            if compact_enabled():
                # Keep the paper overview legible by showing a representative
                # power-of-two progression plus the measured endpoint.
                wdf = wdf[wdf["olap_threads"].isin(COMPACT_OLAP_THREADS)]
            thread_values = _olap_thread_values(wdf["olap_threads"])
            for engine in sorted(wdf["engine"].unique(), key=engine_sort_key):
                esub = wdf[wdf["engine"] == engine].sort_values("olap_threads")
                _plot_pair(
                    ax_oltp, ax_olap, esub["olap_threads"], thread_values,
                    esub["primary_metric_value"], esub["olap_qps"], engine,
                )
            title = HTAP_PANEL_LABELS.get(workload, workload)
            if compact_enabled():
                compact_title = HTAP_COMPACT_PANEL_LABELS.get(workload, title)
                ax_oltp.set_title(f"{compact_title} — OLTP")
                ax_olap.set_title(f"{compact_title} — OLAP")
            else:
                ax_oltp.set_title(f"{title}\nOLTP throughput")
                ax_olap.set_title(f"{title}\nOLAP throughput")
            if not compact_enabled():
                ax_oltp.set_ylabel("new_order/sec")
                ax_olap.set_ylabel("queries/sec")
            for ax in (ax_oltp, ax_olap):
                _set_olap_thread_axis(ax, thread_values)
                if compact_enabled():
                    ax.set_xlabel("OLAP threads" if row == len(workloads) - 1 else "")
                ax.set_ylim(bottom=0)
            ax_oltp.legend(fontsize="small")
        if compact_enabled():
            # Reserve room for the shared OLTP unit and a wider center gutter
            # for the shared OLAP unit.
            fig._compact_layout_left = 0.02
            fig._compact_layout_w_pad = 3.0
            fig.text(
                0.012, 0.5, "Throughput (new_order/sec)", rotation="vertical",
                va="center", ha="center", fontsize=12,
            )
            fig.text(
                0.525, 0.5, "Throughput (queries/sec)", rotation="vertical",
                va="center", ha="center", fontsize=12,
            )
        fig.suptitle(f"HTAP throughput overview - fixed OLTP terminals - GC {gc_choice}")
        fig.tight_layout()
        _save(fig, out_dir, f"htap_analytical_throughput_overview_gc_{gc_choice}")
        plt.close(fig)


def _latency_rows(df: pd.DataFrame, workload: str) -> pd.DataFrame:
    """Return rows containing an actual analytical-query latency sample."""
    sub = df[(df["workload"] == workload) & (df["scan_count"] > 0)].copy()
    latency_columns = ["scan_p50_us", "scan_p95_us", "scan_p99_us"]
    for column in latency_columns:
        sub[column] = pd.to_numeric(sub[column], errors="coerce")
    return sub.dropna(subset=latency_columns, how="all")


def plot_workload_latency_per_engine(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path,
) -> None:
    """Plot p50/p95/p99 query latency over the OLAP-thread sweep per engine."""
    sub = _latency_rows(df, workload)
    for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
        esub = sub[sub["engine"] == engine].sort_values("olap_threads")
        thread_values = _olap_thread_values(esub["olap_threads"])
        positions = _olap_thread_positions(esub["olap_threads"], thread_values)
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.fill_between(
            positions,
            esub["scan_p50_us"].tolist(),
            esub["scan_p99_us"].tolist(),
            color="#777777", alpha=0.12, linewidth=0, label="p50-p99 range",
        )
        for column, label in (
            ("scan_p50_us", "p50"),
            ("scan_p95_us", "p95"),
            ("scan_p99_us", "p99"),
        ):
            ax.plot(positions, esub[column], label=label, **latency_line_style(label))
        _set_olap_thread_axis(ax, thread_values)
        ax.set_ylabel("Query latency (microseconds)")
        ax.set_title(
            f"{ENGINE_LABELS.get(engine, engine)} - "
            f"{HTAP_LABELS.get(workload, workload)} latency - GC {gc_choice}"
        )
        ax.legend(frameon=True, framealpha=0.9, ncol=2)
        fig.tight_layout()
        _save(
            fig, out_dir,
            f"htap_analytical_{workload}_{engine}_latency_gc_{gc_choice}",
        )
        plt.close(fig)


def plot_workload_latency_all_engines(
    df: pd.DataFrame, workload: str, gc_choice: str, out_dir: Path,
) -> None:
    """Plot each latency percentile across engines without mixing percentile lines."""
    sub = _latency_rows(df, workload)
    if sub.empty:
        print(f"No {workload} latency rows for gc={gc_choice} - skipping latency plot.")
        return

    thread_values = _olap_thread_values(sub["olap_threads"])
    fig, axes = plt.subplots(1, 3, figsize=(16, 5), sharex=True)
    for ax, (column, percentile) in zip(
        axes,
        (("scan_p50_us", "p50"), ("scan_p95_us", "p95"), ("scan_p99_us", "p99")),
    ):
        for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
            esub = sub[sub["engine"] == engine].sort_values("olap_threads")
            ax.plot(
                _olap_thread_positions(esub["olap_threads"], thread_values), esub[column],
                label=ENGINE_LABELS.get(engine, engine), **engine_line_style(engine),
            )
        _set_olap_thread_axis(ax, thread_values)
        ax.set_ylabel("Query latency (microseconds)")
        ax.set_title(percentile.upper(), fontweight="bold")
        if ax is axes[0]:
            ax.legend(fontsize="small", frameon=True, framealpha=0.9)
    fig.suptitle(f"{HTAP_LABELS.get(workload, workload)} latency - GC {gc_choice}")
    fig.tight_layout()
    _save(
        fig, out_dir,
        f"htap_analytical_{workload}_all_engines_latency_gc_{gc_choice}",
    )
    plt.close(fig)


def plot_latency_overviews(df: pd.DataFrame, out_dir: Path) -> None:
    """Combine Q1/Q6 latency percentiles into workload rows, split by GC."""
    latency_workloads = ["htap_q1", "htap_q6"]
    sub = df[df["workload"].isin(latency_workloads)]
    percentiles = (
        ("scan_p50_us", "P50"),
        ("scan_p95_us", "P95"),
        ("scan_p99_us", "P99"),
    )

    for gc_choice, gc_df in gc_slices(sub):
        workloads = [
            workload for workload in latency_workloads
            if not _latency_rows(gc_df, workload).empty
        ]
        if not workloads:
            continue

        row_height = 3.8 if compact_enabled() else 2.3
        width = 13 if compact_enabled() else 16
        fig, axes = plt.subplots(
            len(workloads), 3,
            figsize=(width, row_height * len(workloads)),
            squeeze=False,
        )

        for row, workload in enumerate(workloads):
            wdf = _latency_rows(gc_df, workload)
            if compact_enabled():
                wdf = wdf[wdf["olap_threads"].isin(COMPACT_OLAP_THREADS)]
            thread_values = _olap_thread_values(wdf["olap_threads"])
            workload_title = (
                HTAP_COMPACT_PANEL_LABELS.get(workload, workload)
                if compact_enabled()
                else HTAP_PANEL_LABELS.get(workload, workload)
            )

            for column_index, (column, percentile) in enumerate(percentiles):
                ax = axes[row][column_index]
                for engine in sorted(wdf["engine"].unique(), key=engine_sort_key):
                    esub = wdf[wdf["engine"] == engine].sort_values("olap_threads")
                    ax.plot(
                        _olap_thread_positions(esub["olap_threads"], thread_values),
                        esub[column], label=ENGINE_LABELS.get(engine, engine),
                        **engine_line_style(engine),
                    )
                _set_olap_thread_axis(ax, thread_values)
                if compact_enabled():
                    ax.set_title(f"{workload_title} — {percentile}")
                    ax.set_xlabel("OLAP threads" if row == len(workloads) - 1 else "")
                else:
                    ax.set_title(f"{workload_title}\n{percentile} latency")
                ax.set_ylim(bottom=0)
                if row == 0 and column_index == 0:
                    ax.legend(fontsize="small", frameon=True, framealpha=0.9)

        fig.supylabel("Query latency (microseconds)")
        fig.suptitle(f"HTAP Q1/Q6 latency overview - GC {gc_choice}")
        fig.tight_layout()
        _save(fig, out_dir, f"htap_analytical_latency_overview_gc_{gc_choice}")
        plt.close(fig)


def plot_all(df: pd.DataFrame, out_dir: Path) -> None:
    """Generate the same full set of figures from either plotting entry point."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for workload in HTAP_WORKLOADS:
        workload_df = df[df["workload"] == workload]
        for gc_choice, gc_df in gc_slices(workload_df):
            plot_workload_per_engine(gc_df, workload, gc_choice, out_dir)
            plot_workload_all_engines(gc_df, workload, gc_choice, out_dir)
            plot_workload_oltp(gc_df, workload, gc_choice, out_dir)
            plot_workload_latency_per_engine(gc_df, workload, gc_choice, out_dir)
            plot_workload_latency_all_engines(gc_df, workload, gc_choice, out_dir)
    plot_throughput_overviews(df, out_dir)
    plot_latency_overviews(df, out_dir)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", required=True, type=Path)
    p.add_argument(
        "--compact", action="store_true",
        help=("use the shared paper-oriented layout: no overall title, a top legend, "
              "and smaller overview panels"),
    )
    args = p.parse_args()

    set_compact(args.compact)
    df = load_manifest(args.run_dir)
    out_dir = args.run_dir / "plots"
    plot_all(df, out_dir)
    print(f"Wrote plots to {out_dir}")


if __name__ == "__main__":
    main()
