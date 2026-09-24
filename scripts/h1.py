#!/usr/bin/env python3
"""H1: Snapshot-isolation overhead relative to AutoCommit.

Compare single-operation write commit paths at each OLTP thread count.
YCSB A (50% reads / 50% updates) is the main experiment; read-only YCSB C
is a control because reads use the same MVCC path in both modes.
This is not a comparison of multi-statement transactions.

Fixed: record count, duration, GC, allocator and scrambled Zipfian theta=0.99.
Measured: throughput, sampled read/update latency, and peak RSS.
h1_comparison.csv reports SI throughput loss: 100 * (1 - SI / AutoCommit).
Negative loss means SI was faster in that measurement. No pass/fail threshold
for "small overhead" is assumed. Repeat runs to assess measurement variability.
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

from hypothesis_common import configure_checkout, thread_counts, check_run, read_op_latency, positive_int, record_setup

configure_checkout()
from engines import batstore, common
from plot_styles import (compact_enabled, finalize_layout, measurement_positions,
                         measurement_values, set_compact, set_measurement_axis)
from plot_h1_read_p50_vs_updates import plot as plot_read_p50_vs_updates

DEFAULT_THREADS = [1, 2, 4, 8, 16, 32, 48, 64, 80, 96, 112, 128]
WORKLOADS = ["ycsb_a", "ycsb_c"]
MODES = ["atomic", "transaction"]
MODE_LABELS = {"atomic": "Auto-commit", "transaction": "SI"}
# Mode-specific colors kept separate from the stable engine palette used by
# the other hypothesis plots. Markers provide redundant identification.
MODE_COLORS = {"atomic": "#4C566A", "transaction": "#C62828"}
MODE_MARKERS = {"atomic": "*", "transaction": "+"}
PERCENTILE_STYLE = {"p50": "-", "p99": "--"}
LABEL_FONT_SIZE = 14
TICK_FONT_SIZE = 13
TITLE_FONT_SIZE = 14
LEGEND_FONT_SIZE = 13


def _style_plot_text(ax) -> None:
    """Keep H1 text as readable as the labels in the other result plots."""
    ax.xaxis.label.set_size(LABEL_FONT_SIZE)
    ax.yaxis.label.set_size(LABEL_FONT_SIZE)
    ax.title.set_size(TITLE_FONT_SIZE)
    ax.tick_params(axis="both", which="both", labelsize=TICK_FONT_SIZE)


def _mode_legend_handles() -> list[Line2D]:
    """Marker-only mode keys; line style is reserved for percentiles."""
    return [
        Line2D(
            [], [], color=MODE_COLORS[mode], marker=MODE_MARKERS[mode],
            linestyle="None", markersize=9 if mode == "atomic" else 10,
            markeredgecolor=MODE_COLORS[mode], markeredgewidth=1.7,
            label=MODE_LABELS[mode],
        )
        for mode in MODES
    ]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="h1_results")
    p.add_argument("--threads", default=",".join(str(t) for t in DEFAULT_THREADS))
    p.add_argument("--workloads", default=",".join(WORKLOADS))
    p.add_argument("--records", type=positive_int, default=2_000_000)
    p.add_argument("--duration", type=positive_int, default=20)
    p.add_argument("--gc", choices=["on", "off"], default="on")
    p.add_argument("--skip-build", action="store_true")
    p.add_argument("--batstore-allocator", choices=["jemalloc", "mimalloc"], default="jemalloc")
    p.add_argument("--compact", action="store_true", help="use a paper-friendly layout with a shared legend")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    set_compact(args.compact)
    os.environ["BATSTORE_ALLOCATOR"] = args.batstore_allocator

    threads_list = thread_counts(args.threads)
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]

    if not workloads or any(w not in WORKLOADS for w in workloads):
        sys.exit("--workloads must contain ycsb_a and/or ycsb_c")

    if not args.skip_build:
        print("[build] batstore...")
        batstore.ensure_built()

    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    record_setup(run_dir, "H1", args, varies="execution mode and OLTP threads",
                 fixed=f"records={args.records}, duration={args.duration}s, GC={args.gc}, theta=0.99",
                 measures="throughput, SI throughput loss vs AutoCommit, operation latency; C is a read-only control")
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)
    op_latency_path = run_dir / "h1_op_latency.csv"
    op_latency_path.parent.mkdir(parents=True, exist_ok=True)
    with open(op_latency_path, "w", newline="") as f:
        csv.writer(f).writerow([
            "workload", "mode", "threads", "operation",
            "p50_us", "p95_us", "p99_us", "avg_us", "count",
        ])

    print("\n########## H1: autocommit vs. SI-transaction overhead ##########")
    print(f"run directory : {run_dir}")
    print(f"workloads     : {workloads}")
    print(f"threads       : {threads_list}")
    print(f"modes         : {MODES}")
    print("###################################################################\n")

    scale_base = common.Scale(ycsb_records=args.records, ycsb_duration=args.duration)

    results = {w: {m: {} for m in MODES} for w in workloads}
    for workload in workloads:
        ops_to_track = ["read", "update"] if workload == "ycsb_a" else ["read"]
        for index, threads in enumerate(threads_list):
            # Pair modes closely and alternate order to reduce systematic run-order bias.
            for mode in (MODES if index % 2 == 0 else list(reversed(MODES))):
                os.environ["BATSTORE_YCSB_MODE"] = mode
                scale = dataclasses.replace(
                    scale_base, ycsb_threads=threads,
                    label=f"h1 mode={mode} threads={threads}",
                )
                out_dir = run_dir / workload / mode / f"threads_{threads}"
                result = batstore.run(workload, scale, out_dir, gc=args.gc)
                result.config_label = f"{result.config_label} mode={mode}"
                common.append_manifest_row(manifest_path, result)
                check_run(result, out_dir)

                op_rows = {}
                op_csv = out_dir / "ycsb_operation_latency_summary.csv"
                with open(op_latency_path, "a", newline="") as f:
                    w = csv.writer(f)
                    for op in ops_to_track:
                        lat = read_op_latency(op_csv, op)
                        op_rows[op] = lat
                        w.writerow([
                            workload, mode, threads, op,
                            f"{lat['p50']:.2f}", f"{lat['p95']:.2f}", f"{lat['p99']:.2f}",
                            f"{lat['avg']:.2f}", lat["count"],
                        ])

                results[workload][mode][threads] = {
                    "throughput": result.primary_metric_value,
                    "peak_rss_mb": result.peak_rss_mb,
                    "ops": op_rows,
                }
                status = result.notes or "OK"
                print(f"{workload:8s} mode={mode:11s} threads={threads:3d}  "
                      f"throughput={result.primary_metric_value:10.1f} ops/s  "
                      f"peak_rss={result.peak_rss_mb:7.1f} MB  [{status}]")

    with (run_dir / "h1_comparison.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["workload", "threads", "autocommit_ops_sec", "si_ops_sec", "si_throughput_loss_pct"])
        for workload, per_mode in results.items():
            for threads in threads_list:
                atomic = per_mode["atomic"][threads]["throughput"]
                si = per_mode["transaction"][threads]["throughput"]
                writer.writerow([workload, threads, atomic, si, 100 * (1 - si / atomic)])

    print(f"\nmanifest         : {manifest_path}")
    print(f"op latency table : {op_latency_path}")
    plot(results, threads_list, run_dir / "plots")
    if "ycsb_a" in workloads:
        plot_read_p50_vs_updates(run_dir)
        plot_read_p50_vs_updates(run_dir, "total_ops")


def plot(results: dict, threads_list: list, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    display_threads = threads_list
    if compact_enabled():
        compact_threads = {1, 2, 4, 8, 16, 32, 64, 128}
        display_threads = [threads for threads in threads_list if threads in compact_threads]
    axis_values = measurement_values(display_threads)

    # --- Figure 1: throughput, atomic vs. transaction, one panel per workload ---
    throughput_size = (10.5, 3.2) if compact_enabled() else (6.5 * len(results), 5)
    fig, axes = plt.subplots(1, len(results), figsize=throughput_size, squeeze=False)
    axes = axes[0]
    for ax, (workload, per_mode) in zip(axes, results.items()):
        for mode in MODES:
            values = [per_mode[mode][t]["throughput"] / 1_000_000 for t in display_threads]
            positions = measurement_positions(display_threads, axis_values)
            ax.plot(positions, values, label=MODE_LABELS[mode], color=MODE_COLORS[mode],
                     marker=MODE_MARKERS[mode], markersize=9 if mode == "atomic" else 10,
                     markeredgecolor=MODE_COLORS[mode],
                     markeredgewidth=1.7,
                     linewidth=2.0)
        set_measurement_axis(ax, display_threads, "Workers")
        ax.set_ylabel("Throughput (million ops/s)")
        if ax is not axes[0]:
            ax.set_ylabel("")
        if workload == "ycsb_c":
            ax.set_yticks([1, 5, 10])
        ax.set_title(workload.replace("_", " ").upper(), pad=8)
        ax.grid(axis="y", alpha=0.25)
        _style_plot_text(ax)
    if compact_enabled():
        handles = _mode_legend_handles()
        section_handle = Line2D([], [], linestyle="None", marker=None, alpha=0)
        legend = fig.legend(
            [section_handle, *handles], ["Mode:", "Auto-commit", "SI"],
            loc="upper center", bbox_to_anchor=(0.5, 0.97),
            ncol=3, frameon=False, fontsize=LEGEND_FONT_SIZE,
            handlelength=0.6, handletextpad=0.25, columnspacing=1.1,
        )
        legend.get_texts()[0].set_fontweight("semibold")
        fig.tight_layout(rect=(0, 0, 1, 0.86), w_pad=2.8)
    else:
        handles = _mode_legend_handles()
        for ax in axes:
            ax.legend(
                handles=handles, frameon=False, fontsize=LEGEND_FONT_SIZE,
                handlelength=0.6, handletextpad=0.25,
            )
        fig.suptitle("H1: throughput, autocommit vs. SI transaction")
        finalize_layout(fig)
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h1_throughput.{ext}", dpi=150)
    plt.close(fig)

    # --- Figure 2: per-operation latency (p50 solid / p99 dashed) ---
    latency_panels = []
    for workload in ("ycsb_a", "ycsb_c"):
        ops_present = sorted({op for per_mode in results.get(workload, {}).values()
                              for t in per_mode.values() for op in t["ops"]})
        latency_panels.extend((workload, op) for op in ops_present)
    if latency_panels:
        panel_count = len(latency_panels)
        latency_size = ((4.2 * panel_count, 2.9) if compact_enabled()
                        else (6.5 * panel_count, 5))
        fig, axes_array = plt.subplots(
            1, panel_count, figsize=latency_size, squeeze=False,
        )
        axes = list(axes_array[0])
        for panel_index, (ax, (workload, op)) in enumerate(zip(axes, latency_panels)):
            for mode in MODES:
                positions = measurement_positions(display_threads, axis_values)
                for pct in ("p50", "p99"):
                    values = [results[workload][mode][t]["ops"].get(op, {}).get(pct, 0.0)
                              for t in display_threads]
                    ax.plot(positions, values, color=MODE_COLORS[mode], linestyle=PERCENTILE_STYLE[pct],
                             marker=MODE_MARKERS[mode], markersize=8 if mode == "atomic" else 9,
                             markeredgecolor=MODE_COLORS[mode],
                             markeredgewidth=1.6,
                             linewidth=1.8, label=f"{MODE_LABELS[mode]} · {pct}")
            set_measurement_axis(ax, display_threads, "Workers")
            ax.set_ylabel("Latency (µs)")
            if panel_index > 0:
                ax.set_ylabel("")
            ax.set_yscale("log")
            if op == "update":
                ax.set_ylim(0.8, 12_000)
                # Keep the sparse log scale while making its lower range
                # explicit; Matplotlib otherwise labels only 10² and 10⁴ here.
                ax.set_yticks([1, 100, 10_000])
            workload_title = workload.replace("_", " ").upper()
            workload_math = workload_title.replace(" ", r"\ ")
            ax.set_title(
                rf"$\mathbf{{{workload_math}}}$: {op.capitalize()} latency", pad=8,
            )
            ax.grid(axis="y", which="major", alpha=0.25)
            _style_plot_text(ax)
        if compact_enabled():
            mode_handles = _mode_legend_handles()
            percentile_handles = [
                Line2D([0], [0], color="#333333", linestyle=PERCENTILE_STYLE[pct],
                       linewidth=2, label=pct)
                for pct in ("p50", "p99")
            ]
            section_handle = Line2D([], [], linestyle="None", marker=None, alpha=0)
            shared_handles = [
                section_handle, *mode_handles, section_handle, *percentile_handles,
            ]
            shared_labels = ["Mode:", "Auto-commit", "SI", "Percentile:", "p50", "p99"]
            legend = fig.legend(
                shared_handles, shared_labels,
                loc="upper center", bbox_to_anchor=(0.5, 0.96), ncol=6,
                frameon=False, fontsize=LEGEND_FONT_SIZE,
                columnspacing=0.7, handlelength=1.0, handletextpad=0.2,
            )
            for index in (0, 3):
                legend.get_texts()[index].set_fontweight("semibold")
            fig.tight_layout(rect=(0, 0.02, 1, 0.78), w_pad=1.0)
        else:
            for ax in axes:
                ax.legend(frameon=False, fontsize=LEGEND_FONT_SIZE)
            fig.suptitle("H1: per-operation latency, autocommit vs. SI transaction")
            finalize_layout(fig)
        for ext in ("pdf", "png"):
            fig.savefig(out_dir / f"h1_latency.{ext}", dpi=150)
        plt.close(fig)

    print(f"plots            : {out_dir}")


if __name__ == "__main__":
    main()
