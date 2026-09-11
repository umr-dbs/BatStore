#!/usr/bin/env python3
"""H5: OLTP throughput and analytical latency under increasing OLTP concurrency.

Vary OLTP terminals while fixing OLAP callers (default 2). Each caller
repeatedly runs Q1 (default) or Q6. Latencies cover whole query execution,
not individual table scans; percentiles pool completed queries across callers.
For every point, also report committed New-Orders/sec from the OLTP side.
"Low latency" needs an application-specific threshold; none is assumed here.

The main experiment uses 16 warehouses and stops at 32 OLTP terminals.  This
reduces warehouse/district hot-row contention without turning the headline
result into a severe CPU-oversubscription and memory-pressure experiment.

The shared parallel scan pool is fixed at 12 workers by default so concurrency
is the only scheduling variable.  Caller count is not total OS thread count.

For the scale-factor control, run the representative 4/16/32 points once at
each warehouse count (every other option remains identical):

    python3 scripts/h5.py --warehouse-sensitivity
"""
from __future__ import annotations

import argparse
import csv
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib.pyplot as plt

from hypothesis_common import configure_checkout, thread_counts, check_run, positive_int, record_setup, nonnegative_int, check_scan_samples

configure_checkout()
from engines import batstore, common
from plot_styles import finalize_layout, latency_line_style, measurement_positions, measurement_values, set_compact, set_measurement_axis

from h4 import run_point

DEFAULT_WAREHOUSES = 16
DEFAULT_OLTP_TERMINALS = [1, 2, 4, 8, 16, 32, 48, 64, 80, 96, 112, 128]
DEFAULT_SCAN_POOL_WORKERS = 12

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="h5_results")
    p.add_argument("--workload", default="htap_q1", choices=list(common.HTAP_CANONICAL_WORKLOADS))
    p.add_argument("--warehouses", type=positive_int, default=DEFAULT_WAREHOUSES)
    p.add_argument("--duration", type=positive_int, default=60)
    p.add_argument("--oltp-terminals", default=",".join(str(t) for t in DEFAULT_OLTP_TERMINALS),
                   help="Experiment B (H5): OLTP terminal counts to sweep, OLAP threads fixed by --fixed-olap-threads")
    p.add_argument("--fixed-olap-threads", type=positive_int, default=2)
    p.add_argument("--warehouse-sensitivity", action="store_true",
                   help="run the 8-vs-16 warehouse control at 4,16,32 terminals instead of the main sweep")
    p.add_argument("--sensitivity-warehouses", default="8,16",
                   help="warehouse counts for --warehouse-sensitivity (default: 8,16)")
    p.add_argument("--sensitivity-terminals", default="4,16,32",
                   help="terminal counts for --warehouse-sensitivity (default: 4,16,32)")
    p.add_argument("--scan-pool-workers", type=nonnegative_int, default=DEFAULT_SCAN_POOL_WORKERS,
                   help=f"fixed extra shared scan workers (default: {DEFAULT_SCAN_POOL_WORKERS}; 0=disabled)")
    p.add_argument("--gc", choices=["on", "off"], default="on")
    p.add_argument("--skip-build", action="store_true")
    p.add_argument("--compact", action="store_true", help="use a paper-friendly layout with a shared legend")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    set_compact(args.compact)
    if args.warehouse_sensitivity:
        warehouse_list = thread_counts(args.sensitivity_warehouses)
        oltp_terminal_list = thread_counts(args.sensitivity_terminals)
    else:
        warehouse_list = [args.warehouses]
        oltp_terminal_list = thread_counts(args.oltp_terminals)
    if not args.skip_build:
        batstore.ensure_built()
    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    varies = (f"warehouses={warehouse_list}, OLTP terminals={oltp_terminal_list}"
              if args.warehouse_sensitivity else f"OLTP terminals={oltp_terminal_list}")
    fixed_warehouses = "varied" if args.warehouse_sensitivity else str(args.warehouses)
    record_setup(run_dir, "H5", args, varies=varies,
                 fixed=f"OLAP callers={args.fixed_olap_threads}, query={args.workload}, warehouses={fixed_warehouses}, duration={args.duration}s, GC={args.gc}, scan pool={args.scan_pool_workers}",
                 measures="committed New-Order throughput and per-query OLAP latency p50/p95/p99 across completed queries")
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)
    summary_path = run_dir / "h5_summary.csv"
    with summary_path.open("w", newline="") as f:
        csv.writer(f).writerow([
            "warehouses", "oltp_terminals", "fixed_olap_threads",
            "new_order_per_sec", "olap_p50_us", "olap_p95_us", "olap_p99_us",
            "olap_avg_us", "olap_query_count",
        ])

    # OLTP terminals are the only independent variable in the main experiment. The
    # OLAP caller count and shared scan-pool size remain fixed at every point.
    measurements_by_warehouse = {}
    for warehouses in warehouse_list:
        measurements = {}
        for terminals in oltp_terminal_list:
            out_dir = (run_dir / "h5_fixed_olap_vs_oltp_terminals" /
                       f"warehouses_{warehouses}" / f"terminals_{terminals}")
            result = run_point(args.workload, warehouses, terminals, args.fixed_olap_threads,
                               args.duration, args.gc, out_dir, scan_pool_workers=args.scan_pool_workers)
            result.config_label = f"{result.config_label} terminals={terminals}"
            common.append_manifest_row(manifest_path, result)
            check_run(result, out_dir)
            check_scan_samples(result, out_dir)
            measurements[terminals] = {
                "throughput": result.primary_metric_value,
                "p50": result.scan_p50_us, "p95": result.scan_p95_us,
                "p99": result.scan_p99_us, "avg": result.scan_avg_us,
                "count": result.scan_count,
            }
            with summary_path.open("a", newline="") as f:
                csv.writer(f).writerow([
                    warehouses, terminals, args.fixed_olap_threads,
                    f"{result.primary_metric_value:.3f}", f"{result.scan_p50_us:.2f}",
                    f"{result.scan_p95_us:.2f}", f"{result.scan_p99_us:.2f}",
                    f"{result.scan_avg_us:.2f}", result.scan_count,
                ])
            status = result.notes or "OK"
            print(f"[H5] warehouses={warehouses:2d} terminals={terminals:3d}  "
                  f"oltp={result.primary_metric_value:10.2f} {result.primary_metric_name}  "
                  f"query p50={result.scan_p50_us:9.1f}us  p95={result.scan_p95_us:9.1f}us  "
                  f"p99={result.scan_p99_us:9.1f}us  [{status}]")
        measurements_by_warehouse[warehouses] = measurements

    print(f"\nmanifest : {manifest_path}")
    print(f"summary  : {summary_path}")
    if args.warehouse_sensitivity:
        plot_warehouse_sensitivity(
            warehouse_list, oltp_terminal_list, measurements_by_warehouse,
            args.fixed_olap_threads, run_dir / "plots",
        )
    else:
        plot(oltp_terminal_list, measurements_by_warehouse[args.warehouses],
             args.fixed_olap_threads, run_dir / "plots")


def plot(oltp_terminal_list, measurements, fixed_olap_threads, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig, (ax_tp, ax_latency) = plt.subplots(1, 2, figsize=(12, 4.8))
    axis_b = measurement_values(oltp_terminal_list)
    positions_b = measurement_positions(oltp_terminal_list, axis_b)

    throughput = [measurements[t]["throughput"] for t in oltp_terminal_list]
    ax_tp.plot(positions_b, throughput, color="#222222", marker="o", markersize=7,
               markeredgecolor="white", markeredgewidth=0.8, linewidth=2.2)
    set_measurement_axis(ax_tp, oltp_terminal_list, "OLTP terminals")
    ax_tp.set_ylabel("Committed New-Orders/sec")
    ax_tp.set_title(f"OLTP throughput (OLAP callers={fixed_olap_threads})")
    ax_tp.grid(axis="y", alpha=0.3)

    for pct in ("p50", "p95", "p99"):
        values_ms = [measurements[t][pct] / 1000.0 for t in oltp_terminal_list]
        ax_latency.plot(positions_b, values_ms, label=pct, **latency_line_style(pct))
    set_measurement_axis(ax_latency, oltp_terminal_list, "OLTP terminals")
    ax_latency.set_ylabel("OLAP query latency (ms)")
    ax_latency.set_title(f"OLAP latency (OLAP callers={fixed_olap_threads})")
    ax_latency.grid(axis="y", alpha=0.3)
    ax_latency.legend(frameon=False)

    fig.suptitle("H5: Fixed OLAP workers, increasing OLTP concurrency")
    finalize_layout(fig)
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h5_oltp_throughput_and_olap_latency.{ext}", dpi=150)
    plt.close(fig)


def plot_warehouse_sensitivity(
    warehouse_list, oltp_terminal_list, measurements_by_warehouse,
    fixed_olap_threads, out_dir: Path,
) -> None:
    """Compare scale factors without mixing them into the main H5 curve."""
    out_dir.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), squeeze=False)
    ax_tp, ax_latency = axes[0]
    axis_values = measurement_values(oltp_terminal_list)
    positions = measurement_positions(oltp_terminal_list, axis_values)
    for warehouses in warehouse_list:
        per_terminal = measurements_by_warehouse[warehouses]
        label = f"{warehouses} warehouses"
        ax_tp.plot(positions, [per_terminal[t]["throughput"] for t in oltp_terminal_list],
                   marker="o", linewidth=2, label=label)
        ax_latency.plot(positions, [per_terminal[t]["p99"] / 1000.0 for t in oltp_terminal_list],
                        marker="o", linewidth=2, label=label)
    for ax in (ax_tp, ax_latency):
        set_measurement_axis(ax, oltp_terminal_list, "OLTP terminals")
        ax.grid(alpha=0.3)
        ax.legend(frameon=False)
    ax_tp.set_ylabel("Committed New-Orders/sec")
    ax_tp.set_title("OLTP throughput")
    ax_latency.set_ylabel("p99 OLAP query latency (ms)")
    ax_latency.set_title("OLAP latency")
    fig.suptitle(f"H5 scale-factor control (OLAP callers={fixed_olap_threads})")
    finalize_layout(fig)
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h5_warehouse_sensitivity.{ext}", dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    main()
