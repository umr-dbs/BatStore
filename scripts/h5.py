#!/usr/bin/env python3
"""H5: Analytical query latency under increasing transactional concurrency.

Vary OLTP terminals while fixing OLAP callers (default 2). Each caller
repeatedly runs Q1 (default) or Q6. Latencies cover whole query execution,
not individual table scans; percentiles pool completed queries across callers.
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
DEFAULT_OLTP_TERMINALS = [1, 2, 4, 8, 16, 24, 32]
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
                 measures="per-query latency p50/p95/p99 across completed queries, with sample count in manifest")
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)
    # --- Experiment B (H5): OLAP scan latency vs. OLTP terminal count ---
    latency_by_warehouse = {}
    for warehouses in warehouse_list:
        olap_latency = {}
        for terminals in oltp_terminal_list:
            out_dir = (run_dir / "h5_olap_latency_vs_oltp_terminals" /
                       f"warehouses_{warehouses}" / f"terminals_{terminals}")
            result = run_point(args.workload, warehouses, terminals, args.fixed_olap_threads,
                               args.duration, args.gc, out_dir, scan_pool_workers=args.scan_pool_workers)
            result.config_label = f"{result.config_label} terminals={terminals}"
            common.append_manifest_row(manifest_path, result)
            check_run(result, out_dir)
            check_scan_samples(result, out_dir)
            olap_latency[terminals] = {
                "p50": result.scan_p50_us, "p95": result.scan_p95_us, "p99": result.scan_p99_us,
            }
            status = result.notes or "OK"
            print(f"[H5] warehouses={warehouses:2d} terminals={terminals:3d}  "
                  f"query p50={result.scan_p50_us:9.1f}us  p95={result.scan_p95_us:9.1f}us  "
                  f"p99={result.scan_p99_us:9.1f}us  [{status}]")
        latency_by_warehouse[warehouses] = olap_latency

    print(f"\nmanifest : {manifest_path}")
    if args.warehouse_sensitivity:
        plot_warehouse_sensitivity(
            warehouse_list, oltp_terminal_list, latency_by_warehouse,
            args.fixed_olap_threads, run_dir / "plots",
        )
    else:
        plot(oltp_terminal_list, latency_by_warehouse[args.warehouses],
             args.fixed_olap_threads, run_dir / "plots")


def plot(oltp_terminal_list, olap_latency, fixed_olap_threads, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig, ax_h5 = plt.subplots(figsize=(8, 5))
    axis_b = measurement_values(oltp_terminal_list)
    positions_b = measurement_positions(oltp_terminal_list, axis_b)
    for pct in ("p50", "p95", "p99"):
        values = [olap_latency[t][pct] for t in oltp_terminal_list]
        ax_h5.plot(positions_b, values, label=pct, **latency_line_style(pct))
    set_measurement_axis(ax_h5, oltp_terminal_list, "OLTP terminals")
    ax_h5.set_ylabel(f"OLAP query latency (µs, olap_threads={fixed_olap_threads})")
    ax_h5.set_title("H5: OLAP query latency vs. OLTP terminal count")
    ax_h5.legend(frameon=False)

    finalize_layout(fig)
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h5_scan_latency.{ext}", dpi=150)
    plt.close(fig)


def plot_warehouse_sensitivity(
    warehouse_list, oltp_terminal_list, latency_by_warehouse,
    fixed_olap_threads, out_dir: Path,
) -> None:
    """Compare scale factors without mixing them into the main H5 curve."""
    out_dir.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), squeeze=False)
    for ax, pct in zip(axes[0], ("p50", "p99")):
        for warehouses in warehouse_list:
            values = [latency_by_warehouse[warehouses][t][pct] / 1000 for t in oltp_terminal_list]
            ax.plot(oltp_terminal_list, values, marker="o", linewidth=2,
                    label=f"{warehouses} warehouses")
        ax.set_xlabel("OLTP terminals")
        ax.set_ylabel(f"{pct} query latency (ms)")
        ax.set_title(pct)
        ax.grid(alpha=0.3)
        ax.legend(frameon=False)
    fig.suptitle(f"H5 scale-factor control (OLAP callers={fixed_olap_threads})")
    finalize_layout(fig)
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h5_warehouse_sensitivity.{ext}", dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    main()
