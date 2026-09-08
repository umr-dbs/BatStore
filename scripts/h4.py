#!/usr/bin/env python3
"""H4: OLTP throughput under increasing analytical concurrency.

Vary OLAP callers while fixing OLTP terminals (default 4). Each caller
repeatedly runs Q1 (default) or Q6; queries are separate configurations.
Report committed New-Orders/sec relative to the mandatory zero-OLAP baseline.

The shared parallel scan pool is additional to OLAP callers. Its default auto
size can grow with concurrency; use --scan-pool-workers N for a fixed pool
or 0 for no extra pool. Caller count is not total OS thread count.
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
from plot_styles import latency_line_style, measurement_positions, measurement_values, set_measurement_axis

DEFAULT_OLAP_THREADS = [0, 1, 2, 4, 8, 16, 32, 48, 64, 80, 96, 112, 128]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="h4_results")
    p.add_argument("--workload", default="htap_q1", choices=list(common.HTAP_CANONICAL_WORKLOADS))
    p.add_argument("--warehouses", type=positive_int, default=8)
    p.add_argument("--duration", type=positive_int, default=60)
    p.add_argument("--olap-threads", default=",".join(str(t) for t in DEFAULT_OLAP_THREADS),
                   help="Experiment A (H4): OLAP thread counts to sweep, OLTP terminals fixed by --fixed-oltp-terminals")
    p.add_argument("--fixed-oltp-terminals", type=positive_int, default=4)
    p.add_argument("--scan-pool-workers", type=nonnegative_int, default=None,
                   help="extra shared scan workers: omitted=driver auto, 0=disabled, N=fixed pool")
    p.add_argument("--gc", choices=["on", "off"], default="on")
    p.add_argument("--skip-build", action="store_true")
    return p.parse_args()


def run_point(workload: str, warehouses: int, terminals: int, olap_threads: int, duration: int, gc: str,
              out_dir: Path, scan_pool_workers=None) -> common.NormalizedResult:
    scale = common.Scale(tpcc_warehouses=warehouses, tpcc_terminals=terminals, tpcc_duration=duration,
                          htap_olap_threads=olap_threads,
                          label=f"h4/h5 terminals={terminals} olap_threads={olap_threads}")
    return batstore.run(workload, scale, out_dir, gc=gc, scan_pool_workers=scan_pool_workers)


def main() -> None:
    args = parse_args()
    olap_thread_list = thread_counts(args.olap_threads, allow_zero=True)
    olap_thread_list = [0] + [t for t in olap_thread_list if t != 0]
    if not args.skip_build:
        print("[build] batstore...")
        batstore.ensure_built()

    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    record_setup(run_dir, "H4", args, varies=f"OLAP callers={olap_thread_list}",
                 fixed=f"OLTP terminals={args.fixed_oltp_terminals}, query={args.workload}, warehouses={args.warehouses}, duration={args.duration}s, GC={args.gc}, scan pool={args.scan_pool_workers if args.scan_pool_workers is not None else 'auto'}",
                 measures="New-Order throughput, normalized to the zero-OLAP baseline")
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)

    print("\n########## H4: Increasing analytical concurrency ##########")
    print(f"run directory        : {run_dir}")
    print(f"workload              : {args.workload}")
    print(f"[H4] olap_threads     : {olap_thread_list} (oltp_terminals fixed at {args.fixed_oltp_terminals})")
    print("#############################################################\n")

    # --- Experiment A (H4): OLTP throughput vs. OLAP thread count ---
    oltp_throughput = {}
    for olap_threads in olap_thread_list:
        out_dir = run_dir / "h4_oltp_vs_olap_threads" / f"olap_threads_{olap_threads}"
        result = run_point(args.workload, args.warehouses, args.fixed_oltp_terminals, olap_threads,
                            args.duration, args.gc, out_dir, scan_pool_workers=args.scan_pool_workers)
        result.config_label = f"{result.config_label} olap_threads={olap_threads}"
        common.append_manifest_row(manifest_path, result)
        check_run(result, out_dir)
        if olap_threads > 0:
            check_scan_samples(result, out_dir)
        oltp_throughput[olap_threads] = result.primary_metric_value
        status = result.notes or "OK"
        print(f"[H4] olap_threads={olap_threads:3d}  oltp={result.primary_metric_value:10.2f} "
              f"{result.primary_metric_name}  [{status}]")

    print(f"\nmanifest : {manifest_path}")
    plot(olap_thread_list, oltp_throughput, args.fixed_oltp_terminals, run_dir / "plots")


def plot(olap_thread_list, oltp_throughput, fixed_oltp_terminals, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig, ax_h4 = plt.subplots(figsize=(8, 5))

    baseline = oltp_throughput[0]
    axis_a = measurement_values(olap_thread_list)
    positions_a = measurement_positions(olap_thread_list, axis_a)
    relative = [100.0 * oltp_throughput[t] / baseline if baseline else 0.0 for t in olap_thread_list]
    ax_h4.plot(positions_a, relative, color="#222222", marker="o", markersize=7,
               markeredgecolor="white", markeredgewidth=0.8, linewidth=2.2)
    ax_h4.axhline(100.0, color="#999999", linestyle=":", linewidth=1.2)
    set_measurement_axis(ax_h4, olap_thread_list, "OLAP threads")
    ax_h4.set_ylabel(f"OLTP throughput (% of olap_threads=0, oltp_terminals={fixed_oltp_terminals})")
    ax_h4.set_title("H4: OLTP throughput vs. OLAP thread count")

    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h4_oltp_vs_olap.{ext}", dpi=150)
    plt.close(fig)
    print(f"plots    : {out_dir}")


if __name__ == "__main__":
    main()
