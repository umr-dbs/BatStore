#!/usr/bin/env python3
"""H5: Analytical query latency under increasing transactional concurrency.

Vary OLTP terminals while fixing OLAP callers (default 2). Each caller
repeatedly runs Q1 (default) or Q6. Latencies cover whole query execution,
not individual table scans; percentiles pool completed queries across callers.
"Low latency" needs an application-specific threshold; none is assumed here.

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

from h4 import run_point

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="h5_results")
    p.add_argument("--workload", default="htap_q1", choices=list(common.HTAP_CANONICAL_WORKLOADS))
    p.add_argument("--warehouses", type=positive_int, default=8)
    p.add_argument("--duration", type=positive_int, default=60)
    p.add_argument("--oltp-terminals", default=",".join(str(t) for t in [1, 2, 4, 8, 16, 32, 48, 64, 80, 96, 112, 128]),
                   help="Experiment B (H5): OLTP terminal counts to sweep, OLAP threads fixed by --fixed-olap-threads")
    p.add_argument("--fixed-olap-threads", type=positive_int, default=2)
    p.add_argument("--scan-pool-workers", type=nonnegative_int, default=None,
                   help="extra shared scan workers: omitted=driver auto, 0=disabled, N=fixed pool")
    p.add_argument("--gc", choices=["on", "off"], default="on")
    p.add_argument("--skip-build", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    oltp_terminal_list = thread_counts(args.oltp_terminals)
    if not args.skip_build:
        batstore.ensure_built()
    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    record_setup(run_dir, "H5", args, varies=f"OLTP terminals={oltp_terminal_list}",
                 fixed=f"OLAP callers={args.fixed_olap_threads}, query={args.workload}, warehouses={args.warehouses}, duration={args.duration}s, GC={args.gc}, scan pool={args.scan_pool_workers if args.scan_pool_workers is not None else 'auto'}",
                 measures="per-query latency p50/p95/p99 across completed queries, with sample count in manifest")
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)
    # --- Experiment B (H5): OLAP scan latency vs. OLTP terminal count ---
    olap_latency = {}
    for terminals in oltp_terminal_list:
        out_dir = run_dir / "h5_olap_latency_vs_oltp_terminals" / f"terminals_{terminals}"
        result = run_point(args.workload, args.warehouses, terminals, args.fixed_olap_threads,
                            args.duration, args.gc, out_dir, scan_pool_workers=args.scan_pool_workers)
        result.config_label = f"{result.config_label} terminals={terminals}"
        common.append_manifest_row(manifest_path, result)
        check_run(result, out_dir)
        check_scan_samples(result, out_dir)
        olap_latency[terminals] = {
            "p50": result.scan_p50_us, "p95": result.scan_p95_us, "p99": result.scan_p99_us,
        }
        status = result.notes or "OK"
        print(f"[H5] terminals={terminals:3d}  query p50={result.scan_p50_us:9.1f}us  "
              f"p95={result.scan_p95_us:9.1f}us  p99={result.scan_p99_us:9.1f}us  [{status}]")

    print(f"\nmanifest : {manifest_path}")
    plot(oltp_terminal_list, olap_latency, args.fixed_olap_threads, run_dir / "plots")


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

    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h5_scan_latency.{ext}", dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    main()
