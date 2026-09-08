#!/usr/bin/env python3
"""H4 + H5: OLTP and OLAP throughput/latency stay decoupled from each other's thread count.

    H4) BatStore besitzt einen konstanten OLTP-Throughput unabhaengig von der
        Anzahl der parallel laufenden Scans.
    H5) BatStore bietet eine niedrige Latenz von Scan-Anfragen unabhaengig
        von der Anzahl der OLTP Threads.

Both hypotheses are two ends of the same HTAP interference question (does
one side of the workload degrade the other?), and both reuse the exact same
htap_q1/htap_q6 (CH-benCHmark) driver machinery as
`scripts/run_htap_analytical_sweep.py` - so this script runs two sweeps
against that same knob set instead of introducing a second driver mode:

  Experiment A (H4): fix OLTP terminals, sweep the number of dedicated OLAP
  threads (0 = pure-OLTP baseline). Plot OLTP throughput relative to that
  olap_threads=0 baseline - flat at ~100% supports H4.

  Experiment B (H5): fix the number of OLAP threads, sweep OLTP terminals.
  Plot OLAP scan latency (p50/p95/p99, already parsed by batstore.py's
  run() from tpcc_scan.csv) - flat lines support H5.

Usage:
    python3 scripts/h4.py
    python3 scripts/h4.py --workload htap_q6 --olap-threads 0,1,2,4,8,16 --oltp-terminals 1,2,4,8,16
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib.pyplot as plt

from engines import batstore, common
from plot_styles import latency_line_style, measurement_positions, measurement_values, set_measurement_axis

DEFAULT_OLAP_THREADS = [0, 1, 2, 4, 8, 16]
DEFAULT_OLTP_TERMINALS = [1, 2, 4, 8, 16]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="h4_results")
    p.add_argument("--workload", default="htap_q1", choices=list(common.HTAP_CANONICAL_WORKLOADS))
    p.add_argument("--warehouses", type=int, default=8)
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--olap-threads", default=",".join(str(t) for t in DEFAULT_OLAP_THREADS),
                   help="Experiment A (H4): OLAP thread counts to sweep, OLTP terminals fixed by --fixed-oltp-terminals")
    p.add_argument("--fixed-oltp-terminals", type=int, default=4)
    p.add_argument("--oltp-terminals", default=",".join(str(t) for t in DEFAULT_OLTP_TERMINALS),
                   help="Experiment B (H5): OLTP terminal counts to sweep, OLAP threads fixed by --fixed-olap-threads")
    p.add_argument("--fixed-olap-threads", type=int, default=2)
    p.add_argument("--gc", choices=["on", "off"], default="on")
    p.add_argument("--skip-build", action="store_true")
    return p.parse_args()


def run_point(workload: str, warehouses: int, terminals: int, olap_threads: int, duration: int, gc: str,
              out_dir: Path) -> common.NormalizedResult:
    scale = common.Scale(tpcc_warehouses=warehouses, tpcc_terminals=terminals, tpcc_duration=duration,
                          htap_olap_threads=olap_threads,
                          label=f"h4/h5 terminals={terminals} olap_threads={olap_threads}")
    return batstore.run(workload, scale, out_dir, gc=gc)


def main() -> None:
    args = parse_args()
    if not args.skip_build:
        print("[build] batstore...")
        batstore.ensure_built()

    olap_thread_list = [int(t) for t in args.olap_threads.split(",") if t.strip()]
    oltp_terminal_list = [int(t) for t in args.oltp_terminals.split(",") if t.strip()]

    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)

    print("\n########## H4 + H5: HTAP OLTP/OLAP interference ##########")
    print(f"run directory        : {run_dir}")
    print(f"workload              : {args.workload}")
    print(f"[H4] olap_threads     : {olap_thread_list} (oltp_terminals fixed at {args.fixed_oltp_terminals})")
    print(f"[H5] oltp_terminals   : {oltp_terminal_list} (olap_threads fixed at {args.fixed_olap_threads})")
    print("#############################################################\n")

    # --- Experiment A (H4): OLTP throughput vs. OLAP thread count ---
    oltp_throughput = {}
    for olap_threads in olap_thread_list:
        out_dir = run_dir / "h4_oltp_vs_olap_threads" / f"olap_threads_{olap_threads}"
        result = run_point(args.workload, args.warehouses, args.fixed_oltp_terminals, olap_threads,
                            args.duration, args.gc, out_dir)
        result.config_label = f"{result.config_label} olap_threads={olap_threads}"
        common.append_manifest_row(manifest_path, result)
        oltp_throughput[olap_threads] = result.primary_metric_value
        status = result.notes or "OK"
        print(f"[H4] olap_threads={olap_threads:3d}  oltp={result.primary_metric_value:10.2f} "
              f"{result.primary_metric_name}  [{status}]")

    # --- Experiment B (H5): OLAP scan latency vs. OLTP terminal count ---
    olap_latency = {}
    for terminals in oltp_terminal_list:
        out_dir = run_dir / "h5_olap_latency_vs_oltp_terminals" / f"terminals_{terminals}"
        result = run_point(args.workload, args.warehouses, terminals, args.fixed_olap_threads,
                            args.duration, args.gc, out_dir)
        result.config_label = f"{result.config_label} terminals={terminals}"
        common.append_manifest_row(manifest_path, result)
        olap_latency[terminals] = {
            "p50": result.scan_p50_us, "p95": result.scan_p95_us, "p99": result.scan_p99_us,
        }
        status = result.notes or "OK"
        print(f"[H5] terminals={terminals:3d}  scan p50={result.scan_p50_us:9.1f}us  "
              f"p95={result.scan_p95_us:9.1f}us  p99={result.scan_p99_us:9.1f}us  [{status}]")

    print(f"\nmanifest : {manifest_path}")
    plot(olap_thread_list, oltp_throughput, oltp_terminal_list, olap_latency, args.fixed_oltp_terminals,
         args.fixed_olap_threads, run_dir / "plots")


def plot(olap_thread_list, oltp_throughput, oltp_terminal_list, olap_latency, fixed_oltp_terminals,
         fixed_olap_threads, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig, (ax_h4, ax_h5) = plt.subplots(1, 2, figsize=(13, 5))

    baseline = oltp_throughput.get(0, next(iter(oltp_throughput.values())))
    axis_a = measurement_values(olap_thread_list)
    positions_a = measurement_positions(olap_thread_list, axis_a)
    relative = [100.0 * oltp_throughput[t] / baseline if baseline else 0.0 for t in olap_thread_list]
    ax_h4.plot(positions_a, relative, color="#222222", marker="o", markersize=7,
               markeredgecolor="white", markeredgewidth=0.8, linewidth=2.2)
    ax_h4.axhline(100.0, color="#999999", linestyle=":", linewidth=1.2)
    set_measurement_axis(ax_h4, olap_thread_list, "OLAP threads")
    ax_h4.set_ylabel(f"OLTP throughput (% of olap_threads=0, oltp_terminals={fixed_oltp_terminals})")
    ax_h4.set_title("H4: OLTP throughput vs. OLAP thread count")

    axis_b = measurement_values(oltp_terminal_list)
    positions_b = measurement_positions(oltp_terminal_list, axis_b)
    for pct in ("p50", "p95", "p99"):
        values = [olap_latency[t][pct] for t in oltp_terminal_list]
        ax_h5.plot(positions_b, values, label=pct, **latency_line_style(pct))
    set_measurement_axis(ax_h5, oltp_terminal_list, "OLTP terminals")
    ax_h5.set_ylabel(f"OLAP scan latency (µs, olap_threads={fixed_olap_threads})")
    ax_h5.set_title("H5: OLAP scan latency vs. OLTP terminal count")
    ax_h5.legend(frameon=False)

    fig.suptitle("H4 + H5: HTAP OLTP/OLAP interference")
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h4_h5_interference.{ext}", dpi=150)
    plt.close(fig)
    print(f"plots    : {out_dir}")


if __name__ == "__main__":
    main()
