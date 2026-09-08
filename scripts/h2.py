#!/usr/bin/env python3
"""H2: Throughput sensitivity to key-access skew.

Run YCSB A at each skew/thread-count combination. Uniform is a mandatory
baseline. Nonzero theta uses scrambled Zipfian ranks, spreading hot keys
across the key space; this does not guarantee equal load on physical pages.
Compare each skew to uniform at the same thread count. Report throughput and
sampled read/update p99 latency; h2_skew_summary.csv includes relative throughput.
Fixed within each curve: threads, records, duration, GC and execution mode.
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime
import os
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib.pyplot as plt

from hypothesis_common import configure_checkout, thread_counts, check_run, read_op_latency, positive_int, record_setup

configure_checkout()
from engines import batstore, common

DEFAULT_SKEWS = ["uniform", "0.1", "0.4", "0.8", "0.99", "1.4"]
OP_COLORS = {"read": "#0072B2", "update": "#D55E00"}  # Okabe-Ito blue/vermillion


def skew_to_theta(skew: str) -> float:
    try:
        theta = 0.0 if skew == "uniform" else float(skew)
        if math.isfinite(theta) and theta >= 0:
            return theta
    except ValueError:
        pass
    raise SystemExit("--skews must contain uniform or finite nonnegative theta values")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="h2_results")
    p.add_argument("--skews", default=",".join(DEFAULT_SKEWS))
    p.add_argument("--threads", default="1,8,16,32,48,64,80,96,112,128", help="comma-separated OLTP thread counts")
    p.add_argument("--records", type=positive_int, default=2_000_000)
    p.add_argument("--duration", type=positive_int, default=20)
    p.add_argument("--gc", choices=["on", "off"], default="on")
    p.add_argument("--mode", choices=["atomic", "transaction"], default="atomic", help="fixed YCSB execution mode")
    p.add_argument("--skip-build", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    os.environ["BATSTORE_YCSB_MODE"] = args.mode
    threads_list = thread_counts(args.threads)
    skews = list(dict.fromkeys(s.strip() for s in args.skews.split(",")))
    for skew in skews:
        skew_to_theta(skew)
    skews = ["uniform"] + [s for s in skews if skew_to_theta(s) != 0]
    if not args.skip_build:
        print("[build] batstore...")
        batstore.ensure_built()

    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    record_setup(run_dir, "H2", args, varies=f"theta={skews} at threads={threads_list}",
                 fixed=f"YCSB A, mode={args.mode}, records={args.records}, duration={args.duration}s, GC={args.gc}",
                 measures="throughput relative to uniform at matching threads; read/update p99 latency")
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)

    print("\n########## H2: key-skew sensitivity ##########")
    print(f"run directory : {run_dir}")
    print(f"skews         : {skews}")
    print(f"threads       : {args.threads}")
    print("################################################\n")

    scale_base = common.Scale(ycsb_records=args.records, ycsb_duration=args.duration)

    summary_path = run_dir / "h2_skew_summary.csv"
    with summary_path.open("w", newline="") as f:
        csv.writer(f).writerow(["threads", "skew", "throughput_ops_sec", "relative_to_uniform_pct", "read_p99_us", "update_p99_us"])
    for threads in threads_list:
        throughput_by_skew = {}
        latency_by_skew = {}
        for skew in skews:
            theta = skew_to_theta(skew)
            scale = dataclasses.replace(scale_base, ycsb_threads=threads, ycsb_theta=theta, label=f"h2 threads={threads} skew={skew}")
            out_dir = run_dir / f"threads_{threads}" / f"skew_{skew}"
            result = batstore.run("ycsb_a", scale, out_dir, gc=args.gc)
            result.config_label = f"{result.config_label} skew={skew}"
            common.append_manifest_row(manifest_path, result)
            check_run(result, out_dir)

            op_csv = out_dir / "ycsb_operation_latency_summary.csv"
            latency_by_skew[skew] = {op: read_op_latency(op_csv, op) for op in ("read", "update")}
            throughput_by_skew[skew] = result.primary_metric_value

            status = result.notes or "OK"
            print(f"threads={threads:3d} skew={skew:8s} theta={theta:5.2f}  throughput={result.primary_metric_value:10.1f} ops/s  "
                  f"read_p99={latency_by_skew[skew]['read']['p99']:8.1f}us  "
                  f"update_p99={latency_by_skew[skew]['update']['p99']:8.1f}us  [{status}]")

        with summary_path.open("a", newline="") as f:
            for skew in skews:
                csv.writer(f).writerow([threads, skew, throughput_by_skew[skew],
                    100 * throughput_by_skew[skew] / throughput_by_skew["uniform"],
                    latency_by_skew[skew]["read"]["p99"], latency_by_skew[skew]["update"]["p99"]])
        plot(skews, throughput_by_skew, latency_by_skew, run_dir / "plots" / f"threads_{threads}", threads)
    print(f"\nmanifest : {manifest_path}")


def plot(skews: list, throughput_by_skew: dict, latency_by_skew: dict, out_dir: Path, threads: int) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    positions = list(range(len(skews)))

    fig, (ax_tp, ax_lat) = plt.subplots(1, 2, figsize=(13, 5))

    ax_tp.plot(positions, [throughput_by_skew[s] for s in skews], color="#222222",
               marker="o", markersize=7, markeredgecolor="white", markeredgewidth=0.8, linewidth=2.2)
    ax_tp.set_xticks(positions)
    ax_tp.set_xticklabels(skews)
    ax_tp.set_xlabel("skew (zipfian theta, 'uniform' = 0.0)")
    ax_tp.set_ylabel("throughput (ops/sec)")
    ax_tp.set_title("ycsb_a throughput vs. skew")
    ax_tp.grid(alpha=0.3)

    for op, color in OP_COLORS.items():
        ax_lat.plot(positions, [latency_by_skew[s][op]["p99"] for s in skews], color=color,
                    marker="o", markersize=6, markeredgecolor="white", markeredgewidth=0.7,
                    linewidth=1.8, label=f"{op} p99")
    ax_lat.set_xticks(positions)
    ax_lat.set_xticklabels(skews)
    ax_lat.set_xlabel("skew (zipfian theta, 'uniform' = 0.0)")
    ax_lat.set_ylabel("p99 latency (µs)")
    ax_lat.set_title("ycsb_a tail latency vs. skew")
    ax_lat.legend(frameon=False)
    ax_lat.grid(alpha=0.3)

    fig.suptitle(f"H2: key-skew sensitivity (YCSB A, {threads} OLTP threads)")
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h2_skew.{ext}", dpi=150)
    plt.close(fig)
    print(f"plots    : {out_dir}")


if __name__ == "__main__":
    main()
