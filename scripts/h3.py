#!/usr/bin/env python3
"""H3: key skew shouldn't hurt BatStore's throughput.

    H3) Die Performance von BatStore wird nicht durch den Skew der Schluessel
        beeintraechtigt, wenn der Skew gleichverteilt ueber die Seiten ist.

This is the same axis `scripts/run_skew_sweep.py` already covers across every
engine; this script is a focused, BatStore-only re-run with its own inline
plot, at a single representative thread count, so H3 can be checked without
wading through a multi-engine sweep's manifest.

Workload: ycsb_a (50% read / 50% update - skew affects hot-page contention on
both paths) with BatStore's default Zipfian key generator, theta swept from
uniform (0.0) up to strongly skewed (1.4) - same skew points as
run_skew_sweep.py's default, so results are directly comparable to that
sweep. We report both throughput and tail (p99) read/update latency: if a
skewed key range concentrated updates onto one page, we'd expect not just
lower throughput but a blown-up p99 (latch contention), so tail latency is
the more sensitive signal.

Usage:
    python3 scripts/h3.py
    python3 scripts/h3.py --threads 16 --skews uniform,0.4,0.99,1.4
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib.pyplot as plt

from engines import batstore, common

DEFAULT_SKEWS = ["uniform", "0.1", "0.4", "0.8", "0.99", "1.4"]
OP_COLORS = {"read": "#0072B2", "update": "#D55E00"}  # Okabe-Ito blue/vermillion


def skew_to_theta(skew: str) -> float:
    return 0.0 if skew == "uniform" else float(skew)


def read_op_latency(csv_path: Path, operation: str) -> dict:
    empty = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "count": 0}
    if not csv_path.exists():
        return empty
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("operation") == operation:
                try:
                    return {
                        "p50": float(row["p50_us"]), "p95": float(row["p95_us"]),
                        "p99": float(row["p99_us"]), "avg": float(row["avg_us"]),
                        "count": int(row["count"]),
                    }
                except (KeyError, ValueError):
                    return empty
    return empty


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="h3_results")
    p.add_argument("--skews", default=",".join(DEFAULT_SKEWS))
    p.add_argument("--threads", type=int, default=16)
    p.add_argument("--records", type=int, default=2_000_000)
    p.add_argument("--duration", type=int, default=20)
    p.add_argument("--gc", choices=["on", "off"], default="on")
    p.add_argument("--skip-build", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.skip_build:
        print("[build] batstore...")
        batstore.ensure_built()

    skews = [s.strip() for s in args.skews.split(",") if s.strip()]
    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)

    print("\n########## H3: key-skew sensitivity ##########")
    print(f"run directory : {run_dir}")
    print(f"skews         : {skews}")
    print(f"threads       : {args.threads}")
    print("################################################\n")

    scale_base = common.Scale(ycsb_records=args.records, ycsb_threads=args.threads, ycsb_duration=args.duration)

    throughput_by_skew = {}
    latency_by_skew = {}
    for skew in skews:
        theta = skew_to_theta(skew)
        scale = dataclasses.replace(scale_base, ycsb_theta=theta, label=f"h3 skew={skew}")
        out_dir = run_dir / f"skew_{skew}"
        result = batstore.run("ycsb_a", scale, out_dir, gc=args.gc)
        result.config_label = f"{result.config_label} skew={skew}"
        common.append_manifest_row(manifest_path, result)

        op_csv = out_dir / "ycsb_operation_latency_summary.csv"
        latency_by_skew[skew] = {op: read_op_latency(op_csv, op) for op in ("read", "update")}
        throughput_by_skew[skew] = result.primary_metric_value

        status = result.notes or "OK"
        print(f"skew={skew:8s} theta={theta:5.2f}  throughput={result.primary_metric_value:10.1f} ops/s  "
              f"read_p99={latency_by_skew[skew]['read']['p99']:8.1f}us  "
              f"update_p99={latency_by_skew[skew]['update']['p99']:8.1f}us  [{status}]")

    print(f"\nmanifest : {manifest_path}")
    plot(skews, throughput_by_skew, latency_by_skew, run_dir / "plots")


def plot(skews: list, throughput_by_skew: dict, latency_by_skew: dict, out_dir: Path) -> None:
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

    fig.suptitle("H3: key-skew sensitivity (ycsb_a)")
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h3_skew.{ext}", dpi=150)
    plt.close(fig)
    print(f"plots    : {out_dir}")


if __name__ == "__main__":
    main()
