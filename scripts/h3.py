#!/usr/bin/env python3
"""H3: Historical range-scan performance versus snapshot age.

Capture one snapshot at the beginning of the timed workload and repeatedly
scan every TPC-C table at that same version while OLTP updates continue.
The historic driver mode enables allow_historic_query(true): GC, in-place
updates and idle compaction are disabled, and commit logs are retained.
A transaction keeps the snapshot registered throughout the run; this tests
an aging snapshot, not arbitrary AS OF timestamps supplied after the fact.

Group scans by actual snapshot age at scan start into 12 equal windows over
600 seconds by default. Report median latency and tuples/sec per window.
The snapshot ID and scanned cardinality must stay constant across the run.

Usage:
    python3 scripts/h3.py
    python3 scripts/h3.py --duration 180 --terminals 2 --buckets 6
"""
from __future__ import annotations

import argparse
import csv
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib.pyplot as plt

from hypothesis_common import configure_checkout, check_worker_log

configure_checkout()
from engines import batstore, common
from plot_styles import compact_enabled, finalize_layout, set_compact

SERIES_COLOR = "#4C78A8"       # muted blue for raw observations
MEDIAN_COLOR = "#E45756"       # coral for the latency trend
THROUGHPUT_COLOR = "#2A9D8F"   # teal for the secondary metric


def build_args(
    warehouses: int, terminals: int, duration: int, olap_threads: int, wal_path: Path,
) -> list:
    # Positional spec: src/bat_bench/tpcc_driver.rs::main_tpcc (see project research notes).
    return [
        str(batstore.BINARY), "tpcc", str(warehouses), str(terminals), str(duration),
        "false",  # affinity
        "false",  # gc: historical retention overrides GC anyway
        "false",  # update_in_place
        "fg",     # root_star_index
        "historic",  # one fixed snapshot; all history retained
        str(olap_threads),
        "0",      # olap_param (unused by "historic")
        "100000", # num_items
        "3000",   # customers_per_district
        "3000",   # initial_orders_per_district
        "false",  # wal_enabled
        str(wal_path),
        "5",      # wal_flush_ms
        "EUROPE", # ch_region
        "10000",  # num_suppliers
        "0",      # htap_baseline_secs
        "32kib",  # big_tree_size
    ]


def run_historic_scan(warehouses: int, terminals: int, duration: int, olap_threads: int, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    wal_path = output_dir / "tpcc_wal.log"
    args = build_args(warehouses, terminals, duration, olap_threads, wal_path)
    timeout = common.default_subprocess_timeout(duration)
    returncode, _ = common.run_and_track_rss(
        args, cwd=output_dir, stdout_path=output_dir / "stdout.log", timeout=timeout,
    )
    if returncode != 0:
        raise RuntimeError(f"batstore tpcc (olap_mode=historic) failed (returncode={returncode}); "
                            f"see {output_dir / 'stdout.log'}")


def read_historic_scan_rows(scan_csv: Path) -> list:
    if not scan_csv.exists():
        return []
    rows = []
    with open(scan_csv, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("mode") != "historic_full_scan":
                continue
            try:
                rows.append({
                    "elapsed_secs": float(row["delay_secs"]),
                    "snapshot": int(row["snapshot"]),
                    "scanned_tuples": int(row["scanned_tuples"]),
                    "latency_ns": int(row["latency_ns"]),
                    "tuples_per_sec": float(row["tuples_per_sec"]),
                })
            except (KeyError, ValueError):
                continue
    return rows


def bucket_rows(rows: list, duration: int, num_buckets: int) -> list:
    """Splits `rows` into `num_buckets` equal wall-clock windows over [0, duration] and
    returns one summary dict per (non-empty) bucket: window bounds, sample count, median
    latency (us), median tuples_per_sec, median scanned_tuples.
    """
    width = duration / num_buckets
    buckets = [[] for _ in range(num_buckets)]
    for r in rows:
        idx = min(num_buckets - 1, int(r["elapsed_secs"] // width)) if width > 0 else 0
        buckets[idx].append(r)

    def median(values):
        values = sorted(values)
        n = len(values)
        return 0.0 if n == 0 else values[n // 2] if n % 2 else (values[n // 2 - 1] + values[n // 2]) / 2.0

    def percentile(values, fraction):
        values = sorted(values)
        if not values:
            return 0.0
        position = fraction * (len(values) - 1)
        lower = int(position)
        upper = min(lower + 1, len(values) - 1)
        weight = position - lower
        return values[lower] * (1.0 - weight) + values[upper] * weight

    summaries = []
    for idx, bucket in enumerate(buckets):
        if not bucket:
            continue
        latency_us = [r["latency_ns"] / 1000.0 for r in bucket]
        summaries.append({
            "window_start": idx * width,
            "window_end": (idx + 1) * width,
            "count": len(bucket),
            "median_latency_us": median(latency_us),
            "p25_latency_us": percentile(latency_us, 0.25),
            "p75_latency_us": percentile(latency_us, 0.75),
            "median_tuples_per_sec": median([r["tuples_per_sec"] for r in bucket]),
            "median_scanned_tuples": median([r["scanned_tuples"] for r in bucket]),
        })
    return summaries


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="h3_results")
    p.add_argument("--warehouses", type=int, default=8)
    p.add_argument("--terminals", type=int, default=2,
                   help="fixed OLTP terminal count generating updates while the snapshot ages")
    p.add_argument("--duration", type=int, default=600)
    p.add_argument("--olap-threads", type=int, choices=[1], default=1,
                   help="one fixed snapshot to isolate the effect of snapshot age")
    p.add_argument("--buckets", type=int, default=12, help="number of equal snapshot-age windows to bucket scans into")
    p.add_argument("--skip-build", action="store_true")
    p.add_argument("--compact", action="store_true", help="use a paper-friendly layout with a shared legend")
    args = p.parse_args()
    if min(args.duration, args.buckets, args.warehouses, args.terminals) < 1:
        p.error("duration, buckets, warehouses and terminals must be positive")
    return args


def main() -> None:
    args = parse_args()
    set_compact(args.compact)
    if not args.skip_build:
        print("[build] batstore...")
        batstore.ensure_built()

    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    out_dir = run_dir / "tpcc_historic_scan"

    print("\n########## H3: historical scan latency vs. snapshot age ##########")
    print(f"run directory : {run_dir}")
    print(f"warehouses={args.warehouses} terminals={args.terminals} olap_threads={args.olap_threads} "
          f"duration={args.duration}s buckets={args.buckets}")
    print("###############################################################\n")

    run_historic_scan(args.warehouses, args.terminals, args.duration, args.olap_threads, out_dir)

    check_worker_log(out_dir)
    rows = read_historic_scan_rows(out_dir / "tpcc_scan.csv")
    if not rows:
        sys.exit(f"no 'historic_full_scan' rows found in {out_dir / 'tpcc_scan.csv'} - run failed?")
    if len({r["snapshot"] for r in rows}) != 1:
        sys.exit("Historical scans did not use one fixed snapshot")
    if len({r["scanned_tuples"] for r in rows}) != 1:
        sys.exit("Historical snapshot cardinality changed during the run")
    rows.sort(key=lambda r: r["elapsed_secs"])
    print(f"historical snapshot : {rows[0]['snapshot']} (GC and log truncation disabled)")

    summaries = bucket_rows(rows, args.duration, args.buckets)
    summary_path = run_dir / "h3_time_buckets.csv"
    with open(summary_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["window_start_s", "window_end_s", "count", "median_latency_us",
                    "median_tuples_per_sec", "median_scanned_tuples"])
        for s in summaries:
            w.writerow([f"{s['window_start']:.1f}", f"{s['window_end']:.1f}", s["count"],
                        f"{s['median_latency_us']:.2f}", f"{s['median_tuples_per_sec']:.2f}",
                        f"{s['median_scanned_tuples']:.1f}"])

    print(f"total scan samples : {len(rows)}")
    print(f"fixed snapshot cardinality : {rows[0]['scanned_tuples']}")
    print(f"per-bucket summary : {summary_path}")
    for s in summaries:
        print(f"  [{s['window_start']:6.1f}s, {s['window_end']:6.1f}s)  n={s['count']:4d}  "
              f"median_latency={s['median_latency_us']:10.1f} us  "
              f"median_tuples/sec={s['median_tuples_per_sec']:12.1f}")

    plot(rows, summaries, run_dir / "plots")


def plot(rows: list, summaries: list, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    if compact_enabled():
        # The throughput panel is the inverse of latency because every scan
        # visits the same cardinality.  Omit that redundant panel in the
        # paper layout and spend the available ink on raw variation + trend.
        fig, ax_lat = plt.subplots(figsize=(11.5, 7.5))
        ax_lat.scatter(
            [r["elapsed_secs"] for r in rows],
            [r["latency_ns"] / 1_000_000.0 for r in rows],
            s=4, alpha=0.11, color=SERIES_COLOR, edgecolors="none",
            rasterized=True, label="_individual scans",
        )
        # Use an independent proxy so the legend marker remains legible even
        # though thousands of raw observations are intentionally very faint.
        ax_lat.plot([], [], linestyle="none", marker="o", markersize=5,
                    color=SERIES_COLOR, alpha=0.75, label="individual scan")
        bucket_mid = [(s["window_start"] + s["window_end"]) / 2.0 for s in summaries]
        ax_lat.plot(
            bucket_mid, [s["median_latency_us"] / 1000.0 for s in summaries],
            color=MEDIAN_COLOR, marker="o", markersize=5,
            markeredgecolor="white", markeredgewidth=0.7,
            linewidth=2.2, label="50 s window median", zorder=3,
        )
        ax_lat.set_xlabel("Snapshot age at scan start (s)")
        ax_lat.set_ylabel("Full-scan latency (ms)")
        ax_lat.grid(axis="y", alpha=0.25)
        scanned_tuples = rows[0]["scanned_tuples"]
        ax_lat.text(
            0.985, 0.04, f"{scanned_tuples:,} tuples per scan",
            transform=ax_lat.transAxes, ha="right", va="bottom", fontsize=9,
            color="#444444",
            bbox={"boxstyle": "round,pad=0.25", "facecolor": "white",
                  "edgecolor": "#cccccc", "alpha": 0.9},
        )
        finalize_layout(fig)
        for ext in ("pdf", "png"):
            fig.savefig(out_dir / f"h3_scan_vs_snapshot_age_compact.{ext}", dpi=200,
                        bbox_inches="tight")
        plt.close(fig)
        print(f"plots            : {out_dir}")
        return

    fig, (ax_lat, ax_tup) = plt.subplots(1, 2, figsize=(8.2, 2.35))

    bucket_mid = [(s["window_start"] + s["window_end"]) / 2.0 for s in summaries]
    ax_lat.fill_between(
        bucket_mid,
        [s["p25_latency_us"] / 1000.0 for s in summaries],
        [s["p75_latency_us"] / 1000.0 for s in summaries],
        color=SERIES_COLOR, alpha=0.32, linewidth=0,
        label="middle 50% of scans",
    )
    ax_lat.plot(bucket_mid, [s["median_latency_us"] / 1000.0 for s in summaries], color=MEDIAN_COLOR,
                marker="o", markersize=5, markeredgecolor="white", markeredgewidth=0.7,
                linewidth=1.8, label="window median")
    ax_lat.set_xlabel("Snapshot age (s)")
    ax_lat.set_ylabel("Full-scan latency (ms)")
    ax_lat.set_ylim(0, 160)
    ax_lat.set_yticks([0, 40, 80, 120, 160])
    ax_lat.legend(frameon=False, fontsize=7.5, loc="lower right",
                  handlelength=1.7, labelspacing=0.25)
    ax_lat.grid(axis="y", alpha=0.25)

    ax_tup.plot(bucket_mid, [s["median_tuples_per_sec"] / 1_000_000.0 for s in summaries],
                color=THROUGHPUT_COLOR, marker="o", markersize=5,
                markeredgecolor="white", markeredgewidth=0.7, linewidth=1.8)
    ax_tup.set_xlabel("Snapshot age (s)")
    ax_tup.set_ylabel("Throughput (M tuples/s)")
    # Throughput only varies by a few percent.  A zero-based axis communicates
    # that stability instead of visually magnifying the small fluctuations.
    upper_limit = max(40.0, max(s["median_tuples_per_sec"] for s in summaries) / 1_000_000.0 * 1.04)
    ax_tup.set_ylim(0, upper_limit)
    ax_tup.set_yticks([0, 10, 20, 30, 40])
    ax_tup.grid(axis="y", alpha=0.25)

    finalize_layout(fig)
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h3_scan_vs_snapshot_age.{ext}", dpi=150)
    plt.close(fig)
    print(f"plots            : {out_dir}")


if __name__ == "__main__":
    main()
