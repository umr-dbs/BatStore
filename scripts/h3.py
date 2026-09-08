#!/usr/bin/env python3
"""H3: range-scan performance is independent of *when* the scan runs.

    H3) BatStore garantiert die gleiche Performance von Range-Scans
        unabhaengig von dem Zeitpunkt, wann der Scanablaeuft.

`batstore.py`'s `run()` wrapper hardcodes TPC-C's `olap_mode` to "none", so it
can't drive this - we invoke the release binary directly (mirroring what
`batstore.py` itself does) with `olap_mode="fresh"`
(`OlapMode::RepeatedFreshFullScan`, src/bat_bench/olap_scan.rs): one dedicated
OLAP thread opens a fresh MVCC snapshot, scans every TPC-C table back-to-back,
commits, and repeats for the whole run - producing one `tpcc_scan.csv` row per
scan attempt, each stamped with `elapsed_secs` (completion time since the
timed phase started; this script subtracts latency to recover scan start time) and `scanned_tuples`/`latency_ns`.

Caveat: BatStore's TPC-C driver always keeps at least one OLTP terminal
running (no "writers off" mode exists), so the scanned tables are not
perfectly static across the run - NewOrder/Payment keep inserting. We use a
minimal terminal count and report the scanned_tuples growth ratio alongside
the latency numbers, and treat `tuples_per_sec` (scan throughput normalized
for how much data there was to scan) as the primary "is it time-dependent"
signal, since it isn't confounded by slow, roughly-linear data growth the way
raw scanned_tuples/latency_ns are.

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

from hypothesis_common import configure_checkout, thread_counts, check_worker_log

configure_checkout()
from engines import batstore, common

SERIES_COLOR = "#0072B2"  # Okabe-Ito blue - single series, no legend needed


def build_args(
    warehouses: int, terminals: int, duration: int, olap_threads: int, wal_path: Path,
) -> list:
    # Positional spec: src/bat_bench/tpcc_driver.rs::main_tpcc (see project research notes).
    return [
        str(batstore.BINARY), "tpcc", str(warehouses), str(terminals), str(duration),
        "false",  # affinity
        "true",   # gc
        "false",  # update_in_place
        "fg",     # root_star_index
        "fresh",  # olap_mode -> OlapMode::RepeatedFreshFullScan
        str(olap_threads),
        "0",      # olap_param (unused by "fresh")
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


def run_fresh_scan(warehouses: int, terminals: int, duration: int, olap_threads: int, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    wal_path = output_dir / "tpcc_wal.log"
    args = build_args(warehouses, terminals, duration, olap_threads, wal_path)
    timeout = common.default_subprocess_timeout(duration)
    returncode, _ = common.run_and_track_rss(
        args, cwd=output_dir, stdout_path=output_dir / "stdout.log", timeout=timeout,
    )
    if returncode != 0:
        raise RuntimeError(f"batstore tpcc (olap_mode=fresh) failed (returncode={returncode}); "
                            f"see {output_dir / 'stdout.log'}")


def read_fresh_scan_rows(scan_csv: Path) -> list:
    if not scan_csv.exists():
        return []
    rows = []
    with open(scan_csv, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("mode") != "fresh_full_scan":
                continue
            try:
                rows.append({
                    "elapsed_secs": max(0.0, float(row["elapsed_secs"]) - int(row["latency_ns"]) / 1e9),
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

    summaries = []
    for idx, bucket in enumerate(buckets):
        if not bucket:
            continue
        summaries.append({
            "window_start": idx * width,
            "window_end": (idx + 1) * width,
            "count": len(bucket),
            "median_latency_us": median([r["latency_ns"] for r in bucket]) / 1000.0,
            "median_tuples_per_sec": median([r["tuples_per_sec"] for r in bucket]),
            "median_scanned_tuples": median([r["scanned_tuples"] for r in bucket]),
        })
    return summaries


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="h3_results")
    p.add_argument("--warehouses", type=int, default=8)
    p.add_argument("--terminals", type=int, default=2,
                   help="minimal OLTP terminal count (BatStore has no writers-off mode; "
                        "this minimizes how much the scanned tables mutate during the run)")
    p.add_argument("--duration", type=int, default=600)
    p.add_argument("--olap-threads", type=int, default=1)
    p.add_argument("--buckets", type=int, default=12, help="number of equal wall-clock windows to bucket scans into")
    p.add_argument("--skip-build", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.skip_build:
        print("[build] batstore...")
        batstore.ensure_built()

    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    out_dir = run_dir / "tpcc_fresh_scan"

    print("\n########## H3: range-scan latency vs. time-in-run ##########")
    print(f"run directory : {run_dir}")
    print(f"warehouses={args.warehouses} terminals={args.terminals} olap_threads={args.olap_threads} "
          f"duration={args.duration}s buckets={args.buckets}")
    print("###############################################################\n")

    run_fresh_scan(args.warehouses, args.terminals, args.duration, args.olap_threads, out_dir)

    check_worker_log(out_dir)
    rows = read_fresh_scan_rows(out_dir / "tpcc_scan.csv")
    if not rows:
        sys.exit(f"no 'fresh_full_scan' rows found in {out_dir / 'tpcc_scan.csv'} - run failed?")
    rows.sort(key=lambda r: r["elapsed_secs"])

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

    growth = (summaries[-1]["median_scanned_tuples"] / summaries[0]["median_scanned_tuples"] - 1.0) * 100.0 \
        if summaries and summaries[0]["median_scanned_tuples"] else 0.0
    print(f"total scan samples : {len(rows)}")
    print(f"scanned_tuples growth (first bucket -> last bucket): {growth:+.2f}%")
    print(f"per-bucket summary : {summary_path}")
    for s in summaries:
        print(f"  [{s['window_start']:6.1f}s, {s['window_end']:6.1f}s)  n={s['count']:4d}  "
              f"median_latency={s['median_latency_us']:10.1f} us  "
              f"median_tuples/sec={s['median_tuples_per_sec']:12.1f}")

    plot(rows, summaries, run_dir / "plots")


def plot(rows: list, summaries: list, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    fig, (ax_lat, ax_tup) = plt.subplots(1, 2, figsize=(13, 5))

    ax_lat.scatter([r["elapsed_secs"] for r in rows], [r["latency_ns"] / 1000.0 for r in rows],
                    s=10, alpha=0.35, color=SERIES_COLOR, label="individual scan")
    bucket_mid = [(s["window_start"] + s["window_end"]) / 2.0 for s in summaries]
    ax_lat.plot(bucket_mid, [s["median_latency_us"] for s in summaries], color="#D55E00",
                marker="o", markersize=7, markeredgecolor="white", markeredgewidth=0.8,
                linewidth=2.2, label="per-window median")
    ax_lat.set_xlabel("elapsed time in run (s)")
    ax_lat.set_ylabel("full-table-scan latency (µs)")
    ax_lat.set_title("Scan latency over time")
    ax_lat.legend(frameon=False)
    ax_lat.grid(alpha=0.3)

    ax_tup.plot(bucket_mid, [s["median_tuples_per_sec"] for s in summaries], color=SERIES_COLOR,
                marker="o", markersize=7, markeredgecolor="white", markeredgewidth=0.8, linewidth=2.2)
    ax_tup.set_xlabel("elapsed time in run (s)")
    ax_tup.set_ylabel("scan throughput (tuples/sec)")
    ax_tup.set_title("Scan throughput over time (data-size-normalized)")
    ax_tup.grid(alpha=0.3)

    fig.suptitle("H3: range-scan performance vs. time-in-run")
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h3_scan_vs_time.{ext}", dpi=150)
    plt.close(fig)
    print(f"plots            : {out_dir}")


if __name__ == "__main__":
    main()
