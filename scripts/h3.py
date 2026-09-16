#!/usr/bin/env python3
"""H3: Historical range-scan performance versus snapshot age.

For BatStore, PostgreSQL, libmdbx, and WiredTiger, capture one native snapshot
at the beginning of the timed workload and repeatedly scan every TPC-C table
at that same version while OLTP updates continue. A transaction keeps the
snapshot registered throughout the run; this tests an aging snapshot, not
fresh transactions or arbitrary AS OF timestamps supplied after the fact.

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
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib.pyplot as plt

from hypothesis_common import configure_checkout, check_worker_log

configure_checkout()
from engines import batstore, common, libmdbx, postgres_benchbase
from engines import leanstore_build
from plot_styles import (ENGINE_COLORS, ENGINE_LABELS, ENGINE_MARKERS,
                         compact_enabled, finalize_layout, set_compact)

DEFAULT_ENGINES = ("batstore", "postgres", "libmdbx", "wiredtiger")


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


def run_batstore_historic_scan(warehouses: int, terminals: int, duration: int, olap_threads: int, output_dir: Path) -> None:
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


def run_libmdbx_historic_scan(warehouses: int, terminals: int, duration: int, output_dir: Path,
                             timeout_seconds: int | None = None) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = common.fresh_scratch_dir("libmdbx_h3_data") / "db"
    cmd = [str(libmdbx.BINARY), "mdbx_tpcc", str(warehouses), str(terminals),
           str(duration), "100000", "3000", "3000", str(db_path), "historic", "1"]
    timeout = timeout_seconds or common.default_subprocess_timeout(duration)
    returncode, _ = common.run_and_track_rss(
        cmd, cwd=output_dir, stdout_path=output_dir / "stdout.log",
        timeout=timeout,
    )
    if returncode != 0:
        reason = f"timed out after {timeout}s" if returncode is None else f"exit={returncode}"
        raise RuntimeError(f"libmdbx historic TPC-C failed ({reason}); "
                           f"see {output_dir / 'stdout.log'}")


def run_wiredtiger_historic_scan(warehouses: int, terminals: int, duration: int, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    ssd_dir = common.fresh_scratch_dir("wiredtiger_h3_data") / "ssd"
    ssd_dir.mkdir(parents=True, exist_ok=True)
    cmd = [str(leanstore_build.binary("wiredtiger_tpcc")),
           f"--tpcc_warehouse_count={warehouses}", f"--worker_threads={terminals + 1}",
           "--ch_a_threads=1", "--h3_historic=true", f"--dram_gib={common.default_dram_gib()}",
           f"--ssd_path={ssd_dir}", f"--run_for_seconds={duration}",
           "--isolation_level=si", "--print_header"]
    returncode, _ = common.run_and_track_rss(
        cmd, cwd=output_dir, env=leanstore_build.run_env(),
        stdout_path=output_dir / "stdout.log",
        timeout=common.default_subprocess_timeout(duration),
    )
    if returncode != 0:
        raise RuntimeError(f"WiredTiger historic TPC-C failed (returncode={returncode}); "
                           f"see {output_dir / 'stdout.log'}")


def _postgres_scan_sql(duration: int, csv_path: Path) -> str:
    # The temporary function executes all counts inside its caller's one REPEATABLE READ
    # transaction. clock_timestamp (rather than transaction_timestamp) measures real time.
    tables = ("warehouse", "district", "customer", "history", "new_order",
              "oorder", "order_line", "item", "stock")
    count_expr = " + ".join(f"(SELECT count(*) FROM {table})" for table in tables)
    path = str(csv_path).replace("'", "''")
    copy_query = (
        "SELECT 'historic_full_scan' AS mode, elapsed_secs, elapsed_secs AS delay_secs, "
        "snapshot, scanned_tuples, latency_ns, tuples_per_sec "
        f"FROM pg_temp.h3_scan({duration})"
    )
    return f"""
CREATE TEMP TABLE h3_session_marker(value integer);
CREATE OR REPLACE FUNCTION pg_temp.h3_scan(run_seconds double precision)
RETURNS TABLE(elapsed_secs double precision, snapshot text, scanned_tuples bigint,
              latency_ns bigint, tuples_per_sec double precision)
LANGUAGE plpgsql AS $$
DECLARE
  run_start timestamptz := clock_timestamp();
  scan_start timestamptz;
  n bigint;
  ns bigint;
BEGIN
  WHILE extract(epoch FROM clock_timestamp() - run_start) < run_seconds LOOP
    scan_start := clock_timestamp();
    SELECT {count_expr} INTO n;
    ns := (extract(epoch FROM clock_timestamp() - scan_start) * 1000000000)::bigint;
    RETURN QUERY SELECT extract(epoch FROM scan_start - run_start),
      txid_current_snapshot()::text, n, ns,
      CASE WHEN ns = 0 THEN 0.0 ELSE n * 1000000000.0 / ns END;
  END LOOP;
END $$;
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
\\copy ({copy_query}) TO '{path}' WITH (FORMAT csv, HEADER true)
COMMIT;
"""


def run_postgres_historic_scan(warehouses: int, terminals: int, duration: int, output_dir: Path) -> None:
    """Load TPC-C, then scan one PostgreSQL snapshot while BenchBase drives OLTP."""
    output_dir.mkdir(parents=True, exist_ok=True)
    # Reuse the established wrapper for configuration, safety checks, loading, NUMA
    # placement, and OLTP execution. The one-second preparation execution leaves a valid
    # loaded database; the measured run below deliberately skips reload.
    prep_scale = common.Scale(tpcc_warehouses=warehouses, tpcc_terminals=terminals,
                              tpcc_duration=1, label="h3-load")
    prep = postgres_benchbase.run("tpcc", prep_scale, output_dir / "load", gc="off", reload=True)
    if prep.notes.startswith(("FAILED", "TIMEOUT", "SKIPPED")):
        raise RuntimeError(f"PostgreSQL TPC-C load failed: {prep.notes}")

    run_scale = common.Scale(tpcc_warehouses=warehouses, tpcc_terminals=terminals,
                             tpcc_duration=duration + 10, label="h3")
    result_box = {}
    def oltp() -> None:
        result_box["result"] = postgres_benchbase.run(
            "tpcc", run_scale, output_dir / "oltp", gc="off", reload=False)
    thread = threading.Thread(target=oltp, name="h3-postgres-oltp")
    thread.start()

    # Wait until BenchBase has an active TPC-C connection before assigning the reader's
    # snapshot, so age zero corresponds to a genuinely concurrent OLTP phase.
    env = os.environ.copy()
    env["PGPASSWORD"] = common.PG_PASSWORD
    probe = ["psql", "-X", "-qAt", "-h", "localhost", "-U", common.PG_ROLE, "-d", common.PG_DATABASE,
             "-c", "SELECT count(*) FROM pg_stat_activity WHERE application_name='tpcc' AND state <> 'idle'"]
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        seen = subprocess.run(probe, env=env, capture_output=True, text=True)
        if seen.returncode == 0 and seen.stdout.strip().isdigit() and int(seen.stdout.strip()) > 0:
            break
        if not thread.is_alive():
            break
        time.sleep(0.1)

    scan_csv = output_dir / "tpcc_scan.csv"
    with (output_dir / "scan_stdout.log").open("w") as scan_log:
        scan = subprocess.run(
            ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", "localhost",
             "-U", common.PG_ROLE, "-d", common.PG_DATABASE],
            input=_postgres_scan_sql(duration, scan_csv), text=True, env=env,
            stdout=scan_log, stderr=subprocess.STDOUT,
            timeout=common.default_subprocess_timeout(duration),
        )
    thread.join(timeout=common.default_subprocess_timeout(duration + 10))
    result = result_box.get("result")
    if (scan.returncode != 0 or result is None
            or result.notes.startswith(("FAILED", "TIMEOUT", "SKIPPED"))):
        raise RuntimeError(f"PostgreSQL historic scan failed; see {output_dir}")


def read_historic_scan_rows(scan_csv: Path) -> list:
    if not scan_csv.exists():
        return []
    rows = []
    with open(scan_csv, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("mode") != "historic_full_scan":
                continue
            try:
                snapshot_text = row["snapshot"]
                try:
                    snapshot = int(snapshot_text)
                except ValueError:
                    snapshot = snapshot_text
                rows.append({
                    "elapsed_secs": float(row["delay_secs"]),
                    "snapshot": snapshot,
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
    p.add_argument("--engines", default=",".join(DEFAULT_ENGINES),
                   help="comma-separated engines: batstore, postgres, libmdbx, wiredtiger")
    p.add_argument("--warehouses", type=int, default=8)
    p.add_argument("--terminals", type=int, default=2,
                   help="fixed OLTP terminal count generating updates while the snapshot ages")
    p.add_argument("--duration", type=int, default=600)
    p.add_argument("--libmdbx-timeout", type=int, default=None,
                   help="wall-clock limit in seconds for libmdbx, including data loading")
    p.add_argument("--olap-threads", type=int, choices=[1], default=1,
                   help="one fixed snapshot to isolate the effect of snapshot age")
    p.add_argument("--buckets", type=int, default=12, help="number of equal snapshot-age windows to bucket scans into")
    p.add_argument("--skip-build", action="store_true")
    p.add_argument("--compact", action="store_true", help="use a paper-friendly layout with a shared legend")
    args = p.parse_args()
    args.engines = [name.strip().lower() for name in args.engines.split(",") if name.strip()]
    unknown = sorted(set(args.engines) - set(DEFAULT_ENGINES))
    if unknown:
        p.error(f"unsupported engines: {', '.join(unknown)}")
    if not args.engines:
        p.error("at least one engine is required")
    if min(args.duration, args.buckets, args.warehouses, args.terminals) < 1:
        p.error("duration, buckets, warehouses and terminals must be positive")
    if args.libmdbx_timeout is not None and args.libmdbx_timeout <= 0:
        p.error("--libmdbx-timeout must be positive")
    return args


def main() -> None:
    args = parse_args()
    set_compact(args.compact)
    builders = {
        "batstore": batstore.ensure_built,
        "libmdbx": libmdbx.ensure_built,
        "wiredtiger": lambda: leanstore_build.ensure_built(("wiredtiger_tpcc",)),
        "postgres": postgres_benchbase.ensure_built,
    }
    if not args.skip_build:
        for engine in args.engines:
            print(f"[build] {engine}...")
            builders[engine]()

    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    print("\n########## H3: historical scan latency vs. snapshot age ##########")
    print(f"run directory : {run_dir}")
    print(f"warehouses={args.warehouses} terminals={args.terminals} olap_threads={args.olap_threads} "
          f"duration={args.duration}s buckets={args.buckets} engines={','.join(args.engines)}")
    print("###############################################################\n")

    runners = {
        "batstore": lambda out: run_batstore_historic_scan(
            args.warehouses, args.terminals, args.duration, args.olap_threads, out),
        "postgres": lambda out: run_postgres_historic_scan(
            args.warehouses, args.terminals, args.duration, out),
        "libmdbx": lambda out: run_libmdbx_historic_scan(
            args.warehouses, args.terminals, args.duration, out, args.libmdbx_timeout),
        "wiredtiger": lambda out: run_wiredtiger_historic_scan(
            args.warehouses, args.terminals, args.duration, out),
    }
    rows_by_engine = {}
    summaries_by_engine = {}
    failures = {}
    for engine in args.engines:
        out_dir = run_dir / "tpcc_historic_scan" / engine
        print(f"\n[run] {ENGINE_LABELS[engine]}")
        try:
            runners[engine](out_dir)
            check_worker_log(out_dir)
            rows = read_historic_scan_rows(out_dir / "tpcc_scan.csv")
            if not rows:
                raise RuntimeError(f"no historic_full_scan rows in {out_dir / 'tpcc_scan.csv'}")
            if len({r["snapshot"] for r in rows}) != 1:
                raise RuntimeError("historical scans did not use one fixed snapshot")
            if len({r["scanned_tuples"] for r in rows}) != 1:
                raise RuntimeError("historical snapshot cardinality changed during the run")
        except (RuntimeError, OSError, subprocess.TimeoutExpired) as exc:
            failures[engine] = str(exc)
            print(f"  FAILED: {exc}", file=sys.stderr)
            continue
        rows.sort(key=lambda r: r["elapsed_secs"])
        rows_by_engine[engine] = rows
        summaries_by_engine[engine] = bucket_rows(rows, args.duration, args.buckets)
        print(f"  snapshot={rows[0]['snapshot']} samples={len(rows)} tuples/scan={rows[0]['scanned_tuples']}")

    summary_path = run_dir / "h3_time_buckets.csv"
    with open(summary_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["engine", "window_start_s", "window_end_s", "count", "median_latency_us",
                    "median_tuples_per_sec", "median_scanned_tuples"])
        for engine in rows_by_engine:
            for s in summaries_by_engine[engine]:
                w.writerow([engine, f"{s['window_start']:.1f}", f"{s['window_end']:.1f}", s["count"],
                            f"{s['median_latency_us']:.2f}", f"{s['median_tuples_per_sec']:.2f}",
                            f"{s['median_scanned_tuples']:.1f}"])

    print(f"per-bucket summary : {summary_path}")
    if rows_by_engine:
        plot(rows_by_engine, summaries_by_engine, run_dir / "plots")
    if failures:
        failure_path = run_dir / "h3_failures.csv"
        with failure_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["engine", "error"])
            writer.writerows(failures.items())
        sys.exit(f"H3 incomplete: {len(failures)} engine(s) failed; see {failure_path}")


def plot(rows_by_engine: dict, summaries_by_engine: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if compact_enabled():
        fig, axes = plt.subplots(1, 1, figsize=(11.5, 7.5))
        ax_lat, ax_tup = axes, None
    else:
        fig, (ax_lat, ax_tup) = plt.subplots(1, 2, figsize=(10.2, 3.2))

    for engine, summaries in summaries_by_engine.items():
        mids = [(s["window_start"] + s["window_end"]) / 2.0 for s in summaries]
        color = ENGINE_COLORS[engine]
        marker = ENGINE_MARKERS[engine]
        ax_lat.fill_between(
            mids, [s["p25_latency_us"] / 1000.0 for s in summaries],
            [s["p75_latency_us"] / 1000.0 for s in summaries],
            color=color, alpha=0.13, linewidth=0,
        )
        ax_lat.plot(
            mids, [s["median_latency_us"] / 1000.0 for s in summaries],
            color=color, marker=marker, markersize=5, markeredgecolor="white",
            markeredgewidth=0.6, linewidth=1.8, label=ENGINE_LABELS[engine],
        )
        if ax_tup is not None:
            ax_tup.plot(
                mids, [s["median_tuples_per_sec"] / 1_000_000.0 for s in summaries],
                color=color, marker=marker, markersize=5, markeredgecolor="white",
                markeredgewidth=0.6, linewidth=1.8, label=ENGINE_LABELS[engine],
            )

    ax_lat.set_xlabel("Snapshot age at scan start (s)")
    ax_lat.set_ylabel("Full-scan latency (ms)")
    ax_lat.grid(axis="y", alpha=0.25)
    ax_lat.legend(frameon=False, fontsize=8)
    if ax_tup is not None:
        ax_tup.set_xlabel("Snapshot age at scan start (s)")
        ax_tup.set_ylabel("Throughput (M tuples/s)")
        ax_tup.grid(axis="y", alpha=0.25)
        ax_tup.legend(frameon=False, fontsize=8)

    finalize_layout(fig)
    suffix = "_compact" if compact_enabled() else ""
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h3_scan_vs_snapshot_age{suffix}.{ext}", dpi=200 if compact_enabled() else 150,
                    bbox_inches="tight")
    plt.close(fig)
    print(f"plots            : {out_dir}")


if __name__ == "__main__":
    main()
