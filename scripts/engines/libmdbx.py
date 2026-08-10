"""libmdbx engine wrapper - drives cMVBT's own binary (same as cmvbt.py), but its
`mdbx_ycsb`/`mdbx_tpcc` subcommands (src/mv_bench/mdbx_ycsb.rs / mdbx_tpcc.rs), a second,
independent storage backend built on the `libmdbx` crate (path-copying/copy-on-write
B+Tree) instead of cMVBT's own version-chain MVBTree - only compiled in behind the
`mdbx-backend` Cargo feature, since it's an optional comparison point, not part of every
build.

TPC-C + YCSB A-F + htap_q1/htap_q6 - the latter two run mdbx_tpcc.rs's own Q1 ("Pricing
Summary Report")/Q6 ("Forecasting Revenue Change") queries (mdbx_q1/mdbx_q6, a libmdbx
port of `mv_bench::tpch_queries::q1`/`q6`) concurrently with the OLTP terminals, same
mechanism as cmvbt.py's "ch" olap_mode. Unlike cMVBT/LeanStore/WiredTiger, only Q1/Q6 are
implemented (not the full 4-query CH-benCHmark rotation) - libmdbx's own TPC-C schema
(mdbx_tpcc.rs) only has the 11 core tables, not CH-benCHmark's SUPPLIER/NATION/REGION
addition Q4/Q5 need, and Q1/Q6 are the only 2 of CH-benCHmark's 22 queries that are pure
`ORDER_LINE` aggregations needing no joins against those missing tables (see
mdbx_tpcc.rs's module docs).

libmdbx (like LMDB) allows only one read-write transaction active process-wide at a time -
there is no possible write-write race the way cMVBT's OSIC or a real MVCC engine has, so
unlike every other engine here, throughput at higher thread counts is fundamentally
bounded by that single-writer serialization for any workload with a write component. This
is a genuine, expected characteristic of path-copying/CoW MVCC to surface honestly, not a
wrapper bug.
"""
from __future__ import annotations

from pathlib import Path

from . import common

REPO_ROOT = common.CMVBT_REPO
BINARY = REPO_ROOT / "target" / "release" / "cMVBT"

# libmdbx's own MVCC reclaims old pages once no reader references them (like any real
# path-copying store) - no separate on/off toggle exists or is being added, same treatment
# as leanstore.py/wiredtiger.py's SUPPORTS_GC_TOGGLE = False.
SUPPORTS_GC_TOGGLE = False


def ensure_built() -> None:
    import subprocess
    # Same shared binary/allocator knob as cmvbt.py's own ensure_built() - see
    # common.cmvbt_cargo_build_args's doc for why this can't just be duplicated ad hoc.
    subprocess.run(common.cmvbt_cargo_build_args("mdbx-backend"), cwd=REPO_ROOT, check=True)


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "n/a", reload: bool = True) -> common.NormalizedResult:
    """`gc`/`reload` accepted for interface parity with the other engine wrappers but
    unused - see SUPPORTS_GC_TOGGLE above, and every run here is a fresh db_path."""
    del reload
    output_dir.mkdir(parents=True, exist_ok=True)

    # Fixed, wiped-before-every-run path - matches leanstore.py/wiredtiger.py's ssd_path
    # treatment, and your explicit ask for libmdbx specifically.
    db_path = common.fresh_scratch_dir("libmdbx_data") / "db"

    if workload in (["tpcc"] + common.HTAP_WORKLOADS):
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        # htap_mode (position 9): "ch" spawns mdbx_tpcc.rs's one OLAP thread rotating
        # mdbx_q1/mdbx_q6 - see that module's docs and this file's own module doc. Both
        # htap_q1 and htap_q6 request the same "ch" rotation (mirrors cmvbt.py's identical
        # choice); only the mode filter used below when reading tpcc_scan.csv differs.
        htap_mode = "ch" if workload in common.HTAP_WORKLOADS else "none"
        args = [
            str(BINARY), "mdbx_tpcc", str(scale.tpcc_warehouses), str(threads), str(duration),
            "100000", "3000", "3000", str(db_path), htap_mode,
        ]
        metric_name = "new_order_per_sec"
        ts_file, ts_column = "tpcc_oltp_timeseries.csv", "new_order_committed"
    else:
        letter = workload.split("_", 1)[1]
        duration = scale.ycsb_duration
        threads = scale.ycsb_threads
        args = [
            str(BINARY), "mdbx_ycsb", letter, str(scale.ycsb_records), str(threads),
            str(duration), "default", str(scale.ycsb_theta), "10", "100", "100", str(db_path),
        ]
        metric_name = "ops_per_sec"
        ts_file, ts_column = "ycsb_timeseries.csv", "ops_completed"

    timeout = common.default_subprocess_timeout(duration)
    returncode, _peak_rss_unused = common.run_and_track_rss(
        args, cwd=output_dir, stdout_path=output_dir / "stdout.log", timeout=timeout,
    )
    if returncode != 0:
        notes = f"TIMEOUT after {timeout:.0f}s, see stdout.log" if returncode is None else \
            f"FAILED exit={returncode}, see stdout.log"
        return common.NormalizedResult(
            "libmdbx", workload, scale.label, duration, metric_name, 0.0, 0.0,
            threads=threads, gc_enabled="n/a",
            notes=notes,
        )

    total_ops = common.sum_csv_column(output_dir / ts_file, ts_column)
    value = total_ops / duration if duration else 0.0
    peak_rss_mb = common.max_csv_column(output_dir / "mem_stats.csv", "rss_kb") / 1024.0

    latency = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "count": 0}
    if workload == "ycsb_e":
        latency = common.read_latency_summary(output_dir / "ycsb_scan_latency_summary.csv")
    elif workload in common.HTAP_WORKLOADS:
        # mdbx_tpcc.rs's OLAP thread writes both queries' rows into one tpcc_scan.csv (see
        # MdbxScanResult's doc there) - same "filter by mode column" convention cmvbt.py
        # uses for its own tpcc_scan.csv.
        mode = "ch_q1_pricing_summary" if workload == "htap_q1" else "ch_q6_forecast_revenue"
        latency = common.percentiles_from_samples(
            output_dir / "tpcc_scan.csv", "latency_ns", filter_column="mode", filter_value=mode,
        )
        for k in ("p50", "p95", "p99", "avg"):
            latency[k] /= 1000.0  # ns -> us

    return common.NormalizedResult(
        "libmdbx", workload, scale.label, duration, metric_name, value, peak_rss_mb,
        threads=threads, gc_enabled="n/a",
        scan_p50_us=latency["p50"], scan_p95_us=latency["p95"], scan_p99_us=latency["p99"],
        scan_avg_us=latency["avg"], scan_count=latency["count"],
    )
