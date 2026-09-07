"""BatStore engine wrapper: invokes the release binary directly (cwd = output_dir,
since `cargo run -- tpcc|ycsb ...` always writes its CSVs to the current
working directory — see src/bat_bench/tpcc_driver.rs::main_tpcc /
ycsb_driver.rs::main_ycsb, both hardcode `output_dir: PathBuf::from(".")`).

WAL is forced on, unconditionally, for every run here (see the wal_path/args wiring
in run() below) - LeanStore's own WAL (WALMacros.hpp, baked into its B-tree core)
can't be turned off either, so leaving BatStore's WAL off by default would compare a
durable-logging engine against a non-durable one. Written to a tmpfs-backed
scratch dir (common.fresh_scratch_dir), same treatment as every other engine's
on-disk DATA - see common.SCRATCH_ROOT's docstring - so the only overhead this adds
is genuine serialization/fsync cost, not real disk I/O.

Under common.NO_DURABILITY (set by compare_engines_new.py), this instead switches WAL
off entirely (wal_enabled_str below) rather than leaving it on with fsync suppressed -
no such partial toggle exists in bat_wal/writer.rs - and common.fresh_scratch_dir then
returns a plain real-disk directory instead of requiring tmpfs.
"""
from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from typing import Optional

from . import common

REPO_ROOT = common.BATSTORE_REPO
BINARY = REPO_ROOT / "target" / "release" / "batstore"

# BatStore has a real, already-wired `--gc` flag on both drivers (see tpcc_driver.rs/
# ycsb_driver.rs) - a genuine gc=on/off comparison is possible here.
SUPPORTS_GC_TOGGLE = True

# BatStore's TPC-C driver (tpcc_driver.rs) has a real `affinity` flag (DriverConfig::
# affinity - each terminal restricted to its own home warehouse, 0% remote, vs. the
# spec's normal cross-warehouse mix) - see common.AFFINITY_WORKLOADS for which workloads
# actually reach it (plain "tpcc" and the htap_* modes; YCSB/S-YCSB have no such concept).
# No other engine wrapper in this harness has an equivalent (see manual.txt section 6:
# even BatStore's own mdbx_tpcc driver dropped this knob for lack of a libmdbx analog).
SUPPORTS_AFFINITY_TOGGLE = True


def ensure_built() -> None:
    # Always built with --features mdbx-backend (not just when "libmdbx" is also in
    # --engines): batstore.py and libmdbx.py share this exact same binary path, and whichever
    # engine's ensure_built() runs last would otherwise silently determine whether the
    # mdbx_ycsb/mdbx_tpcc subcommands exist - building with the feature unconditionally
    # here removes that ordering dependency entirely. The extra subcommands are inert for
    # BatStore's own tpcc/ycsb/htap_* workloads. Allocator (jemalloc/mimalloc) - see
    # common.batstore_cargo_build_args's doc - comes from BATSTORE_ALLOCATOR/--batstore-allocator.
    subprocess.run(common.batstore_cargo_build_args("mdbx-backend"), cwd=REPO_ROOT, check=True)


def run(
    workload: str, scale: common.Scale, output_dir: Path, gc: str = "on", reload: bool = True,
    big_tree_size: str = "medium", ycsb_payload: str = "standard", read_payload: bool = True,
    scan_pool_workers: Optional[int] = None, affinity: str = "off",
) -> common.NormalizedResult:
    """`reload` is accepted for interface parity with postgres_benchbase.run() but unused -
    every BatStore invocation is a fresh in-process population, there's no persisted state to
    reuse across sweep points.

    `affinity` ("on"/"off", default "off" - matches the hardcoded value every call site used
    before this parameter existed) only reaches the driver for "tpcc" and the htap_* modes
    (positional arg 6 to `BatStore tpcc` - see tpcc_driver.rs::main_tpcc's `affinity` field);
    "on" restricts every terminal to its own home warehouse (0% remote transactions), "off"
    is the spec's normal cross-warehouse mix. Ignored (and reported back as affinity="n/a",
    not whatever was passed) for every other workload, which has no such concept - see
    common.AFFINITY_WORKLOADS.

    `big_tree_size` (tiny/small/medium/large/huge) selects Table::Warehouse/Table::District's
    leaf capacity - see tpcc_schema::BigTreeSize's doc - only wired through for the "tpcc"
    workload (positional arg 21 to `BatStore tpcc`, see tpcc_driver.rs::main_tpcc); left at the
    binary's own "medium" default everywhere else.

    `scan_pool_workers` assigns a shared scan-worker pool (`bat_tree::scan_pool::
    ScanWorkerPool`) for a query to fan its scan out across instead of running it
    sequentially - `ORDER_LINE`'s pool for `htap_q1`/`htap_q6` (positional arg 22 to
    `BatStore tpcc`, `DriverConfig::scan_pool_workers` in tpcc_driver.rs), or the
    usertable's pool for any `ycsb_*` workload (positional arg 20 to `BatStore ycsb`,
    same field in ycsb_driver.rs). `None` (default) omits the positional arg entirely,
    which hands the decision to the binary itself: it auto-enables the pool, sized to
    the machine's own core count, whenever the workload actually benefits (`htap_q1`/
    `htap_q6` mode, or a YCSB mix that issues scans) and the population is large enough
    for the pool to pay off - see `parallel_scan::MIN_ROWS_FOR_SCAN_POOL`'s doc for that
    threshold. Pass `0` to explicitly disable it (the plain sequential path, matching
    every engine's behavior before this parameter existed), or a positive int to force
    an exact worker count. Ignored for every other workload, exactly like `big_tree_size`
    above.
    """
    del reload
    if workload in common.AFFINITY_WORKLOADS and affinity == "on":
        if scale.tpcc_warehouses < scale.tpcc_terminals:
            raise ValueError("warehouse affinity requires at least one warehouse per terminal")
    output_dir.mkdir(parents=True, exist_ok=True)
    gc_bool = "false" if gc == "off" else "true"
    affinity_bool = "true" if affinity == "on" else "false"
    # Only "tpcc" and the htap_* modes reach the driver's `affinity` positional arg at all
    # (see this function's doc) - stamp back "n/a" for every other workload regardless of
    # what was passed in, rather than echoing a setting that was silently ignored.
    affinity_reported = affinity if workload in common.AFFINITY_WORKLOADS else "n/a"
    field_count, field_length = ((1, 8) if ycsb_payload == "u64" else (10, 100))
    # Under common.NO_DURABILITY (see compare_engines_new.py), WAL is switched off
    # entirely rather than left on with fsync somehow suppressed - no such partial toggle
    # exists in bat_wal/writer.rs (its fsync/sync_data call is unconditional whenever WAL
    # is on at all), so "off" here means the driver never opens/writes a WAL file, same as
    # every other engine's own no-durability config in this harness.
    wal_enabled_str = "false" if common.NO_DURABILITY else "true"
    # Fixed, wiped-before-every-run path (matches leanstore.py/wiredtiger.py's ssd_path
    # treatment) - the driver's own `fs::remove_file(wal_path)` before opening it means
    # this only needs to exist, not start empty, but wiping it here keeps behavior
    # identical to every other engine's on-disk DATA dir regardless. Still created even
    # when wal_enabled_str is "false" - harmless, and keeps this codepath uniform.
    wal_path = common.fresh_scratch_dir("batstore_data") / "wal.log"

    if workload == "tpcc":
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        # Positions 7-14 filled explicitly (update_in_place=false,
        # root_star_index="fg", OLAP disabled for this plain TPC-C baseline,
        # olap_param=10.0, num_items/customers_per_district/initial_orders_per_district)
        # so that positions 15-17 (wal_enabled/wal_path/wal_flush_ms) are reachable -
        # Rust's arg() is strictly positional (parms.get(idx)).
        args = [
            str(BINARY), "tpcc", str(scale.tpcc_warehouses), str(threads), str(duration),
            affinity_bool, gc_bool, "false", "fg", "none", "0", "10.0",
            "100000", "3000", "3000", wal_enabled_str, str(wal_path), "5",
            # Positions 18-20 (ch_region/num_suppliers/htap_baseline_secs) filled with the
            # driver's own defaults so position 21 (big_tree_size) is reachable.
            "EUROPE", "10000", "0", big_tree_size,
        ]
        metric_name = "new_order_per_sec"
        ts_file, ts_column = "tpcc_oltp_timeseries.csv", "new_order_committed"
    elif workload in common.HTAP_WORKLOADS:
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        # Run exactly the query named by the workload. The old shared "ch" mode rotates
        # Q1/Q6/Q4/Q5 and therefore made both points pay for unrelated Q4/Q5 joins.
        olap_mode = workload.replace("htap_", "ch_", 1)
        args = [
            str(BINARY), "tpcc", str(scale.tpcc_warehouses), str(threads), str(duration),
            affinity_bool, gc_bool, "false", "fg", olap_mode, str(scale.htap_olap_threads), "10.0",
            "100000", "3000", "3000", wal_enabled_str, str(wal_path), "5", "EUROPE", "10000",
            # Positions 20-21 (htap_baseline_secs/big_tree_size) filled with the driver's
            # own defaults so position 22 (scan_pool_workers) is reachable.
            "0", "32kib",
        ]
        # Omitted entirely (not even "0") when `scan_pool_workers` is `None`: main_tpcc's own
        # CLI parsing then auto-sizes the pool for ch_q1/ch_q6 whenever the population is
        # large enough - see `run()`'s doc above. An explicit `0`/`N` is sent through as-is.
        if scan_pool_workers is not None:
            args.append(str(scan_pool_workers))
        metric_name = "new_order_per_sec"
        ts_file, ts_column = "tpcc_oltp_timeseries.csv", "new_order_committed"
    elif workload == "s_htap":
        duration = scale.s_htap_duration
        # scale.ycsb_threads holds the swept --threads value for every workload (see
        # compare_engines.py's scale_variant construction) - split it into a fixed
        # OLAP-scanner pool plus the remainder as write threads, rather than sweeping
        # write/OLAP counts independently.
        threads = scale.ycsb_threads
        olap_threads = min(scale.s_htap_olap_threads, max(1, threads - 1))
        write_threads = max(1, threads - olap_threads)
        args = [
            str(BINARY), "s_ycsb", str(scale.s_htap_record_count), str(write_threads),
            str(olap_threads), str(duration), str(scale.s_htap_hot_window),
            str(scale.s_htap_theta), str(scale.s_htap_arrival_ratio),
            str(scale.s_htap_max_lateness), str(scale.s_htap_olap_lag),
            str(scale.s_htap_olap_span), str(field_count), str(field_length),
            "false", str(read_payload).lower(), "fg", gc_bool, "false", wal_enabled_str,
            str(wal_path), "5", os.environ.get("BATSTORE_YCSB_MODE", "atomic"),
        ]
        metric_name = "write_ops_per_sec"
        ts_file, ts_column = "s_ycsb_timeseries.csv", "ops_completed"
    else:
        letter = workload.split("_", 1)[1]
        duration = scale.ycsb_duration
        threads = scale.ycsb_threads
        # Positions 8-11 (field_count/field_length/max_scan_length/root_star_index) and 13
        # (update_in_place) filled with the driver's own defaults so positions 14-16
        # (wal_enabled/wal_path/wal_flush_ms) and position 17
        # (write_all_fields=false, standard YCSB default) are reachable.
        args = [
            str(BINARY), "ycsb", letter, str(scale.ycsb_records), str(threads),
            str(duration), "default", str(scale.ycsb_theta),
            str(field_count), str(field_length), "100", "fg", gc_bool, "false", wal_enabled_str, str(wal_path), "5", "false",
            str(read_payload).lower(),
            os.environ.get("BATSTORE_YCSB_MODE", "atomic"),
        ]
        # Same omit-for-auto convention as the htap_q1/htap_q6 branch above: leaving this
        # off lets main_ycsb's own CLI parsing (`default_scan_pool_workers`) auto-enable the
        # pool whenever this workload's mix issues scans and the population is large enough.
        if scan_pool_workers is not None:
            args.append(str(scan_pool_workers))
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
            "batstore", workload, scale.label, duration, metric_name, 0.0, 0.0,
            threads=threads, gc_enabled=gc, affinity=affinity_reported,
            notes=notes,
        )

    if workload in common.AFFINITY_WORKLOADS:
        log = (output_dir / "stdout.log").read_text(errors="replace")
        match = re.search(r"terminals \(OLTP\)\s*=\s*(\d+)", log)
        if match is None or int(match.group(1)) != threads:
            actual = match.group(1) if match else "unknown"
            return common.NormalizedResult(
                "batstore", workload, scale.label, duration, metric_name, 0.0, 0.0,
                threads=threads, gc_enabled=gc, affinity=affinity_reported,
                notes=f"FAILED: requested {threads} terminals but driver reported {actual}; "
                      "rebuild BatStore and check warehouse/worker capacity",
            )

    if not common.NO_DURABILITY and (not wal_path.is_file() or wal_path.stat().st_size == 0):
        return common.NormalizedResult(
            "batstore", workload, scale.label, duration, metric_name, 0.0, 0.0,
            threads=threads, gc_enabled=gc, affinity=affinity_reported,
            notes=f"FAILED: BatStore WAL was enabled but {wal_path} is missing or empty",
        )

    total_ops = common.sum_csv_column(output_dir / ts_file, ts_column)
    value = total_ops / duration if duration else 0.0
    peak_rss_mb = common.max_csv_column(output_dir / "mem_stats.csv", "rss_kb") / 1024.0

    # Scan/OLAP latency: workload "ycsb_e" has its own pre-computed summary (see
    # ycsb_driver.rs::write_results); explicit HTAP workloads write raw per-query samples
    # to tpcc_scan.csv. Plain TPC-C deliberately has no analytical thread.
    if workload == "ycsb_e":
        latency = common.read_latency_summary(output_dir / "ycsb_scan_latency_summary.csv")
    elif workload == "s_htap":
        latency = common.read_latency_summary(output_dir / "s_ycsb_scan_latency_summary.csv")
    elif workload in common.HTAP_WORKLOADS:
        mode = {
            "htap_q1": "ch_q1_pricing_summary",
            "htap_q6": "ch_q6_forecast_revenue",
            "htap_q1_variant": "ch_q1_variant",
            "htap_q6_variant": "ch_q6_variant",
        }[workload]
        latency = common.percentiles_from_samples(
            output_dir / "tpcc_scan.csv", "latency_ns", filter_column="mode", filter_value=mode,
        )
        for k in ("p50", "p95", "p99", "avg"):
            latency[k] /= 1000.0  # ns -> us
    else:
        latency = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "count": 0}

    return common.NormalizedResult(
        "batstore", workload, scale.label, duration, metric_name, value, peak_rss_mb,
        threads=threads, gc_enabled=gc, affinity=affinity_reported,
        scan_p50_us=latency["p50"], scan_p95_us=latency["p95"], scan_p99_us=latency["p99"],
        scan_avg_us=latency["avg"], scan_count=latency["count"],
    )
