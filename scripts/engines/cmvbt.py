"""cMVBT engine wrapper: invokes the release binary directly (cwd = output_dir,
since `cargo run -- tpcc|ycsb ...` always writes its CSVs to the current
working directory — see src/mv_bench/tpcc_driver.rs::main_tpcc /
ycsb_driver.rs::main_ycsb, both hardcode `output_dir: PathBuf::from(".")`).
"""
from __future__ import annotations

import subprocess
from pathlib import Path

from . import common

REPO_ROOT = common.CMVBT_REPO
BINARY = REPO_ROOT / "target" / "release" / "cMVBT"

# cMVBT has a real, already-wired `--gc` flag on both drivers (see tpcc_driver.rs/
# ycsb_driver.rs) - a genuine gc=on/off comparison is possible here.
SUPPORTS_GC_TOGGLE = True


def ensure_built() -> None:
    subprocess.run(["cargo", "build", "--release"], cwd=REPO_ROOT, check=True)


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "on", reload: bool = True) -> common.NormalizedResult:
    """`reload` is accepted for interface parity with postgres_benchbase.run() but unused -
    every cMVBT invocation is a fresh in-process population, there's no persisted state to
    reuse across sweep points.
    """
    del reload
    output_dir.mkdir(parents=True, exist_ok=True)
    gc_bool = "false" if gc == "off" else "true"

    if workload == "tpcc":
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        # Positions 0-4: warehouses/terminals/duration. Rust's arg() is strictly
        # positional (parms.get(idx)), so slot 5 (affinity) must be filled explicitly to
        # reach slot 6 (gc) - "false" matches the driver's own default for affinity.
        args = [
            str(BINARY), "tpcc", str(scale.tpcc_warehouses), str(threads), str(duration),
            "false", gc_bool,
        ]
        metric_name = "new_order_per_sec"
        ts_file, ts_column = "tpcc_oltp_timeseries.csv", "new_order_committed"
    elif workload in ("htap_q1", "htap_q6"):
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        # olap_mode_str="ch" activates the real CH-benCHmark OLAP thread (rotates through
        # every query cMVBT implements - Q1/Q6/Q4/Q5 - see tpcc_driver.rs::main_tpcc); only
        # this workload's own query's rows are read back out of tpcc_scan.csv below (see
        # HTAP_WORKLOADS's module doc in common.py for why Q4/Q5 aren't part of this
        # cross-engine comparison). Positions 7-19 filled with the driver's own defaults so
        # position 9 (olap_mode_str="ch") and 10 (num_olap_threads=1) are reachable.
        args = [
            str(BINARY), "tpcc", str(scale.tpcc_warehouses), str(threads), str(duration),
            "false", gc_bool, "false", "fg", "ch", "1", "10.0",
            "100000", "3000", "3000", "false", "tpcc_wal.log", "5", "EUROPE", "10000",
        ]
        metric_name = "new_order_per_sec"
        ts_file, ts_column = "tpcc_oltp_timeseries.csv", "new_order_committed"
    else:
        letter = workload.split("_", 1)[1]
        duration = scale.ycsb_duration
        threads = scale.ycsb_threads
        # Positions 8-11 (field_count/field_length/max_scan_length/root_star_index) filled
        # with the driver's own defaults so position 12 (gc) is reachable.
        args = [
            str(BINARY), "ycsb", letter, str(scale.ycsb_records), str(threads),
            str(duration), "default", str(scale.ycsb_theta),
            "10", "100", "100", "fg", gc_bool,
        ]
        metric_name = "ops_per_sec"
        ts_file, ts_column = "ycsb_timeseries.csv", "ops_completed"

    returncode, _peak_rss_unused = common.run_and_track_rss(
        args, cwd=output_dir, stdout_path=output_dir / "stdout.log",
    )
    if returncode != 0:
        return common.NormalizedResult(
            "cmvbt", workload, scale.label, duration, metric_name, 0.0, 0.0,
            threads=threads, gc_enabled=gc,
            notes=f"FAILED exit={returncode}, see stdout.log",
        )

    total_ops = common.sum_csv_column(output_dir / ts_file, ts_column)
    value = total_ops / duration if duration else 0.0
    peak_rss_mb = common.max_csv_column(output_dir / "mem_stats.csv", "rss_kb") / 1024.0

    # Scan/OLAP latency: workload "ycsb_e" has its own pre-computed summary (see
    # ycsb_driver.rs::write_results); TPC-C's HTAP scan-sweep OLAP thread (always active by
    # default, see tpcc_driver.rs::main_tpcc's olap_mode_str default) writes raw per-scan
    # samples to tpcc_scan.csv instead - percentiles computed here from those.
    if workload == "ycsb_e":
        latency = common.read_latency_summary(output_dir / "ycsb_scan_latency_summary.csv")
    elif workload == "tpcc":
        latency = common.percentiles_from_samples(output_dir / "tpcc_scan.csv", "latency_ns")
        for k in ("p50", "p95", "p99", "avg"):
            latency[k] /= 1000.0  # ns -> us
    elif workload in ("htap_q1", "htap_q6"):
        mode = "ch_q1_pricing_summary" if workload == "htap_q1" else "ch_q6_forecast_revenue"
        latency = common.percentiles_from_samples(
            output_dir / "tpcc_scan.csv", "latency_ns", filter_column="mode", filter_value=mode,
        )
        for k in ("p50", "p95", "p99", "avg"):
            latency[k] /= 1000.0  # ns -> us
    else:
        latency = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}

    return common.NormalizedResult(
        "cmvbt", workload, scale.label, duration, metric_name, value, peak_rss_mb,
        threads=threads, gc_enabled=gc,
        scan_p50_us=latency["p50"], scan_p95_us=latency["p95"], scan_p99_us=latency["p99"],
        scan_count=latency["count"],
    )
