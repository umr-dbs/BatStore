"""LeanStore native engine wrapper - drives build/frontend/{tpcc,ycsb} directly
and parses LeanStore's own per-second profiling CSV (log_cr.csv's `tx` /
`new_order_tx` columns - the latter added specifically for tpmC parity with
cMVBT, see frontend/tpc-c/tpcc.cpp).
"""
from __future__ import annotations

from pathlib import Path

from . import common, leanstore_build


def ensure_built() -> None:
    leanstore_build.ensure_built(("tpcc", "ycsb"))


def run(workload: str, scale: common.Scale, output_dir: Path) -> common.NormalizedResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    ssd_path = output_dir / "ssd"
    csv_prefix = output_dir / "log"
    env = leanstore_build.run_env()

    if workload == "tpcc":
        duration = scale.tpcc_duration
        args = [
            str(leanstore_build.binary("tpcc")),
            f"--tpcc_warehouse_count={scale.tpcc_warehouses}",
            f"--worker_threads={scale.tpcc_terminals}",
            f"--dram_gib={scale.dram_gib}",
            f"--ssd_path={ssd_path}", "--trunc",
            f"--csv_path={csv_prefix}",
            f"--run_for_seconds={duration}",
            "--isolation_level=si", "--print_tx_console=false",
        ]
        metric_name, metric_column = "new_order_per_sec", "new_order_tx"
    else:
        letter = workload.split("_", 1)[1]
        duration = scale.ycsb_duration
        args = [
            str(leanstore_build.binary("ycsb")),
            f"--ycsb_tuple_count={scale.ycsb_records}",
            f"--worker_threads={scale.ycsb_threads}",
            f"--ycsb_threads={scale.ycsb_threads}",
            f"--zipf_factor={scale.ycsb_theta}",
            *leanstore_build.ycsb_gflags(letter),
            f"--dram_gib={scale.dram_gib}",
            f"--ssd_path={ssd_path}", "--trunc",
            f"--csv_path={csv_prefix}",
            f"--run_for_seconds={duration}",
            "--isolation_level=si", "--print_tx_console=false",
        ]
        metric_name, metric_column = "ops_per_sec", "tx"

    returncode, peak_rss_mb = common.run_and_track_rss(
        args, cwd=output_dir, env=env, stdout_path=output_dir / "stdout.log",
    )
    if returncode != 0:
        return common.NormalizedResult(
            "leanstore", workload, scale.label, duration, metric_name, 0.0, peak_rss_mb,
            notes=f"FAILED exit={returncode}, see stdout.log",
        )

    total = common.sum_csv_column(Path(f"{csv_prefix}_cr.csv"), metric_column)
    value = total / duration if duration else 0.0
    return common.NormalizedResult("leanstore", workload, scale.label, duration, metric_name, value, peak_rss_mb)
