"""cMVBT engine wrapper: invokes the release binary directly (cwd = output_dir,
since `cargo run -- tpcc|ycsb ...` always writes its CSVs to the current
working directory — see src/mv_bench/tpcc_driver.rs::main_tpcc /
ycsb_driver.rs::main_ycsb, both hardcode `output_dir: PathBuf::from(".")`).
"""
from __future__ import annotations

import subprocess
from pathlib import Path

from . import common

REPO_ROOT = Path("/home/amir/RustroverProjects/cMVBT")
BINARY = REPO_ROOT / "target" / "release" / "cMVBT"


def ensure_built() -> None:
    subprocess.run(["cargo", "build", "--release"], cwd=REPO_ROOT, check=True)


def run(workload: str, scale: common.Scale, output_dir: Path) -> common.NormalizedResult:
    output_dir.mkdir(parents=True, exist_ok=True)

    if workload == "tpcc":
        duration = scale.tpcc_duration
        args = [str(BINARY), "tpcc", str(scale.tpcc_warehouses), str(scale.tpcc_terminals), str(duration)]
        metric_name = "new_order_per_sec"
        ts_file, ts_column = "tpcc_oltp_timeseries.csv", "new_order_committed"
    else:
        letter = workload.split("_", 1)[1]
        duration = scale.ycsb_duration
        args = [
            str(BINARY), "ycsb", letter, str(scale.ycsb_records), str(scale.ycsb_threads),
            str(duration), "default", str(scale.ycsb_theta),
        ]
        metric_name = "ops_per_sec"
        ts_file, ts_column = "ycsb_timeseries.csv", "ops_completed"

    returncode, _peak_rss_unused = common.run_and_track_rss(
        args, cwd=output_dir, stdout_path=output_dir / "stdout.log",
    )
    if returncode != 0:
        return common.NormalizedResult(
            "cmvbt", workload, scale.label, duration, metric_name, 0.0, 0.0,
            notes=f"FAILED exit={returncode}, see stdout.log",
        )

    total_ops = common.sum_csv_column(output_dir / ts_file, ts_column)
    value = total_ops / duration if duration else 0.0
    peak_rss_mb = common.max_csv_column(output_dir / "mem_stats.csv", "rss_kb") / 1024.0

    return common.NormalizedResult("cmvbt", workload, scale.label, duration, metric_name, value, peak_rss_mb)
