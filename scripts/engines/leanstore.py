"""LeanStore native engine wrapper - drives build/frontend/{tpcc,ycsb} directly
and parses LeanStore's own per-second profiling CSV (log_cr.csv's `tx` /
`new_order_tx` columns - the latter added specifically for tpmC parity with
cMVBT, see frontend/tpc-c/tpcc.cpp).
"""
from __future__ import annotations

from pathlib import Path

from . import common, leanstore_build

# LeanStore's --pgc flag is dead code in this checkout (grep confirms it's only read in
# ConfigsTable.cpp to report its own value - nothing in BTreeVI.cpp branches on it; its
# page-wise garbage collection runs unconditionally). No working version-GC toggle exists
# here, so this engine only ever reports gc_enabled="n/a".
SUPPORTS_GC_TOGGLE = False


def ensure_built() -> None:
    leanstore_build.ensure_built(("tpcc", "ycsb", "s_htap"))


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "n/a", reload: bool = True,
        ycsb_payload: str = "standard", read_payload: bool = True) -> common.NormalizedResult:
    """`gc`/`reload` accepted for interface parity with the other engine wrappers but
    unused - see SUPPORTS_GC_TOGGLE above, and every run here is a fresh --trunc load."""
    del reload
    output_dir.mkdir(parents=True, exist_ok=True)
    # Fixed, wiped-before-every-run path (not a unique per-combo directory) - LeanStore's
    # on-disk data never accumulates across a long sweep this way. Small artifacts
    # (stdout.log, CSVs) still go in output_dir, which stays per-combo for inspection.
    ssd_path = common.fresh_scratch_dir("leanstore_data") / "ssd"
    csv_prefix = output_dir / "log"
    env = leanstore_build.run_env()

    if workload == "tpcc":
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        args = [
            str(leanstore_build.binary("tpcc")),
            f"--tpcc_warehouse_count={scale.tpcc_warehouses}",
            f"--worker_threads={threads}",
            f"--dram_gib={scale.dram_gib}",
            f"--ssd_path={ssd_path}", "--trunc",
            f"--csv_path={csv_prefix}",
            f"--run_for_seconds={duration}",
            "--isolation_level=si", "--print_tx_console=false",
        ]
        metric_name, metric_column = "new_order_per_sec", "new_order_tx"
    elif workload in ("htap_q1", "htap_q6"):
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        query_no = 101 if workload == "htap_q1" else 106
        # +1 worker: `threads` OLTP threads plus 1 dedicated CH-analytics thread (LeanStore
        # carves ch_a_threads *out of* worker_threads - see tpcc.cpp - so worker_threads
        # must be threads+1 to keep the OLTP thread count comparable to the plain "tpcc"
        # workload at the same sweep point, matching cMVBT's additive OLTP+OLAP design).
        args = [
            str(leanstore_build.binary("tpcc")),
            f"--tpcc_warehouse_count={scale.tpcc_warehouses}",
            f"--worker_threads={threads + 1}",
            "--ch_a_threads=1", "--ch_a_rounds=1", f"--ch_a_query={query_no}",
            f"--dram_gib={scale.dram_gib}",
            f"--ssd_path={ssd_path}", "--trunc",
            f"--csv_path={csv_prefix}",
            f"--run_for_seconds={duration}",
            "--isolation_level=si", "--print_tx_console=false",
        ]
        metric_name, metric_column = "new_order_per_sec", "new_order_tx"
    elif workload == "s_htap":
        duration = scale.s_htap_duration
        # scale.ycsb_threads holds the swept --threads value for every workload (see
        # compare_engines.py's scale_variant construction) - split it into a fixed
        # OLAP-scanner pool plus the remainder as write threads, matching cmvbt.py's
        # s_htap branch exactly.
        threads = scale.ycsb_threads
        olap_threads = min(scale.s_htap_olap_threads, max(1, threads - 1))
        write_threads = max(1, threads - olap_threads)
        args = [
            str(leanstore_build.binary("s_htap")),
            f"--s_htap_record_count={scale.s_htap_record_count}",
            f"--s_htap_write_threads={write_threads}",
            f"--s_htap_olap_threads={olap_threads}",
            f"--s_htap_hot_window={scale.s_htap_hot_window}",
            f"--s_htap_hot_theta={scale.s_htap_theta}",
            f"--s_htap_arrival_ratio={scale.s_htap_arrival_ratio}",
            f"--s_htap_max_lateness={scale.s_htap_max_lateness}",
            f"--s_htap_olap_lag={scale.s_htap_olap_lag}",
            f"--s_htap_olap_span={scale.s_htap_olap_span}",
            f"--worker_threads={write_threads + olap_threads}",
            f"--dram_gib={scale.dram_gib}",
            f"--ssd_path={ssd_path}", "--trunc",
            f"--csv_path={csv_prefix}",
            f"--run_for_seconds={duration}",
            "--isolation_level=si", "--print_tx_console=false",
        ]
        # "tx" (not an s_htap-specific counter): every write op here is one
        # commitTX(), same generic per-commit counter YCSB uses.
        metric_name, metric_column = "write_ops_per_sec", "tx"
    else:
        letter = workload.split("_", 1)[1]
        duration = scale.ycsb_duration
        threads = scale.ycsb_threads
        args = [
            str(leanstore_build.binary("ycsb")),
            f"--ycsb_tuple_count={scale.ycsb_records}",
            f"--worker_threads={threads}",
            f"--ycsb_threads={threads}",
            f"--zipf_factor={scale.ycsb_theta}",
            *leanstore_build.ycsb_gflags(letter),
            f"--ycsb_payload_size={8 if ycsb_payload == 'u64' else 1000}",
            f"--ycsb_read_payload={'true' if read_payload else 'false'}",
            f"--dram_gib={scale.dram_gib}",
            f"--ssd_path={ssd_path}", "--trunc",
            f"--csv_path={csv_prefix}",
            f"--run_for_seconds={duration}",
            "--isolation_level=si", "--print_tx_console=false",
        ]
        metric_name, metric_column = "ops_per_sec", "tx"

    timeout = common.default_subprocess_timeout(duration)
    returncode, peak_rss_mb = common.run_and_track_rss(
        args, cwd=output_dir, env=env, stdout_path=output_dir / "stdout.log", timeout=timeout,
    )
    if returncode != 0:
        notes = f"TIMEOUT after {timeout:.0f}s, see stdout.log" if returncode is None else \
            f"FAILED exit={returncode}, see stdout.log"
        return common.NormalizedResult(
            "leanstore", workload, scale.label, duration, metric_name, 0.0, peak_rss_mb,
            threads=threads, gc_enabled="n/a",
            notes=notes,
        )

    total = common.sum_csv_column(Path(f"{csv_prefix}_cr.csv"), metric_column)
    value = total / duration if duration else 0.0

    latency = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "count": 0}
    if workload == "ycsb_e":
        latency = common.read_latency_summary(output_dir / "ycsb_scan_latency_summary.csv")
    elif workload == "s_htap":
        latency = common.read_latency_summary(output_dir / "s_htap_scan_latency_summary.csv")
    elif workload in ("htap_q1", "htap_q6"):
        latency = common.read_latency_summary(output_dir / "ch_query_latency_summary.csv")

    return common.NormalizedResult(
        "leanstore", workload, scale.label, duration, metric_name, value, peak_rss_mb,
        threads=threads, gc_enabled="n/a",
        scan_p50_us=latency["p50"], scan_p95_us=latency["p95"], scan_p99_us=latency["p99"],
        scan_avg_us=latency["avg"], scan_count=latency["count"],
    )
