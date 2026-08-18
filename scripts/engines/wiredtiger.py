"""WiredTiger engine wrapper - drives LeanStore's WiredTigerAdapter-based
binaries (build/frontend/wiredtiger_{tpcc,ycsb}), which implement real
multi-table TPC-C / YCSB against WiredTiger's C API via the same
Adapter<Record> interface LeanStore's own B-tree uses (see
frontend/shared/WiredTigerAdapter.hpp). Same CLI flags as leanstore.py -
these binaries share LeanStore's gflags-based Config.
"""
from __future__ import annotations

import csv
import io
import os
from pathlib import Path

from . import common, leanstore_build

# No user-facing version-GC toggle exists in this adapter (it doesn't touch LeanStore's
# BTreeVI/pgc at all - separate code path against WiredTiger's own C API - and WiredTiger
# itself exposes no equivalent switch for its history-store version cleanup). Only ever
# reports gc_enabled="n/a", same reasoning as leanstore.py's SUPPORTS_GC_TOGGLE.
SUPPORTS_GC_TOGGLE = False


def ensure_built() -> None:
    leanstore_build.ensure_built(("wiredtiger_tpcc", "wiredtiger_ycsb", "wiredtiger_s_htap"))


def _sum_stdout_column(stdout_path: Path, column: str) -> float:
    """The wiredtiger_{tpcc,ycsb} binaries print their own per-second CSV to
    stdout (not LeanStore's ProfilingTable) - see the `print_header` block in
    frontend/tpc-c/wiredtiger_tpcc.cpp / frontend/ycsb/wiredtiger_ycsb.cpp.
    """
    if not stdout_path.exists():
        return 0.0
    text = stdout_path.read_text(errors="replace")
    # Only the trailing CSV block (after the "~Transactions"/insert-phase banner) is real data.
    lines = text.splitlines()
    header_idx = next((i for i, l in enumerate(lines) if l.startswith("t,tag,")), None)
    if header_idx is None:
        return 0.0
    csv_text = "\n".join(lines[header_idx:])
    total = 0.0
    for row in csv.DictReader(io.StringIO(csv_text)):
        value = row.get(column)
        # DictReader maps a short row (fewer commas than the header - e.g. a trailing
        # non-CSV summary line some binaries print after their per-second loop, such as
        # wiredtiger_s_htap.cpp's "new_arrival ..." counts) to None for any column past
        # what that row actually has - skip it rather than let float(None) raise.
        if value is None:
            continue
        try:
            total += float(value)
        except ValueError:
            continue
    return total


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "n/a", reload: bool = True,
        ycsb_payload: str = "standard", read_payload: bool = True) -> common.NormalizedResult:
    """`gc`/`reload` accepted for interface parity with the other engine wrappers but
    unused - see SUPPORTS_GC_TOGGLE above, and every run here is a fresh ssd_dir."""
    del reload
    output_dir.mkdir(parents=True, exist_ok=True)
    # Fixed, wiped-before-every-run path (not a unique per-combo directory) - matches
    # leanstore.py's treatment, keeps disk usage bounded across a long sweep.
    ssd_dir = common.fresh_scratch_dir("wiredtiger_data") / "ssd"
    ssd_dir.mkdir(parents=True, exist_ok=True)
    env = leanstore_build.run_env()
    stdout_path = output_dir / "stdout.log"

    if workload == "tpcc":
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        args = [
            str(leanstore_build.binary("wiredtiger_tpcc")),
            f"--tpcc_warehouse_count={scale.tpcc_warehouses}",
            f"--worker_threads={threads}",
            f"--dram_gib={scale.dram_gib}",
            f"--ssd_path={ssd_dir}",
            f"--run_for_seconds={duration}",
            "--isolation_level=si", "--print_header",
        ]
        metric_name, metric_column = "new_order_per_sec", "oltp_new_order_committed"
    elif workload in ("htap_q1", "htap_q6"):
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        query_no = 101 if workload == "htap_q1" else 106
        # +1 worker: same reasoning as leanstore.py's htap_q1/htap_q6 branch - this
        # adapter carves ch_a_threads out of worker_threads too (see wiredtiger_tpcc.cpp).
        args = [
            str(leanstore_build.binary("wiredtiger_tpcc")),
            f"--tpcc_warehouse_count={scale.tpcc_warehouses}",
            f"--worker_threads={threads + 1}",
            "--ch_a_threads=1", "--ch_a_rounds=1", f"--ch_a_query={query_no}",
            f"--dram_gib={scale.dram_gib}",
            f"--ssd_path={ssd_dir}",
            f"--run_for_seconds={duration}",
            "--isolation_level=si", "--print_header",
        ]
        metric_name, metric_column = "new_order_per_sec", "oltp_new_order_committed"
    elif workload == "s_htap":
        duration = scale.s_htap_duration
        threads = scale.ycsb_threads
        olap_threads = min(scale.s_htap_olap_threads, max(1, threads - 1))
        write_threads = max(1, threads - olap_threads)
        args = [
            str(leanstore_build.binary("wiredtiger_s_htap")),
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
            f"--ssd_path={ssd_dir}",
            f"--run_for_seconds={duration}",
            "--isolation_level=si", "--print_header",
        ]
        metric_name, metric_column = "write_ops_per_sec", "s_htap_committed"
    else:
        letter = workload.split("_", 1)[1]
        duration = scale.ycsb_duration
        threads = scale.ycsb_threads
        args = [
            str(leanstore_build.binary("wiredtiger_ycsb")),
            f"--ycsb_tuple_count={scale.ycsb_records}",
            f"--worker_threads={threads}",
            f"--zipf_factor={scale.ycsb_theta}",
            *leanstore_build.ycsb_gflags(letter),
            f"--ycsb_payload_size={8 if ycsb_payload == 'u64' else 1000}",
            f"--ycsb_field_count={1 if ycsb_payload == 'u64' else 10}",
            f"--ycsb_field_length={8 if ycsb_payload == 'u64' else 100}",
            f"--ycsb_write_all_fields={os.environ.get('YCSB_WRITE_ALL_FIELDS', 'false')}",
            f"--ycsb_read_payload={'true' if read_payload else 'false'}",
            f"--dram_gib={scale.dram_gib}",
            f"--ssd_path={ssd_dir}",
            f"--run_for_seconds={duration}",
            "--isolation_level=si", "--print_header",
        ]
        metric_name, metric_column = "ops_per_sec", "oltp_committed"

    timeout = common.default_subprocess_timeout(duration)
    returncode, peak_rss_mb = common.run_and_track_rss(
        args, cwd=output_dir, env=env, stdout_path=stdout_path, timeout=timeout,
    )
    if returncode != 0:
        notes = f"TIMEOUT after {timeout:.0f}s, see stdout.log" if returncode is None else \
            f"FAILED exit={returncode}, see stdout.log"
        return common.NormalizedResult(
            "wiredtiger", workload, scale.label, duration, metric_name, 0.0, peak_rss_mb,
            threads=threads, gc_enabled="n/a",
            notes=notes,
        )

    total = _sum_stdout_column(stdout_path, metric_column)
    value = total / duration if duration else 0.0

    latency = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "count": 0}
    if workload == "ycsb_e":
        latency = common.read_latency_summary(output_dir / "ycsb_scan_latency_summary.csv")
    elif workload == "s_htap":
        latency = common.read_latency_summary(output_dir / "s_htap_scan_latency_summary.csv")
    elif workload in ("htap_q1", "htap_q6"):
        latency = common.read_latency_summary(output_dir / "ch_query_latency_summary.csv")

    return common.NormalizedResult(
        "wiredtiger", workload, scale.label, duration, metric_name, value, peak_rss_mb,
        threads=threads, gc_enabled="n/a",
        scan_p50_us=latency["p50"], scan_p95_us=latency["p95"], scan_p99_us=latency["p99"],
        scan_avg_us=latency["avg"], scan_count=latency["count"],
    )
