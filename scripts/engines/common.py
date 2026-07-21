"""Shared types and helpers for the cross-engine benchmark harness.

Normalizes cMVBT/LeanStore/WiredTiger/PostgreSQL onto one comparable schema (see
MANIFEST_HEADER below) so a single manifest.csv + plotting script can overlay all four.
See manual.txt (repo root) for the full usage guide.
"""
from __future__ import annotations

import csv
import dataclasses
import os
import subprocess
import threading
from pathlib import Path
from typing import Optional

# Single source of truth for every sibling-repo checkout this harness drives, env-var
# overridable so the same scripts work unchanged on the real server (whose home directory
# layout may differ from this workstation's).
#
# CMVBT_REPO is self-referential (matching setup_environment.py's existing CMVBT_REPO) -
# this file lives at <cmvbt-repo>/scripts/engines/common.py, so its own grandparent
# directory is always the correct cMVBT checkout to build/run, whether that's
# RustroverProjects/cMVBT (this workstation's dev repo) or wherever the repo lives on the
# server - no hardcoded path or env var needed for the common case.
CMVBT_REPO = Path(os.environ.get("CMVBT_REPO", str(Path(__file__).resolve().parent.parent.parent)))
LEANSTORE_REPO = Path(os.environ.get("LEANSTORE_REPO", "/home/amir/tx_tests/leanstore"))
# Directory name is a misleading holdover from an old CLion default - it's actually
# configured with -DCMAKE_BUILD_TYPE=Release (see setup_environment.py::step_wiredtiger and
# leanstore_build.py's build-type sanity check).
WIREDTIGER_BUILD_DIR = Path(os.environ.get("WIREDTIGER_BUILD_DIR", "/home/amir/tx_tests/wiredtiger/cmake-build-debug"))
BENCHBASE_HOME = Path(os.environ.get("BENCHBASE_HOME", "/home/amir/tx_tests/benchbase/target/benchbase-postgres"))

# Matches the role/database setup_environment.py::step_postgres creates (admin is a
# SUPERUSER role, needed for postgres_benchbase.py's ALTER SYSTEM autovacuum toggle).
PG_ROLE = os.environ.get("PG_ROLE", "admin")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "password")
PG_DATABASE = os.environ.get("PG_DATABASE", "benchbase")

# This harness always pins to one NUMA node - matches the real server (2x AMD EPYC 7742,
# 2 NUMA nodes) where cross-node traffic would otherwise confound every measurement here.
# Deliberately unconditional: if `numactl` isn't installed, run_and_track_rss should fail
# loudly (FileNotFoundError) rather than silently fall back to unpinned execution, which
# would invalidate the whole point of this flag on the server.
NUMA_NODE = 0


def numactl_prefix() -> list:
    return ["numactl", f"--cpubind={NUMA_NODE}", f"--membind={NUMA_NODE}"]


def check_release_build(build_dir: Path, label: str) -> None:
    """Fails loudly if a CMake build directory isn't configured as
    CMAKE_BUILD_TYPE=Release - every engine here must be release/optimized, never a stray
    Debug/RelWithDebInfo leftover from an IDE-generated build dir. This has actually
    happened: the tx_tests wiredtiger and leanstore build dirs were both found configured
    against the wrong source tree (a leftover CLionProjects-relative CMakeCache), one of
    them in plain Debug, before this check existed.
    """
    import sys

    cache = build_dir / "CMakeCache.txt"
    if not cache.exists():
        return  # not configured yet - the caller's own cmake -S/-B step will set it up
    build_type = next(
        (line.split("=", 1)[1].strip() for line in cache.read_text().splitlines()
         if line.startswith("CMAKE_BUILD_TYPE:STRING=")),
        "",
    )
    if build_type != "Release":
        sys.exit(
            f"{label} build dir {build_dir} is configured as CMAKE_BUILD_TYPE={build_type!r}, "
            f"not 'Release' - benchmark numbers from a non-release build are meaningless. "
            f"Delete {cache} and reconfigure, or fix it manually with "
            f"`cmake -S <src> -B {build_dir} -DCMAKE_BUILD_TYPE=Release`."
        )


@dataclasses.dataclass
class Scale:
    """Workstation-sized scale (24 cores / 32GB), not the paper's 64-core/512GB server."""

    tpcc_warehouses: int = 8
    tpcc_terminals: int = 16
    tpcc_duration: int = 60
    ycsb_records: int = 2_000_000
    ycsb_threads: int = 16
    ycsb_duration: int = 30
    ycsb_theta: float = 0.99
    dram_gib: float = 8.0  # LeanStore/WiredTiger buffer pool / cache size; unused by cmvbt and postgres
    label: str = "workstation"


TINY_SCALE = Scale(
    tpcc_warehouses=1, tpcc_terminals=2, tpcc_duration=10,
    ycsb_records=10_000, ycsb_threads=2, ycsb_duration=10,
    dram_gib=1.0, label="tiny",
)


@dataclasses.dataclass
class NormalizedResult:
    engine: str
    workload: str
    config_label: str
    duration_secs: float
    primary_metric_name: str
    primary_metric_value: float
    peak_rss_mb: float
    threads: int = 0
    # "on" | "off" | "n/a" (engine has no working version-GC toggle - see leanstore.py/
    # wiredtiger.py's SUPPORTS_GC_TOGGLE = False and the plan's Context section for why).
    gc_enabled: str = "n/a"
    # Scan/OLAP-scan latency (microseconds), populated only for workload == "ycsb_e" (all
    # engines) or workload == "tpcc" and engine == "cmvbt" (its existing HTAP scan-sweep
    # mode) - 0 elsewhere.
    scan_p50_us: float = 0.0
    scan_p95_us: float = 0.0
    scan_p99_us: float = 0.0
    scan_count: int = 0
    notes: str = ""


MANIFEST_HEADER = [
    "engine", "workload", "config_label", "threads", "gc_enabled", "duration_secs",
    "primary_metric_name", "primary_metric_value", "peak_rss_mb",
    "scan_p50_us", "scan_p95_us", "scan_p99_us", "scan_count", "notes",
]

YCSB_WORKLOADS = ["ycsb_a", "ycsb_b", "ycsb_c", "ycsb_d", "ycsb_e", "ycsb_f"]
# HTAP/CH-benCHmark (TPC-C OLTP running concurrently with one CH-benCHmark/TPC-H-style
# analytical query): restricted to Q1 ("Pricing Summary Report") and Q6 ("Forecasting
# Revenue Change") - the only 2 of CH-benCHmark's 22 queries genuinely implemented across
# all 4 engines (cMVBT and LeanStore/WiredTiger's hand-written scans, BenchBase's real SQL
# for PostgreSQL - see the plan's Context section for why the other queries aren't
# comparable everywhere). Each is one full ORDER_LINE table scan (pure aggregation for Q1,
# filtered aggregation for Q6), no joins - see src/mv_bench/tpch_queries.rs::q1/q6, the
# reference implementation this ports.
HTAP_WORKLOADS = ["htap_q1", "htap_q6"]
ALL_WORKLOADS = ["tpcc"] + YCSB_WORKLOADS + HTAP_WORKLOADS
ENGINES = ["cmvbt", "leanstore", "wiredtiger", "postgres"]


def _read_vmhwm_kb(pid: int) -> float:
    """Peak resident set size (VmHWM) for a running process, 0 if it has already exited."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return float(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        pass
    return 0.0


def run_and_track_rss(cmd, cwd=None, env=None, stdout_path: Optional[Path] = None, timeout=None):
    """Runs `cmd` to completion under `numactl --cpubind=0 --membind=0`, sampling peak RSS
    via /proc every 0.5s.

    Returns (returncode, peak_rss_mb). stdout+stderr are merged and written to
    stdout_path if given (for post-hoc debugging), else discarded.
    """
    cmd = numactl_prefix() + [str(c) for c in cmd]
    stdout_file = open(stdout_path, "wb") if stdout_path else subprocess.DEVNULL
    proc = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=stdout_file, stderr=subprocess.STDOUT)
    peak_kb = 0.0
    stop = threading.Event()

    def sampler():
        nonlocal peak_kb
        while not stop.is_set():
            peak_kb = max(peak_kb, _read_vmhwm_kb(proc.pid))
            stop.wait(0.5)

    t = threading.Thread(target=sampler, daemon=True)
    t.start()
    try:
        proc.wait(timeout=timeout)
    finally:
        stop.set()
        t.join()
        if stdout_path:
            stdout_file.close()

    return proc.returncode, peak_kb / 1024.0


def sum_csv_column(csv_path: Path, column: str) -> float:
    if not csv_path.exists():
        return 0.0
    total = 0.0
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            try:
                total += float(row[column])
            except (KeyError, ValueError):
                continue
    return total


def avg_csv_column(csv_path: Path, column: str) -> float:
    if not csv_path.exists():
        return 0.0
    total, count = 0.0, 0
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            try:
                total += float(row[column])
                count += 1
            except (KeyError, ValueError):
                continue
    return total / count if count else 0.0


def max_csv_column(csv_path: Path, column: str) -> float:
    if not csv_path.exists():
        return 0.0
    peak = 0.0
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            try:
                peak = max(peak, float(row[column]))
            except (KeyError, ValueError):
                continue
    return peak


def read_latency_summary(csv_path: Path) -> dict:
    """Reads a pre-computed one-row `p50_us,p95_us,p99_us,count,avg_us` summary (the format
    cMVBT/LeanStore/WiredTiger's YCSB-E scan-latency instrumentation writes directly, since
    a raw-per-op-sample CSV would blow up to tens of millions of rows at full sweep scale -
    see ycsb_driver.rs::write_results). All-zero if the file doesn't exist (e.g. a non-scan
    workload, or an older binary predating this instrumentation).
    """
    if not csv_path.exists():
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0, "avg": 0.0}
    with open(csv_path, newline="") as f:
        row = next(csv.DictReader(f), None)
    if not row:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0, "avg": 0.0}
    try:
        return {
            "p50": float(row["p50_us"]), "p95": float(row["p95_us"]), "p99": float(row["p99_us"]),
            "count": int(row["count"]), "avg": float(row["avg_us"]),
        }
    except (KeyError, ValueError):
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0, "avg": 0.0}


def write_manifest_header(manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    if not manifest_path.exists():
        with open(manifest_path, "w", newline="") as f:
            csv.writer(f).writerow(MANIFEST_HEADER)


def append_manifest_row(manifest_path: Path, result: NormalizedResult) -> None:
    with open(manifest_path, "a", newline="") as f:
        csv.writer(f).writerow([
            result.engine, result.workload, result.config_label, result.threads, result.gc_enabled,
            f"{result.duration_secs:.1f}",
            result.primary_metric_name, f"{result.primary_metric_value:.3f}",
            f"{result.peak_rss_mb:.2f}",
            f"{result.scan_p50_us:.2f}", f"{result.scan_p95_us:.2f}", f"{result.scan_p99_us:.2f}",
            result.scan_count, result.notes,
        ])


def percentiles_from_samples(csv_path: Path, column: str, filter_column: str = None, filter_value: str = None) -> dict:
    """Reads one numeric `column` (e.g. per-op latency) from every row of `csv_path` and
    returns {p50,p95,p99,count,avg} in the same units as the input column. Pure-Python
    (sorted-list nearest-rank percentiles) so every engine wrapper computes percentiles the
    same way - no numpy/pandas dependency needed just for this.

    `filter_column`/`filter_value`, if given, restrict to rows where that column equals
    that value first - e.g. cMVBT's tpcc_scan.csv mixes several OLAP modes/queries in one
    file (its `mode` column), and htap_q1/htap_q6 each need only their own query's rows.

    Returns all-zero if the file doesn't exist or has no valid rows (e.g. a workload that
    doesn't exercise scans, or an engine/version predating this instrumentation).
    """
    if not csv_path.exists():
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0, "avg": 0.0}
    values = []
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            if filter_column is not None and row.get(filter_column) != filter_value:
                continue
            try:
                values.append(float(row[column]))
            except (KeyError, ValueError):
                continue
    if not values:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0, "avg": 0.0}
    values.sort()
    n = len(values)

    def _pct(p: float) -> float:
        idx = min(n - 1, max(0, int(round(p * (n - 1)))))
        return values[idx]

    return {
        "p50": _pct(0.50), "p95": _pct(0.95), "p99": _pct(0.99),
        "count": n, "avg": sum(values) / n,
    }
