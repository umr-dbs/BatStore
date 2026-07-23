"""vWeaver_ermia (ERMIA) engine wrapper - drives the `ermia_SI` binary (plain snapshot
isolation; `ermia_SI_SSN`/`ermia_SSI` also exist in this repo but aren't wired up here)
directly, mirroring the flag shape of its own benchmarks/run.sh reference script.

ERMIA ships real, standard YCSB A-F workload definitions (`--workload=<letter>` in
benchmarks/ycsb.cc, matching the textbook read/update/insert/scan/rmw ratios exactly - no
approximation needed, unlike LeanStore's separate-ratio-flags mapping) and a real
multi-table TPC-C (benchmarks/tpcc.cc). No CH-benCHmark/HTAP support in either - htap_q1/
htap_q6 simply skip this engine.

Builds cleanly against https://github.com/SNU-DBXLab-papers/vWeaver_ermia's own default
("vweaver") branch with two fixes: a dead `#include <sys/vtimes.h>` (removed from modern
glibc, see patches/vweaver_ermia.patch), and generating dbcore/burt-hash.cpp - a gitignored
build artifact upstream normally produces via `python2 dbcore/burt-hash.py`, but no
python2 exists here, so setup_environment.py runs a Python 3 port instead (see
patches/vweaver_burt_hash_gen.py). An EARLIER version of this pin/patch was generated
against the wrong upstream (a divergent fork, Rudeus/vWeaver_ermia) by mistake and
described a masstree scan() overload bug that only exists in that fork - it doesn't apply
here and has been removed.

The one remaining thing to know operationally: ermia_SI preallocates its entire working
set up front via `mmap(..., MAP_HUGETLB)` (dbcore/sm-alloc.cpp::prepare_node_memory, sized
by `-node_memory_gb`) and crashes immediately (uncaught os_error) if the kernel has no
hugepages reserved - setup_environment.py's step_vweaver_hugepages() reserves them.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

from . import common

REPO = common.VWEAVER_REPO
BUILD_DIR = REPO / "build"
BINARY = BUILD_DIR / "ermia_SI"

# -enable_gc gates real version-chain reclamation (dbcore/sm-alloc.cpp:441,
# dbcore/sm-oid.cpp:1049) - a genuine on/off toggle, same as cMVBT's own --gc, unlike
# LeanStore/WiredTiger which have no equivalent at all.
SUPPORTS_GC_TOGGLE = True

_COMMITS_RE = re.compile(r"^([\d.]+)\s+commits/s,")
_TXN_ROW_RE = re.compile(r"^([A-Za-z_]+)\t([\d.]+)\s+commits/s")


def ensure_built() -> None:
    if BINARY.exists():
        return
    subprocess.run(
        ["cmake", "-S", str(REPO), "-B", str(BUILD_DIR),
         "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_BUILD_PARAM=Vweaver"],
        cwd=REPO, env=_clang_env(), check=True,
    )
    subprocess.run(
        ["cmake", "--build", str(BUILD_DIR), "--target", "ermia_SI",
         "--parallel", str(os.cpu_count() or 4)],
        check=True,
    )


def _clang_env() -> dict:
    env = os.environ.copy()
    env["CC"] = shutil.which("clang") or "clang"
    env["CXX"] = shutil.which("clang++") or "clang++"
    # Mirrors setup_environment.py's _clang_env(): clang can pick a GCC toolchain dir for
    # C++ header search that doesn't match whichever GCC version's libstdc++-dev is
    # actually installed - pinned explicitly to GCC 13's paths.
    gcc_cpp_version = "13"
    env["CXXFLAGS"] = (
        f"-I/usr/include/c++/{gcc_cpp_version} -I/usr/include/x86_64-linux-gnu/c++/{gcc_cpp_version} "
        + env.get("CXXFLAGS", "")
    )
    env["LDFLAGS"] = f"-L/usr/lib/gcc/x86_64-linux-gnu/{gcc_cpp_version} " + env.get("LDFLAGS", "")
    return env


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "on", reload: bool = True) -> common.NormalizedResult:
    """`reload` accepted for interface parity with postgres_benchbase.run() but unused -
    every run here is a fresh log dir, no persisted state to reuse across sweep points."""
    del reload
    output_dir.mkdir(parents=True, exist_ok=True)
    # Fixed, wiped-before-every-run path, same disk-hygiene treatment as leanstore.py/
    # wiredtiger.py's ssd_path - ERMIA's own run.sh uses a fixed /dev/shm path for the same
    # reason (a tmpfs target for its log buffer flush); this harness keeps everything
    # under one consistent WORKSPACE_ROOT-relative scratch tree instead.
    log_dir = common.fresh_scratch_dir("vweaver_ermia_log")
    stdout_path = output_dir / "stdout.log"
    enable_gc = "true" if gc == "on" else "false"
    # Same "memory budget" knob LeanStore/WiredTiger get via -dram-gib, sized to
    # config::node_memory_gb (dbcore/sm-alloc.cpp) - this is a hard preallocated pool, not
    # an evictable cache, so unlike those two engines running it too small aborts the run
    # rather than just degrading throughput; bump --dram-gib if that happens.
    node_memory_gb = max(1, int(scale.dram_gib))

    if workload == "tpcc":
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        args = [
            str(BINARY), "-verbose", "-benchmark", "tpcc",
            "-threads", str(threads), "-scale_factor", str(scale.tpcc_warehouses),
            "-seconds", str(duration), f"-enable_gc={enable_gc}",
            f"-node_memory_gb={node_memory_gb}",
            "-log_data_dir", str(log_dir), "-log_buffer_mb=128", "-log_segment_mb=131072",
            "-parallel_loading",
            # Standard TPC-C mix (NewOrder/Payment/OrderStatus/Delivery/StockLevel), no
            # warehouse-spread skew - matches benchmarks/run.sh's own plain "tpcc" default.
            "-benchmark_options", "--workload-mix=45,43,0,4,4,4,0,0 --warehouse-spread=0",
        ]
        metric_name = "new_order_per_sec"
    else:
        letter = workload.split("_", 1)[1].upper()
        duration = scale.ycsb_duration
        threads = scale.ycsb_threads
        args = [
            str(BINARY), "-verbose", "-benchmark", "ycsb",
            "-threads", str(threads), "-scale_factor", "1", "-seconds", str(duration),
            f"-enable_gc={enable_gc}", f"-node_memory_gb={node_memory_gb}",
            "-log_data_dir", str(log_dir), "-log_buffer_mb=128", "-log_segment_mb=131072",
            "-parallel_loading",
            "-benchmark_options",
            f"--workload={letter} --initial-table-size={scale.ycsb_records} "
            f"--zipfian --zipfian-theta={scale.ycsb_theta}",
        ]
        metric_name = "ops_per_sec"

    returncode, peak_rss_mb = common.run_and_track_rss(
        args, cwd=REPO, stdout_path=stdout_path,
    )
    if returncode != 0:
        return common.NormalizedResult(
            "vweaver_ermia", workload, scale.label, duration, metric_name, 0.0, peak_rss_mb,
            threads=threads, gc_enabled=gc,
            notes=f"FAILED exit={returncode}, see stdout.log",
        )

    value = _parse_throughput(stdout_path, workload)
    notes = "" if value else "throughput line not found in stdout.log"
    return common.NormalizedResult(
        "vweaver_ermia", workload, scale.label, duration, metric_name, value, peak_rss_mb,
        threads=threads, gc_enabled=gc, notes=notes,
    )


def _parse_throughput(stdout_path: Path, workload: str) -> float:
    """benchmarks/bench.cc's summary print (see std::cout << agg_throughput << " commits/s"
    and the per-txn-type breakdown loop right after it): the aggregate line covers every
    txn type combined (used as-is for YCSB's ops_per_sec - every op there is one txn), but
    TPC-C's metric is New-Order-only like every other engine's new_order_per_sec, so pull
    that one row out of the per-txn breakdown instead of the aggregate.
    """
    if not stdout_path.exists():
        return 0.0
    text = stdout_path.read_text(errors="replace")
    if workload == "tpcc":
        for line in text.splitlines():
            m = _TXN_ROW_RE.match(line)
            if m and m.group(1) == "NewOrder":
                return float(m.group(2))
        return 0.0
    for line in text.splitlines():
        m = _COMMITS_RE.match(line)
        if m:
            return float(m.group(1))
    return 0.0
