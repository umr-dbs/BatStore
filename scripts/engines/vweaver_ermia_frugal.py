"""vweaver_ermia_frugal engine wrapper - drives the SAME upstream (SNU-DBXLab-papers/
vWeaver_ermia) checkout as engines/vweaver_ermia.py, but built with
`-DCMAKE_BUILD_PARAM=Eval_skiplist` (CMakeLists.txt: `-DHYU_SKIPLIST -O3`, no
`-DHYU_VWEAVER`) into its own `build_frugal/` directory - the plain frugal-list version
chain this fork's own CMakeLists.txt offers as an alternative to vWeaver's "weaving"
compaction, with neither enabled.

This variant exists because vweaver_ermia.py's own "Vweaver" build was reported not
working. Root-caused here to two independent things:

1. A runtime `os_error` thrown out of dbcore/sm-alloc.cpp::prepare_node_memory() -
   verified to be IDENTICAL for every CMAKE_BUILD_PARAM (that function doesn't branch on
   HYU_VWEAVER/HYU_SKIPLIST at all), i.e. plain insufficient hugepages
   (setup_environment.py's step_vweaver_hugepages()), not a vWeaver-specific bug.

2. A genuine, separate upstream defect that DOES block this specific ("just frugal
   lists", no vWeaver) variant: `-DHYU_SKIPLIST` alone doesn't even link.
   `MM::deallocate_skiplist()` (dbcore/sm-alloc.h) is declared and called (dbcore/
   sm-oid.cpp, sm-oid.h) but was never defined anywhere upstream, and the dedicated
   `#elif defined(HYU_SKIPLIST)` branch of `MM::gc_version_chain()` (dbcore/sm-alloc.cpp)
   that would recycle a version's Lv-pointer array on reclaim was present in the source
   but commented out, silently falling through to the plain-masstree `#else` branch
   instead (a leak of every reclaimed version's Lv-pointer array, not a crash).
   patches/vweaver_ermia_frugal.patch fixes both, using the vendor's own (disabled)
   reference code for the GC branch - see that patch file's inline comments. It's a no-op
   for the "Vweaver" build (guarded entirely by #ifdef HYU_SKIPLIST, which that build
   doesn't define), so it's applied unconditionally onto the one shared checkout
   regardless of which variant(s) get built from it.

Same YCSB A-F / TPC-C support (or lack of CH-benCHmark/HTAP support) as
vweaver_ermia.py - see that module's docstring; this one only duplicates what differs
(build dir, binary, CMAKE_BUILD_PARAM, engine name).
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from . import common
from .vweaver_ermia import _clang_env, _parse_throughput

REPO = common.VWEAVER_REPO
BUILD_DIR = REPO / "build_frugal"
BINARY = BUILD_DIR / "ermia_SI"

# HYU_SKIPLIST has no -enable_gc-shaped toggle of its own beyond the same generic
# -enable_gc flag every build here shares (dbcore/sm-alloc.cpp::prepare_node_memory is
# unconditional) - same as vweaver_ermia.py.
SUPPORTS_GC_TOGGLE = True


def ensure_built() -> None:
    if BINARY.exists():
        return
    subprocess.run(
        ["cmake", "-S", str(REPO), "-B", str(BUILD_DIR),
         "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_BUILD_PARAM=Eval_skiplist"],
        cwd=REPO, env=_clang_env(), check=True,
    )
    subprocess.run(
        ["cmake", "--build", str(BUILD_DIR), "--target", "ermia_SI",
         "--parallel", str(os.cpu_count() or 4)],
        check=True,
    )


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "on", reload: bool = True) -> common.NormalizedResult:
    """See vweaver_ermia.run() - identical shape, `reload` unused for the same reason."""
    del reload
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir = common.fresh_scratch_dir("vweaver_ermia_frugal_log")
    stdout_path = output_dir / "stdout.log"
    enable_gc = "true" if gc == "on" else "false"
    node_memory_gb = max(2, int(scale.dram_gib * 2))

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

    run_env = os.environ.copy()
    run_env["LD_LIBRARY_PATH"] = str(BUILD_DIR) + os.pathsep + run_env.get("LD_LIBRARY_PATH", "")
    returncode, peak_rss_mb = common.run_and_track_rss(
        args, cwd=REPO, env=run_env, stdout_path=stdout_path,
    )
    if returncode != 0:
        return common.NormalizedResult(
            "vweaver_ermia_frugal", workload, scale.label, duration, metric_name, 0.0, peak_rss_mb,
            threads=threads, gc_enabled=gc,
            notes=f"FAILED exit={returncode}, see stdout.log",
        )

    value = _parse_throughput(stdout_path, workload)
    notes = "" if value else "throughput line not found in stdout.log"
    return common.NormalizedResult(
        "vweaver_ermia_frugal", workload, scale.label, duration, metric_name, value, peak_rss_mb,
        threads=threads, gc_enabled=gc, notes=notes,
    )
