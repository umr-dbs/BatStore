"""vWeaver_ermia (ERMIA) engine wrapper - drives the `ermia_SI` binary (plain snapshot
isolation; `ermia_SI_SSN`/`ermia_SSI` also exist in this repo but aren't wired up here)
directly, mirroring the flag shape of its own benchmarks/run.sh reference script.

ERMIA ships real, standard YCSB A-F workload definitions (`--workload=<letter>` in
benchmarks/ycsb.cc, matching the textbook read/update/insert/scan/rmw ratios exactly - no
approximation needed, unlike LeanStore's separate-ratio-flags mapping) and a real
multi-table TPC-C (benchmarks/tpcc.cc).

htap_q1/htap_q6 (exact BenchBase predicates) and htap_q1_variant/htap_q6_variant
(the former engine-local predicates): upstream had no CH-benCHmark/HTAP support at all - added in
patches/vweaver_ermia_chbenchmark.patch (`RunChQ1`/`RunChQ6` in benchmarks/tpcc.cc, ported
from `bat_bench::tpch_queries::q1`/`q6` in the sibling BatStore harness - a full
`ORDER_LINE` table scan, pure aggregation, no joins, see that patch's inline comments for
why only these 2 of CH-benCHmark's 22 queries). Passing `--enable-chbenchmark` in
`-benchmark_options` spawns the requested number of dedicated threads
(`tpcc_bench_runner::StartHtapThread`) repeating only the requested query concurrently
with the normal OLTP `tpcc_worker` threads - the same
"N OLTP threads + M always-on OLAP threads" convention every other engine here uses
for htap_q1/htap_q6 - and prints one `HTAP_SCAN,<mode>,<elapsed_secs>,<scanned_tuples>,
<latency_ns>,<summary>` line per completed query to stdout (parsed by `_parse_htap_scan`
below into the same tpcc_scan.csv shape batstore.py/libmdbx.py already produce - ERMIA has no
separate result-file mechanism the way those two do, everything comes out over stdout, see
`_parse_throughput`'s own doc).

Applies identically to both CMAKE_BUILD_PARAM variants (vweaver_ermia_frugal.py builds the
exact same patched checkout, just with a different -DCMAKE_BUILD_PARAM). Developing it
surfaced a real, separate bug worth knowing about if you ever touch RunChQ1/RunChQ6 again:
buffering an unbounded (multi-million-row, ever-growing) table scan's rows into
`tpcc_table_scanner`-style per-row arena allocations silently overflows str_arena's fixed
128MB reservation in a Release build (str_arena::next's own overrun ASSERT compiles to
nothing under NDEBUG) - this manifested as a SIGSEGV/heap-corruption crash in a totally
unrelated subsystem (sm_tx_log) several seconds into a real load, not as an obvious
out-of-memory error at the actual overflow site. Fixed by aggregating directly inside the
scan callback (streaming, O(1) memory) instead of buffering first - see the patch.

Builds cleanly against https://github.com/SNU-DBXLab-papers/vWeaver_ermia's own default
("vweaver") branch with fixes for a dead `#include <sys/vtimes.h>` (removed from modern
glibc), an off-by-one benchmark start barrier that otherwise waits forever, and TPC-C
creating one unrequested extra worker, all in patches/vweaver_ermia.patch, plus generation
of dbcore/burt-hash.cpp - a gitignored
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
# dbcore/sm-oid.cpp:1049) - a genuine on/off toggle, same as BatStore's own --gc, unlike
# LeanStore/WiredTiger which have no equivalent at all.
SUPPORTS_GC_TOGGLE = True

_COMMITS_RE = re.compile(r"^([\d.]+)\s+commits/s,")
_TXN_ROW_RE = re.compile(r"^([A-Za-z_]+)\t([\d.]+)\s+commits/s")
# tpcc_bench_runner::HtapThreadMain's printf format (patches/vweaver_ermia_chbenchmark.patch):
# "HTAP_SCAN,<mode>,<elapsed_secs>,<scanned_tuples>,<latency_ns>,<summary>"
_HTAP_SCAN_RE = re.compile(
    r"^HTAP_SCAN,([a-z0-9_]+),([\d.]+),(\d+),(\d+),(-?[\d.]+)$"
)


def ensure_built() -> None:
    subprocess.run(
        ["cmake", "-S", str(REPO), "-B", str(BUILD_DIR),
         "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_BUILD_PARAM=Vweaver",
         f"-DYCSB_PAYLOAD_BYTES={os.environ.get('YCSB_PAYLOAD_BYTES', '1000')}"],
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


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "on", reload: bool = True,
        ycsb_payload: str = "standard", read_payload: bool = True) -> common.NormalizedResult:
    """`reload` accepted for interface parity with postgres_benchbase.run() but unused -
    every run here is a fresh log dir, no persisted state to reuse across sweep points."""
    del reload
    output_dir.mkdir(parents=True, exist_ok=True)
    # Fixed, wiped-before-every-run path, same disk-hygiene treatment as leanstore.py/
    # wiredtiger.py's ssd_path - ERMIA's own run.sh uses a fixed /dev/shm path for the same
    # reason (a tmpfs target for its log buffer flush); fresh_scratch_dir is tmpfs-backed
    # too (see common.SCRATCH_ROOT), so this now matches upstream's own recommendation
    # instead of just approximating it.
    log_dir = common.fresh_scratch_dir("vweaver_ermia_log")
    stdout_path = output_dir / "stdout.log"
    enable_gc = "true" if gc == "on" else "false"
    # Same "memory budget" knob LeanStore/WiredTiger get via -dram-gib, sized to
    # config::node_memory_gb (dbcore/sm-alloc.cpp) - this is a hard preallocated pool, not
    # an evictable cache, so unlike those two engines running it too small aborts the run
    # rather than just degrading throughput; bump --dram-gib if that happens. Doubled here
    # specifically: scale.dram_gib is now sized proportional to the workload's own raw
    # row-data estimate (see common.dram_gib_for), which is fine as a soft buffer-pool cap
    # for LeanStore/WiredTiger but doesn't budget for ERMIA's own internal overhead
    # (indexes, undo/version-chain buffers, ...) - undersizing THIS specific value is a
    # hard crash, not degraded throughput, so it gets extra headroom the other two don't.
    node_memory_gb = max(2, int(scale.dram_gib * 2))

    if workload in (["tpcc"] + common.HTAP_WORKLOADS):
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        # Standard TPC-C mix (NewOrder/Payment/OrderStatus/Delivery/StockLevel), no
        # warehouse-spread skew - matches benchmarks/run.sh's own plain "tpcc" default.
        # --enable-chbenchmark spawns the requested query-specific OLAP threads - see
        # this module's own doc and patches/vweaver_ermia_chbenchmark.patch.
        benchmark_options = "--workload-mix=45,43,0,4,4,4,0,0 --warehouse-spread=0"
        if workload in common.HTAP_WORKLOADS:
            query_flag = "--chbenchmark-q1" if "q1" in workload else "--chbenchmark-q6"
            variant_flag = " --chbenchmark-variant" if workload.endswith("_variant") else ""
            benchmark_options += (
                f" --enable-chbenchmark {query_flag}{variant_flag}"
                f" --chbenchmark-threads={scale.htap_olap_threads}"
            )
        args = [
            str(BINARY), "-verbose", "-benchmark", "tpcc",
            "-threads", str(threads), "-scale_factor", str(scale.tpcc_warehouses),
            "-seconds", str(duration), f"-enable_gc={enable_gc}",
            f"-node_memory_gb={node_memory_gb}",
            "-log_data_dir", str(log_dir), "-log_buffer_mb=128", "-log_segment_mb=131072",
            "-parallel_loading",
            "-benchmark_options", benchmark_options,
        ]
        metric_name = "new_order_per_sec"
    else:
        letter = workload.split("_", 1)[1].upper()
        if letter not in ("C", "F"):
            raise RuntimeError(
                f"vWeaver/ERMIA YCSB-{letter} is disabled: upstream TxnInsert/TxnUpdate/TxnScan "
                "handlers are stubs; only payload-reading YCSB-C and RMW YCSB-F are implemented"
            )
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
            f"--zipfian --zipfian-theta={scale.ycsb_theta} " + ("" if read_payload else "--key-only"),
        ]
        metric_name = "ops_per_sec"

    # ermia_SI dynamically links libermia_si.so from the same build dir (see
    # CMakeLists.txt's add_library(ermia_si SHARED ...)) with no install step and no
    # baked-in install-RPATH - it only works unmodified via CMake's build-tree RPATH,
    # which breaks the moment the build directory is moved (e.g. a renamed WORKSPACE_ROOT
    # between sessions), producing "error while loading shared libraries: libermia_si.so:
    # cannot open shared object file". Setting LD_LIBRARY_PATH explicitly sidesteps RPATH
    # entirely so this can't recur.
    run_env = os.environ.copy()
    run_env["LD_LIBRARY_PATH"] = str(BUILD_DIR) + os.pathsep + run_env.get("LD_LIBRARY_PATH", "")
    timeout = common.default_subprocess_timeout(duration)
    returncode, peak_rss_mb = common.run_and_track_rss(
        args, cwd=REPO, env=run_env, stdout_path=stdout_path, timeout=timeout,
    )
    if returncode != 0:
        notes = f"TIMEOUT after {timeout:.0f}s, see stdout.log" if returncode is None else \
            f"FAILED exit={returncode}, see stdout.log"
        return common.NormalizedResult(
            "vweaver_ermia", workload, scale.label, duration, metric_name, 0.0, peak_rss_mb,
            threads=threads, gc_enabled=gc,
            notes=notes,
        )

    value = _parse_throughput(stdout_path, workload)
    notes = "" if value else "throughput line not found in stdout.log"

    latency = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "count": 0}
    if workload in common.HTAP_WORKLOADS:
        mode = {
            "htap_q1": "ch_q1_pricing_summary",
            "htap_q6": "ch_q6_forecast_revenue",
            "htap_q1_variant": "ch_q1_variant",
            "htap_q6_variant": "ch_q6_variant",
        }[workload]
        scan_csv = _write_htap_scan_csv(stdout_path, output_dir)
        latency = common.percentiles_from_samples(
            scan_csv, "latency_ns", filter_column="mode", filter_value=mode,
        )
        for k in ("p50", "p95", "p99", "avg"):
            latency[k] /= 1000.0  # ns -> us

    return common.NormalizedResult(
        "vweaver_ermia", workload, scale.label, duration, metric_name, value, peak_rss_mb,
        threads=threads, gc_enabled=gc,
        scan_p50_us=latency["p50"], scan_p95_us=latency["p95"], scan_p99_us=latency["p99"],
        scan_avg_us=latency["avg"], scan_count=latency["count"], notes=notes,
    )


def _write_htap_scan_csv(stdout_path: Path, output_dir: Path) -> Path:
    """Extracts every `HTAP_SCAN,...` line `_HTAP_SCAN_RE` matches in `stdout_path` (see
    tpcc_bench_runner::HtapThreadMain, patches/vweaver_ermia_chbenchmark.patch) and writes
    them into the same tpcc_scan.csv column shape batstore.py/libmdbx.py's own tpcc_scan.csv
    already use, so common.percentiles_from_samples reads all three identically.
    `snapshot`/`staleness_versions`/`delay_secs` are always blank/0 here - ERMIA's
    transaction id isn't threaded through the stdout line (unlike libmdbx's
    MdbxScanResult), and neither field is actually consumed downstream.
    """
    scan_csv = output_dir / "tpcc_scan.csv"
    rows = []
    if stdout_path.exists():
        for line in stdout_path.read_text(errors="replace").splitlines():
            m = _HTAP_SCAN_RE.match(line)
            if not m:
                continue
            mode, elapsed_secs, scanned_tuples, latency_ns, summary = m.groups()
            scanned_tuples, latency_ns = int(scanned_tuples), int(latency_ns)
            tuples_per_sec = scanned_tuples / (latency_ns / 1e9) if latency_ns else 0.0
            rows.append(
                f"{mode},{elapsed_secs},0,,{scanned_tuples},{latency_ns},"
                f"{tuples_per_sec:.2f},{summary},"
            )
    with open(scan_csv, "w") as f:
        f.write("mode,elapsed_secs,delay_secs,snapshot,scanned_tuples,latency_ns,"
                "tuples_per_sec,summary,staleness_versions\n")
        f.write("\n".join(rows) + ("\n" if rows else ""))
    return scan_csv


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
    if workload in (["tpcc"] + common.HTAP_WORKLOADS):
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
