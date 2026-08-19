"""Shared types and helpers for the cross-engine benchmark harness.

Normalizes BatStore/LeanStore/WiredTiger/PostgreSQL onto one comparable schema (see
MANIFEST_HEADER below) so a single manifest.csv + plotting script can overlay all four.
See manual.txt (repo root) for the full usage guide.
"""
from __future__ import annotations

import csv
import dataclasses
import os
import shutil
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Optional

# Every sibling-repo checkout this harness drives lives under one workspace, rooted at
# wherever setup_environment.py was invoked FROM - not a hardcoded absolute path - so the
# exact same scripts clone/build/run into place unchanged on any machine (this workstation
# or the real server), starting from nothing. Override via WORKSPACE_ROOT if you want the
# checkouts somewhere other than <invocation-dir>/tx_tests.
WORKSPACE_ROOT = Path(os.environ.get("WORKSPACE_ROOT", str(Path.cwd() / "tx_tests")))

# BATSTORE_REPO: setup_environment.py always attempts to clone BatStore into
# WORKSPACE_ROOT/batstore (see its step_batstore). If that succeeded (Cargo.toml present -
# distinguishes a real clone from a stray empty directory), use it; otherwise fall back to
# this file's own grandparent directory - the checkout this script is already part of,
# self-referential so it remains correct whenever the remote clone is unavailable or this
# checkout should be used directly.
_workspace_batstore = WORKSPACE_ROOT / "batstore"
_self_referential_batstore = Path(__file__).resolve().parent.parent.parent
BATSTORE_REPO = Path(os.environ.get(
    "BATSTORE_REPO",
    os.environ.get(
        "CMVBT_REPO",
        str(_workspace_batstore) if (_workspace_batstore / "Cargo.toml").exists() else str(_self_referential_batstore),
    ),
))
LEANSTORE_REPO = Path(os.environ.get("LEANSTORE_REPO", str(WORKSPACE_ROOT / "leanstore")))
# Directory name is a misleading holdover from an old CLion default - it's actually
# configured with -DCMAKE_BUILD_TYPE=Release (see setup_environment.py::step_wiredtiger and
# leanstore_build.py's build-type sanity check).
WIREDTIGER_BUILD_DIR = Path(os.environ.get("WIREDTIGER_BUILD_DIR", str(WORKSPACE_ROOT / "wiredtiger" / "cmake-build-debug")))
BENCHBASE_HOME = Path(os.environ.get("BENCHBASE_HOME", str(WORKSPACE_ROOT / "benchbase" / "target" / "benchbase-postgres")))
VWEAVER_REPO = Path(os.environ.get("VWEAVER_REPO", str(WORKSPACE_ROOT / "vWeaver_ermia")))

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

# Every engine's on-disk DATA directory (LeanStore/WiredTiger's ssd image, libmdbx's
# environment, ERMIA's log dir, BatStore's WAL file - see fresh_scratch_dir below) is created
# under here, not under WORKSPACE_ROOT/scratch on real disk - this harness is
# in-memory-only: every one of those engines still does real file I/O (page eviction, WAL
# flush, mmap writeback), and the only way to guarantee none of it ever reaches a physical
# disk, regardless of how --dram-gib/buffer-pool sizing is set, is to back that I/O with
# tmpfs (RAM) instead of a real filesystem. /dev/shm is tmpfs on every mainstream Linux
# distro by default.
SCRATCH_ROOT = Path(os.environ.get("SCRATCH_ROOT", "/dev/shm/batstore_bench_scratch"))

# Global allocator BatStore's own binary is built with - see Cargo.toml's `mimalloc`
# feature and src/main.rs's global-allocator cfg (jemalloc is the crate's own default;
# mimalloc measured a few % *worse* on TPC-C despite winning a few % on YCSB's WAL path,
# so it stays opt-in rather than becoming the default there too). Read fresh from the
# environment on every call (not cached at import time) so compare_engines.py's
# --batstore-allocator flag can set it right before either batstore.py's or libmdbx.py's
# ensure_built() runs - both build the exact same binary (see their own module docs),
# so this one helper is the single place that decides the cargo invocation for either.
def batstore_cargo_build_args(*extra_features: str) -> list[str]:
    allocator = os.environ.get("BATSTORE_ALLOCATOR", os.environ.get("CMVBT_ALLOCATOR", "jemalloc"))
    args = ["cargo", "build", "--release"]
    if allocator == "mimalloc":
        args.append("--features")
        args.append(",".join(("mimalloc",) + extra_features))
    elif allocator == "jemalloc":
        if extra_features:
            args += ["--features", ",".join(extra_features)]
    else:
        raise ValueError(f"unknown BATSTORE_ALLOCATOR={allocator!r} (expected 'jemalloc' or 'mimalloc')")
    return args


def numactl_prefix() -> list:
    return ["numactl", f"--cpubind={NUMA_NODE}", f"--membind={NUMA_NODE}"]


def numa_node_cpu_list(node: int = NUMA_NODE) -> str:
    """Kernel CPU-list syntax for a NUMA node, suitable for cgroup AllowedCPUs."""
    path = Path(f"/sys/devices/system/node/node{node}/cpulist")
    if not path.exists():
        raise RuntimeError(f"cannot pin to NUMA node {node}: {path} does not exist")
    cpus = path.read_text().strip()
    if not cpus:
        raise RuntimeError(f"cannot pin to NUMA node {node}: {path} is empty")
    return cpus


def default_subprocess_timeout(duration: float) -> float:
    """A generous `run_and_track_rss(..., timeout=...)` bound for a run whose measured
    phase is `duration` seconds: `4x duration + 300s` headroom for population/load,
    connection setup, and warm-up, none of which count against `duration` itself but
    have been observed to stall for minutes under host contention (this is a shared
    server, not dedicated hardware - see NUMA_NODE's docstring for the isolation
    assumption this only partially holds). Bounded rather than unlimited so one stuck
    engine fails that one (workload, threads) point instead of hanging the entire
    sweep - see run_and_track_rss's own docstring for what "stuck" looked like."""
    return duration * 4 + 300


def _mount_fstype(path: Path) -> str:
    """The fstype of the mount `path` actually lives under - the longest /proc/mounts
    mountpoint that's a prefix of `path` (path itself need not exist yet).

    Resolve symlinks first: PostgreSQL's tmpfs setup intentionally leaves its configured
    data_directory path in /var/lib/postgresql and makes that path a symlink into /dev/shm.
    Checking the lexical /var/lib path incorrectly reports the filesystem containing the
    symlink (ext4), rather than the filesystem containing PostgreSQL's actual data (tmpfs).
    strict=False still supports fresh_scratch_dir's not-yet-created child paths.
    """
    path_str = str(path.resolve(strict=False))
    best_mnt, best_fstype = "", ""
    with open("/proc/mounts") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 3:
                continue
            mnt, fstype = parts[1], parts[2]
            if (path_str == mnt or path_str.startswith(mnt.rstrip("/") + "/")) and len(mnt) > len(best_mnt):
                best_mnt, best_fstype = mnt, fstype
    return best_fstype


def fresh_scratch_dir(name: str) -> Path:
    """Returns SCRATCH_ROOT/<name>, entirely deleted and recreated first - the SAME path
    every call, for every engine's on-disk data (ssd images, mdbx/DB directories, WAL/log
    dirs). Called at the start of every single run(), so disk usage never accumulates
    across a long thread/workload sweep the way a fresh uniquely-named directory per sweep
    point would - only the current experiment's data ever exists at all, never every prior
    one's too.

    Fails loudly (sys.exit) if SCRATCH_ROOT isn't tmpfs-backed, rather than silently
    running against real disk - same reasoning as numactl_prefix()'s unconditional NUMA
    pinning above: a silent fallback here would quietly turn an "in-memory-only" run into a
    disk-bound one with no indication in the results.

    Deliberately separate from a run's own `output_dir` (under the timestamped
    comparison_results/run_<ts>/ tree, on real disk) - that keeps holding the small
    per-run artifacts (stdout.log, result CSVs) worth preserving for post-hoc inspection;
    only the heavy data files live here.
    """
    fstype = _mount_fstype(SCRATCH_ROOT)
    if fstype != "tmpfs":
        sys.exit(
            f"SCRATCH_ROOT={SCRATCH_ROOT} is not tmpfs-backed (mount fstype={fstype!r}) - refusing to "
            f"run, since every engine's on-disk data directory must be RAM-backed for this harness's "
            f"in-memory-only benchmarking (see manual.txt section 4). Either unset SCRATCH_ROOT to use "
            f"the /dev/shm default, or point it at a tmpfs mount yourself, e.g. "
            f"`sudo mount -t tmpfs -o size=64G tmpfs {SCRATCH_ROOT}` (mkdir it first)."
        )
    scratch_dir = SCRATCH_ROOT / name
    # Never ignore cleanup failures: reusing a partially stale database directory would
    # contaminate the next measurement, and a root-owned directory should produce an
    # actionable ownership error instead of failing later inside an engine.
    if scratch_dir.exists():
        try:
            shutil.rmtree(scratch_dir)
        except PermissionError as exc:
            raise PermissionError(
                f"cannot reset benchmark scratch directory {scratch_dir}; repair ownership with "
                f"`sudo chown -R $(id -u):$(id -g) {scratch_dir}`"
            ) from exc
    scratch_dir.mkdir(parents=True, exist_ok=True)
    return scratch_dir


def _node_mem_total_gib(node: int) -> float:
    """Total memory (GiB) local to NUMA node `node`, from
    /sys/devices/system/node/node<N>/meminfo's "Node N MemTotal:" line - present on any
    Linux kernel, single-node machines included (they still expose node0 with the whole
    system's memory). Falls back to /proc/meminfo's system-wide MemTotal if the per-node
    file is missing for some reason (e.g. a kernel without NUMA support compiled in)."""
    node_meminfo = Path(f"/sys/devices/system/node/node{node}/meminfo")
    if node_meminfo.exists():
        prefix, source = f"Node {node} MemTotal:", node_meminfo
    else:
        prefix, source = "MemTotal:", Path("/proc/meminfo")
    if not source.exists():
        return 0.0
    for line in source.read_text().splitlines():
        if line.strip().startswith(prefix):
            return float(line.split()[-2]) / (1024 * 1024)  # kB -> GiB
    return 0.0


def total_system_mem_gib() -> float:
    """Whole-machine memory (GiB), from /proc/meminfo's system-wide MemTotal - unlike
    _node_mem_total_gib, always the FULL total across every NUMA node combined (hugepage
    reservations via `sysctl vm.nr_hugepages` aren't confined to one node the way
    --membind pins the actual benchmark subprocesses - see setup_environment.py's
    step_vweaver_hugepages, the one caller that needs the whole-machine figure)."""
    path = Path("/proc/meminfo")
    if not path.exists():
        return 0.0
    for line in path.read_text().splitlines():
        if line.strip().startswith("MemTotal:"):
            return float(line.split()[-2]) / (1024 * 1024)  # kB -> GiB
    return 0.0


def default_dram_gib(headroom_gib: float = 16.0, min_gib: float = 2.0) -> float:
    """A --dram-gib default that scales with the ACTUAL machine this runs on (32GB
    workstation or the ~500GB/2-NUMA-node real server) instead of a number hardcoded for
    one of them - reads NUMA_NODE's own local memory (not total system memory: every
    subprocess here is `--membind=NUMA_NODE`-pinned, so on the 2-socket server only that
    one node's local share, roughly half the machine's total, is ever actually available
    to it; sizing off system-wide total there would let LeanStore/WiredTiger request more
    than membind permits).

    Halves what's left after `headroom_gib` (OS, JVM/BenchBase, other non-pinned
    processes) because fresh_scratch_dir's tmpfs backing means the SAME data is resident in
    RAM twice at once at steady state: once as the buffer pool's own cached pages, once
    again as the "disk" file underneath it (also tmpfs, i.e. RAM) - a dram_gib sized off
    the full node total would double-book memory the tmpfs copy already claimed.
    """
    node_total = _node_mem_total_gib(NUMA_NODE)
    if node_total <= 0:
        return min_gib  # couldn't read either meminfo source - stay conservative, don't guess
    return max(min_gib, round((node_total - headroom_gib) / 2, 1))


def workload_dataset_gib(workload: str, scale: "Scale") -> float:
    """Rough, order-of-magnitude estimate (GiB) of a workload's own raw data size - the
    standard, widely-cited rules of thumb for these two benchmarks (~100MB/warehouse for
    TPC-C at this schema's table cardinalities; ~1KB/record for YCSB's default field_count
    x field_length), NOT a byte-exact accounting of any one engine's actual on-disk
    encoding (indexes, MVCC-version chains, page-header waste all vary by engine and
    aren't represented here - this is a cross-engine-comparable proxy, not a measurement).
    Good enough to size a buffer pool proportional to what a workload actually needs
    instead of one fixed value applied to every scale regardless of how small the loaded
    data actually is - see dram_gib_for.
    """
    if workload in (["tpcc"] + HTAP_WORKLOADS):
        return scale.tpcc_warehouses * 0.1
    if workload == "s_htap":
        return scale.s_htap_record_count * 1024 / (1024 ** 3)
    return scale.ycsb_records * 1024 / (1024 ** 3)


def dram_gib_for(workload: str, scale: "Scale", safety_factor: float = 4.0) -> float:
    """The --dram-gib value to actually use for a SPECIFIC (workload, scale) combination -
    proportional to that workload's own estimated dataset (workload_dataset_gib x
    safety_factor, comfortably covering it so LeanStore/WiredTiger never evict) rather
    than default_dram_gib's flat, machine-wide ceiling applied identically regardless of
    how small the loaded data actually is. Still capped at default_dram_gib() so a large
    --warehouses/--ycsb-records override can't request more than this machine can safely
    give a single --membind-pinned process.

    This is what keeps peak_rss_mb representative of the engine's genuine working-set
    memory use rather than dominated by a buffer pool sized off total machine memory: each
    buffer frame's own bookkeeping (lock, page id, dirty flag, ...) is real memory
    proportional to dram_gib itself in many LeanStore-family engines, paid upfront
    independent of how much data actually ends up loaded - a dram_gib picked to fit the
    workload keeps that overhead proportional too, instead of ballooning it to whatever a
    500GB server happens to have free.
    """
    return min(default_dram_gib(), max(2.0, round(workload_dataset_gib(workload, scale) * safety_factor, 1)))


def check_release_build(build_dir: Path, label: str) -> None:
    """Fails loudly if a CMake build directory isn't configured as
    CMAKE_BUILD_TYPE=Release - every engine here must be release/optimized, never a stray
    Debug/RelWithDebInfo leftover from an IDE-generated build dir. This has actually
    happened: the tx_tests wiredtiger and leanstore build dirs were both found configured
    against the wrong source tree (a leftover CLionProjects-relative CMakeCache), one of
    them in plain Debug, before this check existed.
    """
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
    """The default (non-TINY_SCALE) data volume/duration - used unchanged on both this
    24-core/32GB workstation and the real 64-core/512GB-ish 2-NUMA-node server (see
    compare_engines.py's module docstring: the default thread sweep is sized for that
    server). Only `dram_gib` actually adapts to which machine it's running on (see
    default_dram_gib) - every other field here is machine-independent data volume, tune it
    yourself via --warehouses/--ycsb-records/etc. if 8 warehouses / 2M YCSB records isn't
    the scale you want on the bigger box."""

    tpcc_warehouses: int = 8
    tpcc_terminals: int = 16
    tpcc_duration: int = 60
    ycsb_records: int = 2_000_000
    ycsb_threads: int = 16
    ycsb_duration: int = 30
    ycsb_theta: float = 0.99
    # htap_q1/htap_q6 (see HTAP_WORKLOADS): number of dedicated analytical (OLAP) threads
    # repeating the workload's query concurrently with the OLTP terminals swept via
    # tpcc_terminals - the x-axis for an "HTAP scaling" plot (throughput vs. number of
    # analytical threads, TPC-C's own OLTP side held fixed). Defaults to 1, matching every
    # engine wrapper's previous hardcoded behavior.
    htap_olap_threads: int = 1
    # "S-HTAP" streaming workload (src/bat_bench/s_htap_driver.rs): near-sorted
    # arrivals + recency-biased hot-tail updates running concurrently with OLAP scans
    # that straddle the cold/hot boundary - see that module's doc. `s_htap_record_count`
    # is the cold historical corpus loaded up front (mirrors ycsb_records); the swept
    # --threads value (scale.ycsb_threads, set for every workload - see
    # compare_engines.py's scale_variant construction) is split into
    # `s_htap_olap_threads` dedicated OLAP scanners plus the remainder as write
    # threads, not an additional independent knob here. A longer default duration than
    # plain YCSB: this workload's whole point is slow analytical scans, which need more
    # wall-clock time than a point-op mix to produce a meaningful latency distribution.
    s_htap_record_count: int = 2_000_000
    s_htap_duration: int = 60
    s_htap_hot_window: int = 10_000
    s_htap_theta: float = 0.99
    s_htap_arrival_ratio: float = 0.2
    s_htap_max_lateness: int = 50
    s_htap_olap_threads: int = 2
    s_htap_olap_lag: int = 0
    s_htap_olap_span: int = 30_000
    # LeanStore/WiredTiger buffer pool / cache size; unused by batstore and postgres (batstore has
    # no comparable cap and runs fully in-memory already - see batstore.py's wal_enabled
    # default; postgres's shared_buffers is configured on the server directly, outside this
    # harness). Computed from the ACTUAL machine's own NUMA-node-local memory (see
    # default_dram_gib) rather than a number hardcoded for one specific box - this Scale is
    # used unchanged on both the 32GB workstation and the real ~500GB/2-NUMA-node server
    # (compare_engines.py's own module docstring - the default thread sweep is sized for
    # that server), and those two need very different buffer-pool sizes to both (a) never
    # evict under the default warehouse/records sweep and (b) not overcommit memory once
    # fresh_scratch_dir's tmpfs backing is accounted for (see that function's docstring -
    # every page lives in RAM twice at once: once as tmpfs "disk", once as buffer-pool
    # cache). Bump via --dram-gib if you raise --warehouses/--ycsb-records enough to
    # outgrow whatever this computes on your machine (printed in compare_engines.py's
    # startup banner).
    dram_gib: float = dataclasses.field(default_factory=default_dram_gib)
    label: str = "workstation"


TINY_SCALE = Scale(
    tpcc_warehouses=1, tpcc_terminals=2, tpcc_duration=10,
    ycsb_records=10_000, ycsb_threads=2, ycsb_duration=10,
    dram_gib=2.0, label="tiny",
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
    # engines) or workload == "tpcc" and engine == "batstore" (its existing HTAP scan-sweep
    # mode) - 0 elsewhere.
    scan_p50_us: float = 0.0
    scan_p95_us: float = 0.0
    scan_p99_us: float = 0.0
    scan_avg_us: float = 0.0
    scan_count: int = 0
    notes: str = ""


MANIFEST_HEADER = [
    "engine", "workload", "config_label", "threads", "gc_enabled", "duration_secs",
    "primary_metric_name", "primary_metric_value", "peak_rss_mb",
    "scan_p50_us", "scan_p95_us", "scan_p99_us", "scan_avg_us", "scan_count", "notes",
]

YCSB_WORKLOADS = ["ycsb_a", "ycsb_b", "ycsb_c", "ycsb_d", "ycsb_e", "ycsb_f"]
# HTAP/CH-benCHmark (TPC-C OLTP running concurrently with one CH-benCHmark/TPC-H-style
# analytical query): restricted to Q1 ("Pricing Summary Report") and Q6 ("Forecasting
# Revenue Change") - the only 2 of CH-benCHmark's 22 queries genuinely implemented across
# every engine that supports this at all: BatStore and LeanStore/WiredTiger's hand-written
# scans, BenchBase's real SQL for PostgreSQL, and libmdbx's own hand-written scans
# (src/bat_bench/mdbx_tpcc.rs::mdbx_q1/mdbx_q6) - see the plan's Context section for why the
# other queries aren't comparable everywhere. Both vWeaver_ermia variants got it too, via
# patches/vweaver_ermia_chbenchmark.patch's RunChQ1/RunChQ6 in ERMIA's own
# benchmarks/tpcc.cc (see manual.txt section 5) - though vweaver_ermia_frugal has a
# separate, pre-existing KNOWN ISSUE (also section 5) that crashes it on any sustained
# workload, htap_q1/htap_q6 included. Each is one full ORDER_LINE table scan (pure
# aggregation for Q1, filtered aggregation for Q6), no joins - see
# src/bat_bench/tpch_queries.rs::q1/q6, the reference implementation every engine's own port
# (including libmdbx's and both vWeaver_ermia variants') mirrors function-for-function.
HTAP_WORKLOADS = ["htap_q1", "htap_q6"]
# "S-HTAP" streaming workload (see Scale's s_htap_* fields' doc) - one name, no
# lettered variants (unlike YCSB A-F): the interesting axis here is the hot_window/
# olap_lag/olap_span shape, not a fixed menu of op-mix presets, so it stays a single
# workload tuned via those Scale fields / compare_engines.py flags instead.
ALL_WORKLOADS = ["tpcc"] + YCSB_WORKLOADS + HTAP_WORKLOADS + ["s_htap"]
ENGINES = ["batstore", "leanstore", "wiredtiger", "postgres", "vweaver_ermia", "vweaver_ermia_frugal", "libmdbx"]


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


def _read_vmrss_kb(pid: int) -> float:
    """Current (not peak) resident set size for a running process, 0 if it has exited -
    used to sum memory across a whole process TREE (see sum_process_tree_rss_mb), where
    each member's own VmHWM would double-count each process's historical peak rather than
    a coherent instant-in-time total across the tree."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return float(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        pass
    return 0.0


def _process_tree_pids(root_pid: int) -> list:
    """root_pid plus every recursive descendant, found by scanning /proc/*/stat's PPID
    field - used for multi-process engines (PostgreSQL: postmaster + checkpointer +
    bgwriter + walwriter + one backend process per connection) where a single PID's RSS
    isn't the whole engine's memory footprint."""
    children = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text()
            # comm (2nd field) is parenthesized and may itself contain spaces/parens, so
            # split on the LAST ')' - everything after it is state, ppid, ...
            ppid = int(stat.rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            continue
        children.setdefault(ppid, []).append(int(entry.name))

    pids, stack = [], [root_pid]
    while stack:
        pid = stack.pop()
        pids.append(pid)
        stack.extend(children.get(pid, []))
    return pids


def sum_process_tree_rss_mb(root_pid: int) -> float:
    return sum(_read_vmrss_kb(pid) for pid in _process_tree_pids(root_pid)) / 1024.0


def start_process_tree_sampler(root_pid: int, interval: float = 0.5):
    """Background peak-RSS sampler for a whole process tree, mirroring
    run_and_track_rss's own single-PID sampler thread but tracking the peak of the
    SUMMED current RSS across root_pid + all its descendants (there's no per-tree
    equivalent of a single process's VmHWM to just read directly).

    Returns (stop_event, thread, peak_box) - set stop_event, join thread, then read
    peak_box["mb"] once stopped.
    """
    peak_box = {"mb": 0.0}
    stop = threading.Event()

    def loop():
        while not stop.is_set():
            peak_box["mb"] = max(peak_box["mb"], sum_process_tree_rss_mb(root_pid))
            stop.wait(interval)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return stop, thread, peak_box


def run_and_track_rss(cmd, cwd=None, env=None, stdout_path: Optional[Path] = None, timeout=None):
    """Runs `cmd` to completion under `numactl --cpubind=0 --membind=0`, sampling peak RSS
    via /proc every 0.5s.

    Returns (returncode, peak_rss_mb). stdout+stderr are merged and written to
    stdout_path if given (for post-hoc debugging), else discarded.

    `timeout` (seconds) bounds how long a single subprocess may run. Without it, a
    genuinely stuck engine (observed: libmdbx's population phase stalling indefinitely
    under host contention - see task notes) blocks this ENTIRE call forever, and since
    every caller here runs sequentially in one sweep (compare_engines.py), one stuck
    data point silently hangs the whole multi-hour comparison with no way to recover
    short of someone noticing and killing it by hand. On timeout, the whole process
    group is force-killed (not just `cmd`'s own PID) and `(None, peak_rss_mb)` is
    returned - `None` is the caller's signal to record this point as a timeout rather
    than treat it as a crash (returncode would otherwise collide with a real negative
    signal-exit code). `start_new_session=True` puts `cmd` in its own process group
    specifically so this cleanup can reach any children numactl/the engine itself
    spawns, rather than relying on numactl having exec'd in place of forking.
    """
    cmd = numactl_prefix() + [str(c) for c in cmd]
    stdout_file = open(stdout_path, "wb") if stdout_path else subprocess.DEVNULL
    proc = subprocess.Popen(
        cmd, cwd=cwd, env=env, stdout=stdout_file, stderr=subprocess.STDOUT, start_new_session=True,
    )
    peak_kb = 0.0
    stop = threading.Event()

    def sampler():
        nonlocal peak_kb
        while not stop.is_set():
            peak_kb = max(peak_kb, _read_vmhwm_kb(proc.pid))
            stop.wait(0.5)

    t = threading.Thread(target=sampler, daemon=True)
    t.start()
    timed_out = False
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()
    finally:
        stop.set()
        t.join()
        if stdout_path:
            stdout_file.close()

    return (None if timed_out else proc.returncode), peak_kb / 1024.0


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
    BatStore/LeanStore/WiredTiger's YCSB-E scan-latency instrumentation writes directly, since
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
            f"{result.scan_avg_us:.2f}", result.scan_count, result.notes,
        ])


def percentiles_from_samples(csv_path: Path, column: str, filter_column: str = None, filter_value: str = None) -> dict:
    """Reads one numeric `column` (e.g. per-op latency) from every row of `csv_path` and
    returns {p50,p95,p99,count,avg} in the same units as the input column. Pure-Python
    (sorted-list nearest-rank percentiles) so every engine wrapper computes percentiles the
    same way - no numpy/pandas dependency needed just for this.

    `filter_column`/`filter_value`, if given, restrict to rows where that column equals
    that value first - e.g. BatStore's tpcc_scan.csv mixes several OLAP modes/queries in one
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
