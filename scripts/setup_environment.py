#!/usr/bin/env python3
"""Bootstraps the standard engines used by scripts/compare_engines.py, FROM NOTHING:
clones their sibling repos (BatStore itself only if it's reachable, see step_batstore), applies
this harness's required patches, then builds each one, sets up PostgreSQL, and creates the
Python plotting venv. The failure-prone vWeaver/ERMIA variants and their hugepage setup are
excluded by default; pass --full to include them.

Everything is cloned/built under WORKSPACE_ROOT (scripts/engines/common.py -
<the directory you invoke this script from>/tx_tests by default, override via the
WORKSPACE_ROOT env var) - run this from wherever you want the whole workspace to live;
nothing here assumes a specific machine's home directory layout.

Reproducible by default: setup-managed checkouts are deleted and cloned again before
building. Pass --reuse-checkouts for the older incremental/idempotent behavior.

apt-get/postgres steps run via plain `sudo ...` (no `-y`, so apt's own
"Do you want to continue?" prompt still gates the install) - run this
script yourself in an interactive terminal so sudo can prompt you for your
password. It never tries to elevate privileges silently.

Usage:
    python3 scripts/setup_environment.py
    python3 scripts/setup_environment.py --full
    python3 scripts/setup_environment.py --skip-postgres --skip-benchbase
    WORKSPACE_ROOT=/data/tx_tests python3 scripts/setup_environment.py
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from engines import common  # noqa: E402 - needs sys.path set up first

WORKSPACE_ROOT = common.WORKSPACE_ROOT

# Shared with scripts/engines/*.py (single source of truth - see common.py's module docs
# for why these are env-var overridable).
LEANSTORE_REPO = common.LEANSTORE_REPO
WIREDTIGER_BUILD_DIR = common.WIREDTIGER_BUILD_DIR
WIREDTIGER_REPO = WIREDTIGER_BUILD_DIR.parent
LEANSTORE_BUILD_DIR = LEANSTORE_REPO / "build"
BENCHBASE_DIST = common.BENCHBASE_HOME
BENCHBASE_REPO = BENCHBASE_DIST.parent.parent
VWEAVER_REPO = common.VWEAVER_REPO
VWEAVER_BUILD_DIR = VWEAVER_REPO / "build"
VWEAVER_FRUGAL_BUILD_DIR = VWEAVER_REPO / "build_frugal"

BATSTORE_REPO_URL = "https://github.com/umr-dbs/BatStore.git"
BATSTORE_WORKSPACE_CLONE = WORKSPACE_ROOT / "batstore"
# The exact commit patches/leanstore.patch (this repo) was generated against - pinned so
# the patch always applies cleanly regardless of how far upstream LeanStore has moved.
LEANSTORE_URL = "https://github.com/leanstore/leanstore.git"
LEANSTORE_PATCH_COMMIT = "90fcf185c1c8506344a7aa779928787d494348f4"
LEANSTORE_PATCH_PATH = Path(__file__).resolve().parent.parent / "patches" / "leanstore.patch"
LEANSTORE_YCSB_PAYLOAD_PATCH_PATH = Path(__file__).resolve().parent.parent / "patches" / "ycsb_payload_leanstore.patch"
LEANSTORE_TPCC_SEMANTICS_PATCH_PATH = Path(__file__).resolve().parent.parent / "patches" / "tpcc_semantics_leanstore.patch"
LEANSTORE_S_HTAP_PATCH_PATH = Path(__file__).resolve().parent.parent / "patches" / "s_htap_leanstore.patch"
WIREDTIGER_URL = "https://github.com/wiredtiger/wiredtiger.git"
BENCHBASE_URL = "https://github.com/cmu-db/benchbase.git"
BENCHBASE_PATCH_COMMIT = "33c00473807ebd49304d114a6d769d2d2b2bbb34"
BENCHBASE_YCSB_PAYLOAD_PATCH_PATH = Path(__file__).resolve().parent.parent / "patches" / "ycsb_payload_benchbase.patch"
VWEAVER_URL = "https://github.com/SNU-DBXLab-papers/vWeaver_ermia.git"
# Pinned so patches/vweaver_ermia.patch (removal of `sys/vtimes.h`, which is absent from
# modern glibc, plus benchmark start-barrier and TPC-C extra-worker fixes) always applies
# cleanly.
# This is the SNU-DBXLab-papers repo's own
# default branch ("vweaver") HEAD at the time this was verified - NOT the same commit or
# even the same repo as an earlier, mistaken pin against a divergent fork (Rudeus/
# vWeaver_ermia) that doesn't share this history at all.
VWEAVER_PATCH_COMMIT = "82a287bff035169ef7c751a84df5df83038aec5f"
VWEAVER_PATCH_PATH = Path(__file__).resolve().parent.parent / "patches" / "vweaver_ermia.patch"
# Fixes two upstream bugs in the pure "just frugal lists" build (-DCMAKE_BUILD_PARAM=
# Eval_skiplist, i.e. -DHYU_SKIPLIST with no -DHYU_VWEAVER - see engines/
# vweaver_ermia_frugal.py's module docstring for the full root-cause writeup): a missing
# MM::deallocate_skiplist() definition (declared and called, never defined - a hard link
# error) and MM::gc_version_chain()'s dedicated HYU_SKIPLIST branch being present in the
# source but commented out (silently leaking each reclaimed version's Lv-pointer array via
# the vanilla masstree branch instead). Entirely guarded by #ifdef HYU_SKIPLIST, so it's a
# no-op for the plain "Vweaver" build - applied unconditionally onto this one shared
# checkout right alongside vweaver_ermia.patch, regardless of which variant(s) actually
# get built from it.
VWEAVER_FRUGAL_PATCH_PATH = Path(__file__).resolve().parent.parent / "patches" / "vweaver_ermia_frugal.patch"
# Adds CH-benCHmark Q1 ("Pricing Summary Report")/Q6 ("Forecasting Revenue Change") support
# (RunChQ1/RunChQ6 in benchmarks/tpcc.cc, a dedicated OLAP thread rotating them concurrently
# with the normal OLTP tpcc_worker threads, gated behind a new -enable-chbenchmark
# benchmark_option flag) - upstream had neither; see engines/vweaver_ermia.py's module
# docstring for the full writeup, including a real heap-corruption bug (str_arena overrun,
# silent in a Release build) hit and fixed during development. Applies identically to both
# CMAKE_BUILD_PARAM variants (no #ifdef HYU_VWEAVER/HYU_SKIPLIST branching in this patch at
# all), so - like vweaver_ermia_frugal.patch - it's applied unconditionally onto the one
# shared checkout regardless of which variant(s) actually get built from it.
VWEAVER_CHBENCHMARK_PATCH_PATH = Path(__file__).resolve().parent.parent / "patches" / "vweaver_ermia_chbenchmark.patch"
VWEAVER_YCSB_PAYLOAD_PATCH_PATH = Path(__file__).resolve().parent.parent / "patches" / "ycsb_payload_vweaver.patch"
# dbcore/burt-hash.cpp is gitignored upstream (dbcore/.gitignore) and meant to be generated
# fresh at build time by `python2 dbcore/burt-hash.py` (see dbcore/CMakeLists.txt) - no
# python2 on this system, so this repo ships a Python 3 port instead (see that file's header).
VWEAVER_BURT_HASH_GEN = Path(__file__).resolve().parent.parent / "patches" / "vweaver_burt_hash_gen.py"

# Everything LeanStore's own README asks for, minus librocksdb-dev/liblmdb-dev
# (only needed for the rocksdb_*/lmdb_* frontend targets, which
# scripts/engines/leanstore.py never builds), plus postgresql itself, plus numactl
# (every engine subprocess here runs under `numactl --cpubind=0 --membind=0` - see
# engines/common.py::run_and_track_rss - matching the real 2-NUMA-node server), plus
# maven (BenchBase is now built with plain `mvn`, not its bundled ./mvnw wrapper, so
# build errors are visible instead of the wrapper's own download/bootstrap noise), plus
# clang/libnuma-dev (vWeaver_ermia/ERMIA only builds with clang, and needs libnuma - see
# its README). No ninja-build: both cmake builds below go through `cmake --build`, which
# drives whatever generator got configured (default: Unix Makefiles via the
# system `make`, already required anyway) - one less dependency to install.
APT_PACKAGES = [
    "cmake", "libtbb-dev", "libaio-dev", "libsnappy-dev", "zlib1g-dev",
    "libbz2-dev", "liblz4-dev", "libzstd-dev", "liburing-dev", "numactl",
    "postgresql", "postgresql-contrib", "maven",
]
VWEAVER_APT_PACKAGES = ["clang", "libnuma-dev", "libgoogle-glog-dev", "libibverbs-dev"]

PG_ROLE = common.PG_ROLE
PG_PASSWORD = common.PG_PASSWORD
# Used by the default PostgreSQL tmpfs step (see step_postgres_tmpfs) - lives under the same
# tmpfs-verified SCRATCH_ROOT every other engine's data now uses (see common.py::
# fresh_scratch_dir), so PostgreSQL's storage gets the identical in-memory-only guarantee.
PG_TMPFS_DATA_DIR = common.SCRATCH_ROOT / "postgresql_data"
PG_DATABASE = common.PG_DATABASE


def log(msg: str) -> None:
    print(f"\n>>> {msg}")


def run(cmd, cwd=None, env=None, check=True) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(str(c) for c in cmd)}" + (f"   (in {cwd})" if cwd else ""))
    return subprocess.run(cmd, cwd=cwd, env=env, check=check)


def is_apt_package_installed(pkg: str) -> bool:
    result = subprocess.run(
        ["dpkg-query", "-W", "-f=${Status}", pkg], capture_output=True, text=True,
    )
    return result.returncode == 0 and "install ok installed" in result.stdout


def step_apt_packages(full: bool = False) -> None:
    log("Checking apt dependencies")
    packages = APT_PACKAGES + (VWEAVER_APT_PACKAGES if full else [])
    missing = [p for p in packages if not is_apt_package_installed(p)]
    if not missing:
        print("All required apt packages already installed.")
        return
    print(f"Missing packages: {missing}")
    print("Running sudo apt-get install (you'll be prompted for your password, "
          "and apt will ask its own yes/no confirmation before installing).")
    run(["sudo", "apt-get", "install"] + missing)


def step_fresh_checkouts() -> None:
    """Delete only checkouts owned by this setup under WORKSPACE_ROOT.

    Exact-path and containment checks make WORKSPACE_ROOT/environment overrides unable to
    turn this into a broad recursive deletion. The source checkout containing this script
    is never one of these targets.
    """
    log("Removing setup-managed checkouts for a reproducible fresh build")
    workspace = WORKSPACE_ROOT.resolve()
    targets = [
        WIREDTIGER_REPO, LEANSTORE_REPO, BENCHBASE_REPO, VWEAVER_REPO,
        BATSTORE_WORKSPACE_CLONE,
    ]
    for target in targets:
        resolved = target.resolve(strict=False)
        if resolved.parent != workspace:
            sys.exit(
                f"refusing to delete setup checkout {target}: expected a direct child of "
                f"WORKSPACE_ROOT={workspace}. Use --reuse-checkouts with custom repo paths."
            )
        if resolved.exists():
            print(f"Deleting stale setup checkout: {resolved}")
            shutil.rmtree(resolved)


def shutil_cpu_count() -> int:
    import os
    return os.cpu_count() or 4


def step_wiredtiger() -> None:
    log("Cloning + building WiredTiger (from-source, Release, ENABLE_PYTHON=OFF)")
    lib = WIREDTIGER_BUILD_DIR / "libwiredtiger.so"
    if lib.exists():
        print(f"{lib} already exists, skipping.")
        return
    if not WIREDTIGER_REPO.exists():
        run(["git", "clone", WIREDTIGER_URL, str(WIREDTIGER_REPO)])

    # `-S`/`-B` (not a pre-existing build dir + relative `.`) so this works on
    # a fresh checkout with no IDE-generated build directory yet, and is
    # independent of the process's current working directory.
    WIREDTIGER_BUILD_DIR.mkdir(parents=True, exist_ok=True)
    common.check_release_build(WIREDTIGER_BUILD_DIR, "WiredTiger")
    if not (WIREDTIGER_BUILD_DIR / "CMakeCache.txt").exists():
        run(["cmake", "-S", str(WIREDTIGER_REPO), "-B", str(WIREDTIGER_BUILD_DIR),
             "-DCMAKE_BUILD_TYPE=Release", "-DENABLE_PYTHON=OFF"])
    # `--build`/`--target`/`--parallel` are generator-agnostic (Makefiles,
    # Ninja, ...) - no need to know or require any specific build tool.
    run(["cmake", "--build", str(WIREDTIGER_BUILD_DIR), "--target", "wiredtiger_shared", "wt",
         "--parallel", str(shutil_cpu_count())])


def step_leanstore() -> None:
    log("Cloning + patching + building LeanStore (native tpcc/ycsb/s_htap + WiredTiger-adapter frontends)")
    targets = ["tpcc", "ycsb", "wiredtiger_tpcc", "wiredtiger_ycsb", "s_htap", "wiredtiger_s_htap"]
    binaries = [LEANSTORE_BUILD_DIR / "frontend" / t for t in targets]
    if not LEANSTORE_REPO.exists():
        run(["git", "clone", LEANSTORE_URL, str(LEANSTORE_REPO)])
        run(["git", "checkout", LEANSTORE_PATCH_COMMIT], cwd=LEANSTORE_REPO)
        log(f"Applying {LEANSTORE_PATCH_PATH.name} (CH-benCHmark Q1/Q6 analytical queries, "
            f"YCSB-E/HTAP scan-latency instrumentation, New-Order-only counters, WiredTiger "
            f"adapter log=(enabled=true) so its WAL isn't silently off in this comparison)")
        run(["git", "apply", str(LEANSTORE_PATCH_PATH)], cwd=LEANSTORE_REPO)
    if subprocess.run(["git", "apply", "--reverse", "--check", str(LEANSTORE_YCSB_PAYLOAD_PATCH_PATH)],
                      cwd=LEANSTORE_REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
        log(f"Applying {LEANSTORE_YCSB_PAYLOAD_PATCH_PATH.name} (canonical/u64 YCSB payloads and explicit payload reads)")
        run(["git", "apply", str(LEANSTORE_YCSB_PAYLOAD_PATCH_PATH)], cwd=LEANSTORE_REPO)
    if subprocess.run(["git", "apply", "--reverse", "--check", str(LEANSTORE_TPCC_SEMANTICS_PATCH_PATH)],
                      cwd=LEANSTORE_REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
        log(f"Applying {LEANSTORE_TPCC_SEMANTICS_PATCH_PATH.name} (TPC-C 1% New-Order rollback)")
        run(["git", "apply", str(LEANSTORE_TPCC_SEMANTICS_PATCH_PATH)], cwd=LEANSTORE_REPO)
    if subprocess.run(["git", "apply", "--reverse", "--check", str(LEANSTORE_S_HTAP_PATCH_PATH)],
                      cwd=LEANSTORE_REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
        log(f"Applying {LEANSTORE_S_HTAP_PATCH_PATH.name} (native + WiredTiger-adapter S-HTAP frontends)")
        run(["git", "apply", str(LEANSTORE_S_HTAP_PATCH_PATH)], cwd=LEANSTORE_REPO)

    LEANSTORE_BUILD_DIR.mkdir(parents=True, exist_ok=True)
    common.check_release_build(LEANSTORE_BUILD_DIR, "LeanStore")
    if not (LEANSTORE_BUILD_DIR / "CMakeCache.txt").exists():
        wt_include = WIREDTIGER_BUILD_DIR / "include"
        run([
            "cmake", "-S", str(LEANSTORE_REPO), "-B", str(LEANSTORE_BUILD_DIR),
            "-DCMAKE_BUILD_TYPE=Release",
            f"-DCMAKE_CXX_FLAGS=-I{wt_include}",
            f"-DCMAKE_EXE_LINKER_FLAGS=-L{WIREDTIGER_BUILD_DIR} -Wl,-rpath,{WIREDTIGER_BUILD_DIR}",
        ])

    # `--build`/`--target`/`--parallel`: generator-agnostic, no `cwd=` needed.
    run(["cmake", "--build", str(LEANSTORE_BUILD_DIR), "--target", *targets,
         "--parallel", str(shutil_cpu_count())])


def step_vweaver_hugepages() -> None:
    # ermia_SI preallocates its whole working-memory pool up front via
    # mmap(..., MAP_HUGETLB) once per NUMA node (dbcore/sm-alloc.cpp::prepare_node_memory,
    # sized by -node_memory_gb - see engines/vweaver_ermia.py) and throws (uncaught
    # os_error, crashing the process) if the kernel has no hugepages reserved at all - this
    # is independent of how large -node_memory_gb actually is, since /proc/sys/vm/nr_hugepages
    # defaults to 0 on a fresh machine. Reserves enough 2MB pages for
    # common.Scale().dram_gib (the default -node_memory_gb value, see that module) with a
    # 4x safety margin for multiple NUMA-node allocations; bump manually
    # (sudo sysctl -w vm.nr_hugepages=N) if you override --dram-gib higher than the default.
    log("Reserving hugepages for vWeaver_ermia's node-memory pool")
    needed_gb = int(common.Scale().dram_gib) * 4
    # Unlike everything else Scale.dram_gib sizes (a per-run buffer pool, freed the moment
    # that run's process exits), a hugepage reservation is a MACHINE-WIDE, PERSISTENT claim
    # that outlives this one step - it stays in effect for the rest of this comparison run
    # across every other engine too (LeanStore/WiredTiger's tmpfs-backed buffer pools,
    # Postgres, BenchBase's JVM all still need regular, non-huge memory afterward). Now that
    # Scale.dram_gib itself scales with the real server's ~500GB (see default_dram_gib), the
    # naive 4x here could try to reserve most of the machine - cap it well below the actual
    # total so ERMIA's one-time preallocation can't starve every engine that runs after it.
    total_gib = common.total_system_mem_gib()
    if total_gib > 0:
        cap_gb = int(total_gib * 0.4)
        if needed_gb > cap_gb:
            print(f"Computed hugepage request (~{needed_gb}GB, from dram_gib x4) exceeds 40% of "
                  f"this machine's {total_gib:.0f}GB total - capping at {cap_gb}GB so the rest of "
                  f"this comparison run isn't starved by a reservation that outlives this one step. "
                  f"Raise it yourself (`sudo sysctl -w vm.nr_hugepages=N`) if ERMIA actually needs "
                  f"more than that.")
            needed_gb = cap_gb
    needed_pages = (needed_gb * 1024) // 2  # kernel default Hugepagesize is 2048kB
    current = subprocess.run(
        ["sysctl", "-n", "vm.nr_hugepages"], capture_output=True, text=True, check=True,
    )
    if int(current.stdout.strip() or 0) >= needed_pages:
        print(f"vm.nr_hugepages already >= {needed_pages}, skipping.")
        return
    print(f"Reserving {needed_pages} x 2MB hugepages (~{needed_gb}GB) via sudo sysctl "
          f"(you'll be prompted for your password).")
    run(["sudo", "sysctl", "-w", f"vm.nr_hugepages={needed_pages}"])


def step_vweaver_ermia() -> None:
    log("Cloning + building vWeaver_ermia (ERMIA) - ermia_SI target only")
    binary = VWEAVER_BUILD_DIR / "ermia_SI"
    if not VWEAVER_REPO.exists():
        run(["git", "clone", VWEAVER_URL, str(VWEAVER_REPO)])
        run(["git", "checkout", VWEAVER_PATCH_COMMIT], cwd=VWEAVER_REPO)
        log(f"Applying {VWEAVER_PATCH_PATH.name} (dead sys/vtimes.h include + "
            f"benchmark start-barrier/TPC-C worker-count fixes)")
        run(["git", "apply", str(VWEAVER_PATCH_PATH)], cwd=VWEAVER_REPO)
        log(f"Applying {VWEAVER_FRUGAL_PATCH_PATH.name} (missing deallocate_skiplist() "
            f"definition + disabled HYU_SKIPLIST GC branch - no-op for this Vweaver build, "
            f"needed by the separate vweaver_ermia_frugal build below)")
        run(["git", "apply", str(VWEAVER_FRUGAL_PATCH_PATH)], cwd=VWEAVER_REPO)
        log(f"Applying {VWEAVER_CHBENCHMARK_PATCH_PATH.name} (adds CH-benCHmark Q1/Q6 - "
            f"htap_q1/htap_q6 - support to both build variants)")
        run(["git", "apply", str(VWEAVER_CHBENCHMARK_PATCH_PATH)], cwd=VWEAVER_REPO)
    if subprocess.run(["git", "apply", "--reverse", "--check", str(VWEAVER_YCSB_PAYLOAD_PATCH_PATH)],
                      cwd=VWEAVER_REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
        log(f"Applying {VWEAVER_YCSB_PAYLOAD_PATCH_PATH.name} (canonical/u64 YCSB payloads and key-only option)")
        run(["git", "apply", str(VWEAVER_YCSB_PAYLOAD_PATCH_PATH)], cwd=VWEAVER_REPO)

    burt_hash_cpp = VWEAVER_REPO / "dbcore" / "burt-hash.cpp"
    if not burt_hash_cpp.exists() or burt_hash_cpp.stat().st_size == 0:
        log(f"Generating {burt_hash_cpp} via {VWEAVER_BURT_HASH_GEN.name} (Python 3 port - "
            f"see that file's header)")
        with open(burt_hash_cpp, "w") as f:
            subprocess.run(["python3", str(VWEAVER_BURT_HASH_GEN)], stdout=f, check=True)

    # README: "We do not allow building in the source directory" + requires clang.
    # CMAKE_BUILD_PARAM=Vweaver (not CMAKE_BUILD_TYPE, which this repo's CMakeLists.txt
    # doesn't branch on for anything but flag selection) picks the actual "vWeaver" system
    # under evaluation.
    VWEAVER_BUILD_DIR.mkdir(parents=True, exist_ok=True)
    common.check_release_build(VWEAVER_BUILD_DIR, "vWeaver_ermia")
    if not (VWEAVER_BUILD_DIR / "CMakeCache.txt").exists():
        run(["cmake", "-S", str(VWEAVER_REPO), "-B", str(VWEAVER_BUILD_DIR),
             "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_BUILD_PARAM=Vweaver"],
            env=_clang_env())
    run(["cmake", "--build", str(VWEAVER_BUILD_DIR), "--target", "ermia_SI",
         "--parallel", str(shutil_cpu_count())])


def step_vweaver_ermia_frugal() -> None:
    log("Building vweaver_ermia_frugal (ERMIA, plain frugal-list version chain, no vWeaver) "
        "- ermia_SI target only, into its own build_frugal/ dir")
    binary = VWEAVER_FRUGAL_BUILD_DIR / "ermia_SI"
    if binary.exists():
        print(f"{binary} already built, skipping.")
        return
    if not VWEAVER_REPO.exists():
        sys.exit(f"{VWEAVER_REPO} doesn't exist yet - run the 'vweaver' step first "
                  f"(it clones + patches the shared checkout both variants build from).")

    burt_hash_cpp = VWEAVER_REPO / "dbcore" / "burt-hash.cpp"
    if not burt_hash_cpp.exists() or burt_hash_cpp.stat().st_size == 0:
        log(f"Generating {burt_hash_cpp} via {VWEAVER_BURT_HASH_GEN.name} (Python 3 port - "
            f"see that file's header)")
        with open(burt_hash_cpp, "w") as f:
            subprocess.run(["python3", str(VWEAVER_BURT_HASH_GEN)], stdout=f, check=True)

    # CMAKE_BUILD_PARAM=Eval_skiplist -> -DHYU_SKIPLIST -O3 (no -DHYU_VWEAVER) - the plain
    # frugal-list version chain, with neither vWeaver's own compaction nor the Eval_frugal
    # variant's HYU_VWEAVER-dependent side-by-side stat collection. Needs
    # patches/vweaver_ermia_frugal.patch (applied by step_vweaver_ermia() onto this same
    # checkout) to even link - see that patch and engines/vweaver_ermia_frugal.py's module
    # docstring.
    VWEAVER_FRUGAL_BUILD_DIR.mkdir(parents=True, exist_ok=True)
    common.check_release_build(VWEAVER_FRUGAL_BUILD_DIR, "vweaver_ermia_frugal")
    if not (VWEAVER_FRUGAL_BUILD_DIR / "CMakeCache.txt").exists():
        run(["cmake", "-S", str(VWEAVER_REPO), "-B", str(VWEAVER_FRUGAL_BUILD_DIR),
             "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_BUILD_PARAM=Eval_skiplist"],
            env=_clang_env())
    run(["cmake", "--build", str(VWEAVER_FRUGAL_BUILD_DIR), "--target", "ermia_SI",
         "--parallel", str(shutil_cpu_count())])


def _clang_env() -> dict:
    import os
    env = os.environ.copy()
    env["CC"] = shutil.which("clang") or "clang"
    env["CXX"] = shutil.which("clang++") or "clang++"
    # clang can find GCC's C++ runtime lib for linking but not always its headers (or vice
    # versa) when multiple GCC versions coexist (observed here: clang auto-selected GCC 14's
    # toolchain dir for header search, but only GCC 13's libstdc++-dev is installed) -
    # pointed explicitly at whichever GCC version's C++ headers/libs are actually present.
    gcc_cpp_version = "13"
    env["CXXFLAGS"] = (
        f"-I/usr/include/c++/{gcc_cpp_version} -I/usr/include/x86_64-linux-gnu/c++/{gcc_cpp_version} "
        + env.get("CXXFLAGS", "")
    )
    env["LDFLAGS"] = f"-L/usr/lib/gcc/x86_64-linux-gnu/{gcc_cpp_version} " + env.get("LDFLAGS", "")
    return env


def step_postgres() -> None:
    log("Setting up PostgreSQL role/database for BenchBase")
    if shutil.which("psql") is None:
        sys.exit("psql not found - install the 'postgresql' apt package first (see step above).")

    # Package installation does not necessarily start PostgreSQL (notably in containers
    # and on hosts where it was stopped previously).  Probe first so an already-running
    # installation remains untouched, then start the service and retry.  Previously the
    # failed probe was mistaken for "role absent", producing a misleading CREATE ROLE
    # failure (and, later, ValueError from int('')).
    probe_cmd = ["sudo", "-u", "postgres", "psql", "-X", "-v", "ON_ERROR_STOP=1",
                 "-tAc", "SELECT 1"]
    probe = subprocess.run(probe_cmd, capture_output=True, text=True)
    if probe.returncode != 0:
        log("PostgreSQL is not accepting connections; starting the service")
        run(["sudo", "systemctl", "start", "postgresql"])
        probe = subprocess.run(probe_cmd, capture_output=True, text=True)
        if probe.returncode != 0:
            sys.exit(
                "PostgreSQL was started but is still not accepting local connections:\n"
                f"{probe.stderr.strip()}\n"
                "Check `sudo systemctl status postgresql` and the PostgreSQL log."
            )

    def postgres_sql(sql: str) -> subprocess.CompletedProcess:
        result = subprocess.run(
            ["sudo", "-u", "postgres", "psql", "-X", "-v", "ON_ERROR_STOP=1",
             "-tAc", sql],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            sys.exit(f"PostgreSQL command failed:\n{result.stderr.strip()}")
        return result

    check = postgres_sql(f"SELECT 1 FROM pg_roles WHERE rolname='{PG_ROLE}'")
    if check.stdout.strip() == "1":
        print(f"Role '{PG_ROLE}' already exists, skipping.")
    else:
        run(["sudo", "-u", "postgres", "psql", "-c",
             f"CREATE ROLE {PG_ROLE} WITH LOGIN SUPERUSER PASSWORD '{PG_PASSWORD}';"])

    check_db = postgres_sql(f"SELECT 1 FROM pg_database WHERE datname='{PG_DATABASE}'")
    if check_db.stdout.strip() == "1":
        print(f"Database '{PG_DATABASE}' already exists, skipping.")
    else:
        run(["sudo", "-u", "postgres", "psql", "-c",
             f"CREATE DATABASE {PG_DATABASE} OWNER {PG_ROLE};"])

    # Default max_connections=100 is below compare_engines.py's own DEFAULT_THREADS ceiling
    # (128, see compare_engines.py) - BenchBase opens roughly one JDBC connection per
    # terminal/thread, so the highest thread-count sweep points fail to even connect
    # without this. Sized well above 128 for headroom (superuser/monitoring connections
    # also count against the limit).
    max_conn = postgres_sql("SHOW max_connections")
    if int(max_conn.stdout.strip()) < 300:
        log("Raising PostgreSQL max_connections to 300 (default 100 is below the thread sweep's ceiling)")
        run(["sudo", "-u", "postgres", "psql", "-c", "ALTER SYSTEM SET max_connections = 300;"])
        run(["sudo", "systemctl", "restart", "postgresql"])
    else:
        print(f"max_connections already {max_conn.stdout.strip()}, skipping.")


def _pg_data_directory() -> Path:
    """Data directory of the (first) PostgreSQL cluster, via `pg_lsclusters` - unlike
    `SHOW data_directory` over psql, this doesn't need the server to actually be up, which
    matters for step_postgres_tmpfs's post-reboot recovery path (the server is down at
    exactly the point this needs to find where to restore its data TO)."""
    result = subprocess.run(["pg_lsclusters", "-h"], capture_output=True, text=True, check=True)
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        sys.exit("pg_lsclusters found no PostgreSQL cluster - is PostgreSQL installed (see step above)?")
    fields = lines[0].split()
    return Path(fields[5])


def step_postgres_tmpfs() -> None:
    """Moves the real PostgreSQL server's data directory onto tmpfs (PG_TMPFS_DATA_DIR,
    under common.SCRATCH_ROOT - the same tmpfs-verified root every other engine's data now
    uses, see common.py::fresh_scratch_dir), so PostgreSQL gets the same in-memory-only
    guarantee LeanStore/WiredTiger/libmdbx/vWeaver_ermia already have from that function,
    and BatStore already has for its tmpfs-backed WAL.

    Run by default (disable with --skip-postgres-tmpfs). Unlike every other step here,
    this one STOPS your real, already-running PostgreSQL SERVER (not a
    subprocess this harness spawns and owns) and relocates its actual data. Run
    step_postgres() (role/db/max_connections) first - this rsyncs whatever's already in
    the real data directory, so the `admin` role and `benchbase` database created there
    carry over automatically.

    Idempotent and safe to re-run: the ORIGINAL on-disk data directory is renamed aside to
    `<original>.diskbackup`, never deleted, and stays the authoritative on-disk copy -
    tmpfs is repopulated from it (via rsync) whenever PG_TMPFS_DATA_DIR is missing or
    doesn't already look like a valid cluster (no PG_VERSION file), which is exactly the
    state you'll find it in after a reboot (tmpfs is volatile and comes back empty).

    KNOWN LIMITATION: this does NOT survive a reboot unattended - there is no systemd unit
    installed to auto-restore PG_TMPFS_DATA_DIR before postgresql.service starts (that
    would mean authoring/testing a systemd dependency override against a real production
    Postgres install, which this harness deliberately does not attempt sight-unseen). If
    the machine reboots, PostgreSQL will fail to start (empty tmpfs dir) until you re-run
    the setup script (the full default invocation is simplest), which restores tmpfs from
    `.diskbackup` automatically, the same as a first run.
    """
    log("Relocating PostgreSQL's data directory onto tmpfs")
    if shutil.which("psql") is None:
        sys.exit("psql not found - install the 'postgresql' apt package first (see step above).")

    real_datadir = _pg_data_directory()
    backup_dir = real_datadir.with_name(real_datadir.name + ".diskbackup")

    already_linked = real_datadir.is_symlink() and real_datadir.resolve() == PG_TMPFS_DATA_DIR.resolve()
    tmpfs_populated = subprocess.run(
        ["sudo", "-u", "postgres", "test", "-f",
         str(PG_TMPFS_DATA_DIR / "PG_VERSION")]
    ).returncode == 0

    # Running the whole script through sudo makes mkdir(parents=True) create the shared
    # scratch root as root. Restore the actual invoking user's ownership so subsequent
    # engines can create sibling directories such as libmdbx_data. PostgreSQL's child
    # directory remains separately owned by postgres.
    common.SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
    if "SUDO_UID" in os.environ and "SUDO_GID" in os.environ:
        invoking_uid = int(os.environ["SUDO_UID"])
        invoking_gid = int(os.environ["SUDO_GID"])
        os.chown(common.SCRATCH_ROOT, invoking_uid, invoking_gid)
        # Repair engine scratch directories left by an earlier root-run too. Do not touch
        # postgresql_data: the server correctly requires that tree to remain postgres-owned.
        for name in (
            "batstore_data", "leanstore_data", "wiredtiger_data", "libmdbx_data",
            "vweaver_ermia_log", "vweaver_ermia_frugal_log",
        ):
            engine_dir = common.SCRATCH_ROOT / name
            if engine_dir.exists():
                run(["chown", "-R", f"{invoking_uid}:{invoking_gid}", str(engine_dir)])

    if already_linked and tmpfs_populated:
        print(f"{real_datadir} is already a symlink into tmpfs and looks populated - skipping.")
        return

    if not already_linked and not backup_dir.exists():
        # First run: the data directory is still the real, disk-backed cluster. Stop the
        # server before moving anything out from under it.
        log(f"Stopping PostgreSQL and moving {real_datadir} -> {backup_dir} (permanent on-disk backup)")
        run(["sudo", "systemctl", "stop", "postgresql"])
        run(["sudo", "mv", str(real_datadir), str(backup_dir)])
    elif not already_linked:
        # real_datadir exists as a real directory AND a .diskbackup already exists too -
        # an inconsistent state (e.g. a previous run died between the mv above and creating
        # the symlink below) - don't guess, let the user look at it.
        sys.exit(
            f"{real_datadir} is a real directory AND {backup_dir} already exists - refusing to "
            f"guess which one is authoritative. Inspect both manually, then either remove "
            f"{real_datadir} and re-run (if {backup_dir} is the good copy) or remove {backup_dir}."
        )
    else:
        # Already symlinked from a prior run, just not currently running (service stopped,
        # or tmpfs came back empty after a reboot) - stop it if it's up before rsyncing.
        run(["sudo", "systemctl", "stop", "postgresql"], check=False)

    if not backup_dir.exists():
        sys.exit(f"{backup_dir} (the on-disk backup) doesn't exist - can't restore tmpfs from it.")

    if not (PG_TMPFS_DATA_DIR / "PG_VERSION").exists():
        log(f"Populating {PG_TMPFS_DATA_DIR} (tmpfs) from {backup_dir}")
        PG_TMPFS_DATA_DIR.mkdir(parents=True, exist_ok=True)
        run(["sudo", "rsync", "-a", "--delete", f"{backup_dir}/", f"{PG_TMPFS_DATA_DIR}/"])
        run(["sudo", "chown", "-R", "postgres:postgres", str(PG_TMPFS_DATA_DIR)])
        run(["sudo", "chmod", "700", str(PG_TMPFS_DATA_DIR)])

    if real_datadir.exists() or real_datadir.is_symlink():
        run(["sudo", "rm", "-f" if real_datadir.is_symlink() else "-rf", str(real_datadir)])
    run(["sudo", "ln", "-s", str(PG_TMPFS_DATA_DIR), str(real_datadir)])

    run(["sudo", "systemctl", "start", "postgresql"])
    fstype = common._mount_fstype(PG_TMPFS_DATA_DIR)
    if fstype != "tmpfs":
        sys.exit(f"{PG_TMPFS_DATA_DIR} isn't tmpfs (fstype={fstype!r}) - SCRATCH_ROOT misconfigured?")
    verify = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-tAc", "SELECT 1;"], capture_output=True, text=True,
    )
    if verify.returncode != 0 or verify.stdout.strip() != "1":
        sys.exit(f"PostgreSQL didn't come back up after the move - see: {verify.stderr}")
    print(f"PostgreSQL data directory is now {real_datadir} -> {PG_TMPFS_DATA_DIR} (tmpfs); "
          f"on-disk backup preserved at {backup_dir}.")


# Non-essential quality-gate plugins (code-style checks, static analysis) that: (a) aren't
# needed to produce a working benchbase.jar, and (b) some build environments (observed on
# the server: com.spotify.fmt:fmt-maven-plugin) can't resolve/download at all - Maven must
# fetch a plugin's descriptor before it can even decide whether to skip its goals, so a
# `-Dfmt.skip=true`-style property doesn't help when the actual problem is the download
# itself failing. Stripped from pom.xml entirely, right after cloning, so Maven never
# attempts to resolve them. Add more (groupId, artifactId) pairs here if another
# environment hits the same class of failure with a different plugin.
BENCHBASE_POM_PLUGINS_TO_STRIP = [
    ("com.spotify.fmt", "fmt-maven-plugin"),
]


def _patch_benchbase_pom(pom_path: Path) -> None:
    text = pom_path.read_text()
    original = text
    for group_id, artifact_id in BENCHBASE_POM_PLUGINS_TO_STRIP:
        # Matches the whole <plugin>...</plugin> block containing this artifactId - safe
        # because Maven <plugin> elements never nest another <plugin> inside themselves.
        pattern = re.compile(
            r"[ \t]*<plugin>(?:(?!</plugin>).)*?<artifactId>" + re.escape(artifact_id)
            + r"</artifactId>(?:(?!</plugin>).)*?</plugin>\n?",
            re.DOTALL,
        )
        new_text, n = pattern.subn("", text)
        if n:
            log(f"Stripped {n} {group_id}:{artifact_id} block(s) from {pom_path.name} "
                f"(build-environment workaround, see BENCHBASE_POM_PLUGINS_TO_STRIP)")
            text = new_text
        elif artifact_id not in text:
            pass  # already stripped by a prior run - nothing to do, stays idempotent
    if text != original:
        pom_path.write_text(text)


def step_benchbase() -> None:
    log("Building BenchBase (PostgreSQL TPC-C/YCSB client)")
    if not BENCHBASE_REPO.exists():
        run(["git", "clone", BENCHBASE_URL, str(BENCHBASE_REPO)])
        run(["git", "checkout", BENCHBASE_PATCH_COMMIT], cwd=BENCHBASE_REPO)

    # Kept as a main-repository patch because BenchBase is an external checkout under
    # ignored tx_tests/. This makes the JDBC column fix and payload/read modes reproducible.
    reverse = subprocess.run(
        ["git", "apply", "--reverse", "--check", str(BENCHBASE_YCSB_PAYLOAD_PATCH_PATH)],
        cwd=BENCHBASE_REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if reverse.returncode != 0:
        run(["git", "apply", str(BENCHBASE_YCSB_PAYLOAD_PATCH_PATH)], cwd=BENCHBASE_REPO)

    _patch_benchbase_pom(BENCHBASE_REPO / "pom.xml")

    # `mvn` (not the bundled ./mvnw wrapper) so build errors show up directly in this
    # console instead of being interleaved with (or hidden by) the wrapper's own
    # download/bootstrap output - and no `-q`, for the same reason: full Maven output
    # visible here to debug plugin/dependency failures on sight.
    # BenchBase's pom.xml targets Java 23; override to whatever JDK is actually installed
    # (verified fine with 21) rather than requiring a JDK 23 install. `-Dmaven.compiler.
    # release=21` (not separate source/target) matters here: plain source/target 21 on a
    # newer JDK (e.g. 25) makes javac emit "system modules path not set in conjunction with
    # -source 21" - a WARNING everywhere else, but BenchBase's own pom.xml enables
    # -Werror, turning it into a hard build failure. -Dmaven.compiler.release uses the
    # JDK's bundled ct.sym API data for the target release instead, which avoids the
    # warning entirely (confirmed: plain source/target 21 fails on JDK 25 here, release=21
    # builds clean).
    run([
        "mvn", "clean", "package", "-P", "postgres",
        "-DskipTests", "-Dmaven.compiler.release=21",
    ], cwd=BENCHBASE_REPO)

    tgz = BENCHBASE_REPO / "target" / "benchbase-postgres.tgz"
    if not tgz.exists():
        sys.exit(f"Build finished but {tgz} is missing - check the Maven output above.")
    run(["tar", "xzf", str(tgz)], cwd=BENCHBASE_REPO / "target")


def step_batstore() -> None:
    log("Setting up BatStore")
    if not BATSTORE_WORKSPACE_CLONE.exists():
        log(f"Attempting to clone {BATSTORE_REPO_URL} into {BATSTORE_WORKSPACE_CLONE}")
        result = subprocess.run(["git", "clone", BATSTORE_REPO_URL, str(BATSTORE_WORKSPACE_CLONE)])
        if result.returncode != 0:
            shutil.rmtree(BATSTORE_WORKSPACE_CLONE, ignore_errors=True)  # drop any partial clone
            print(f"Clone failed (BatStore may be private, or this machine may lack "
                  f"access) - falling back to the checkout this script is already part of "
                  f"instead. If you expect access here (e.g. you've since been granted it, "
                  f"or you're on the real server with credentials configured), just re-run "
                  f"this script.")

    active_repo = (
        BATSTORE_WORKSPACE_CLONE if (BATSTORE_WORKSPACE_CLONE / "Cargo.toml").exists()
        else common.BATSTORE_REPO
    )
    log(f"Building BatStore ({active_repo})")
    # setup_environment promises a binary usable by every wrapper, including libmdbx.
    # Building without this feature makes `compare_engines.py --skip-build --engines
    # libmdbx` fail because the mdbx_ycsb/mdbx_tpcc subcommands do not exist.
    run(common.batstore_cargo_build_args("mdbx-backend"), cwd=active_repo)


def step_python_venv() -> None:
    log("Setting up the Python plotting venv")
    venv_dir = common.BATSTORE_REPO / "scripts" / ".venv"
    pip = venv_dir / "bin" / "pip"
    if pip.exists():
        print(f"{venv_dir} already set up, skipping.")
        return
    run([sys.executable, "-m", "venv", str(venv_dir)])
    run([str(pip), "install", "-q", "-r", str(common.BATSTORE_REPO / "scripts" / "requirements.txt")])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--full", action="store_true",
        help="also set up both vWeaver/ERMIA variants and their required hugepages "
             "(excluded from the default setup)",
    )
    parser.add_argument(
        "--reuse-checkouts", action="store_true",
        help="do not delete and freshly clone setup-managed repositories (default is fresh)",
    )
    parser.add_argument("--skip-apt", action="store_true")
    parser.add_argument("--skip-wiredtiger", action="store_true")
    parser.add_argument("--skip-leanstore", action="store_true")
    parser.add_argument("--skip-vweaver", action="store_true")
    parser.add_argument("--skip-vweaver-frugal", action="store_true")
    parser.add_argument("--skip-hugepages", action="store_true")
    parser.add_argument("--skip-postgres", action="store_true")
    parser.add_argument("--skip-benchbase", action="store_true")
    parser.add_argument("--skip-batstore", "--skip-cmvbt", dest="skip_batstore", action="store_true")
    parser.add_argument("--skip-venv", action="store_true")
    parser.add_argument(
        "--skip-postgres-tmpfs", action="store_true",
        help="do not relocate the PostgreSQL server's data directory onto tmpfs "
             "(see step_postgres_tmpfs's docstring) "
             "so it gets the same in-memory-only guarantee as every other engine. Stops/restarts "
             "your actual PostgreSQL service and does not survive a reboot unattended - read the "
             "docstring before using this on a Postgres install you care about.",
    )
    args = parser.parse_args()

    if not args.reuse_checkouts:
        step_fresh_checkouts()

    steps = [
        ("apt", args.skip_apt, lambda: step_apt_packages(args.full)),
        ("wiredtiger", args.skip_wiredtiger, step_wiredtiger),
        ("leanstore", args.skip_leanstore, step_leanstore),
        ("hugepages", not args.full or args.skip_hugepages, step_vweaver_hugepages),
        ("vweaver", not args.full or args.skip_vweaver, step_vweaver_ermia),
        ("vweaver-frugal", not args.full or args.skip_vweaver_frugal, step_vweaver_ermia_frugal),
        ("postgres", args.skip_postgres, step_postgres),
        ("benchbase", args.skip_benchbase, step_benchbase),
        ("batstore", args.skip_batstore, step_batstore),
        ("venv", args.skip_venv, step_python_venv),
    ]

    print("########## cross-engine benchmark environment setup ##########")
    print(f"workspace root: {WORKSPACE_ROOT}")
    for name, skip, fn in steps:
        if skip:
            if name in {"hugepages", "vweaver", "vweaver-frugal"} and not args.full:
                print(f"\n>>> Skipping {name} (excluded by default; pass --full to include it)")
            else:
                print(f"\n>>> Skipping {name} (--skip-{name})")
            continue
        try:
            fn()
        except subprocess.CalledProcessError as e:
            sys.exit(f"\nStep '{name}' failed ({e}). Fix the issue above and re-run - "
                     f"earlier steps will be skipped since they're already done.")

    if not args.skip_postgres_tmpfs and not args.skip_postgres:
        try:
            step_postgres_tmpfs()
        except subprocess.CalledProcessError as e:
            sys.exit(f"\nStep 'postgres-tmpfs' failed ({e}). Your PostgreSQL service may currently be "
                     f"stopped or mid-move - check `systemctl status postgresql` and the step's "
                     f"docstring before re-running.")
    else:
        reason = "--skip-postgres" if args.skip_postgres else "--skip-postgres-tmpfs"
        print(f"\n>>> Skipping postgres-tmpfs ({reason})")

    print("\n########## setup complete ##########")
    print(f"Run the comparison with: python3 scripts/compare_engines.py --tiny")
    print("################################################################\n")


if __name__ == "__main__":
    main()
