#!/usr/bin/env python3
"""Bootstraps everything scripts/compare_engines.py needs, FROM NOTHING: clones every
sibling repo (LeanStore, WiredTiger, BenchBase, vWeaver_ermia/ERMIA - cMVBT itself only if
it's reachable, see step_cmvbt), applies this harness's required patches, then builds
each one, sets up PostgreSQL, and creates the Python plotting venv.

Everything is cloned/built under WORKSPACE_ROOT (scripts/engines/common.py -
<the directory you invoke this script from>/tx_tests by default, override via the
WORKSPACE_ROOT env var) - run this from wherever you want the whole workspace to live;
nothing here assumes a specific machine's home directory layout.

Idempotent - safe to re-run. Every step checks whether its target already
exists/works before doing anything, so a second run after a partial failure
just picks up where it left off.

apt-get/postgres steps run via plain `sudo ...` (no `-y`, so apt's own
"Do you want to continue?" prompt still gates the install) - run this
script yourself in an interactive terminal so sudo can prompt you for your
password. It never tries to elevate privileges silently.

Usage:
    python3 scripts/setup_environment.py
    python3 scripts/setup_environment.py --skip-postgres --skip-benchbase
    WORKSPACE_ROOT=/data/tx_tests python3 scripts/setup_environment.py
"""
from __future__ import annotations

import argparse
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

CMVBT_OSIC_URL = "https://github.com/umr-dbs/cMVBT-OSIC.git"
CMVBT_WORKSPACE_CLONE = WORKSPACE_ROOT / "cmvbt"
# The exact commit patches/leanstore.patch (this repo) was generated against - pinned so
# the patch always applies cleanly regardless of how far upstream LeanStore has moved.
LEANSTORE_URL = "https://github.com/leanstore/leanstore.git"
LEANSTORE_PATCH_COMMIT = "90fcf185c1c8506344a7aa779928787d494348f4"
LEANSTORE_PATCH_PATH = Path(__file__).resolve().parent.parent / "patches" / "leanstore.patch"
WIREDTIGER_URL = "https://github.com/wiredtiger/wiredtiger.git"
BENCHBASE_URL = "https://github.com/cmu-db/benchbase.git"
VWEAVER_URL = "https://github.com/SNU-DBXLab-papers/vWeaver_ermia.git"
# Pinned so patches/vweaver_ermia.patch (one fix: a dead `#include <sys/vtimes.h>`, removed
# from modern glibc) always applies cleanly. This is the SNU-DBXLab-papers repo's own
# default branch ("vweaver") HEAD at the time this was verified - NOT the same commit or
# even the same repo as an earlier, mistaken pin against a divergent fork (Rudeus/
# vWeaver_ermia) that doesn't share this history at all.
VWEAVER_PATCH_COMMIT = "82a287bff035169ef7c751a84df5df83038aec5f"
VWEAVER_PATCH_PATH = Path(__file__).resolve().parent.parent / "patches" / "vweaver_ermia.patch"
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
    "postgresql", "postgresql-contrib", "maven", "clang", "libnuma-dev",
    # vWeaver_ermia/ERMIA-specific: Google's logging library (linked unconditionally via
    # its CMakeLists.txt's LINK_FLAGS) and libibverbs (its dbcore/rdma.cpp is always
    # compiled in even though this harness never exercises RDMA replication).
    "libgoogle-glog-dev", "libibverbs-dev",
]

PG_ROLE = common.PG_ROLE
PG_PASSWORD = common.PG_PASSWORD
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


def step_apt_packages() -> None:
    log("Checking apt dependencies")
    missing = [p for p in APT_PACKAGES if not is_apt_package_installed(p)]
    if not missing:
        print("All required apt packages already installed.")
        return
    print(f"Missing packages: {missing}")
    print("Running sudo apt-get install (you'll be prompted for your password, "
          "and apt will ask its own yes/no confirmation before installing).")
    run(["sudo", "apt-get", "install"] + missing)


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
    log("Cloning + patching + building LeanStore (native tpcc/ycsb + WiredTiger-adapter frontends)")
    targets = ["tpcc", "ycsb", "wiredtiger_tpcc", "wiredtiger_ycsb"]
    binaries = [LEANSTORE_BUILD_DIR / "frontend" / t for t in targets]
    if all(b.exists() for b in binaries):
        print("All 4 frontend binaries already built, skipping.")
        return

    if not LEANSTORE_REPO.exists():
        run(["git", "clone", LEANSTORE_URL, str(LEANSTORE_REPO)])
        run(["git", "checkout", LEANSTORE_PATCH_COMMIT], cwd=LEANSTORE_REPO)
        log(f"Applying {LEANSTORE_PATCH_PATH.name} (CH-benCHmark Q1/Q6 analytical queries, "
            f"YCSB-E/HTAP scan-latency instrumentation, New-Order-only counters)")
        run(["git", "apply", str(LEANSTORE_PATCH_PATH)], cwd=LEANSTORE_REPO)

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
    if binary.exists():
        print(f"{binary} already built, skipping.")
        return
    if not VWEAVER_REPO.exists():
        run(["git", "clone", VWEAVER_URL, str(VWEAVER_REPO)])
        run(["git", "checkout", VWEAVER_PATCH_COMMIT], cwd=VWEAVER_REPO)
        log(f"Applying {VWEAVER_PATCH_PATH.name} (dead sys/vtimes.h include)")
        run(["git", "apply", str(VWEAVER_PATCH_PATH)], cwd=VWEAVER_REPO)

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

    check = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-tAc", f"SELECT 1 FROM pg_roles WHERE rolname='{PG_ROLE}'"],
        capture_output=True, text=True,
    )
    if check.stdout.strip() == "1":
        print(f"Role '{PG_ROLE}' already exists, skipping.")
    else:
        run(["sudo", "-u", "postgres", "psql", "-c",
             f"CREATE ROLE {PG_ROLE} WITH LOGIN SUPERUSER PASSWORD '{PG_PASSWORD}';"])

    check_db = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-tAc", f"SELECT 1 FROM pg_database WHERE datname='{PG_DATABASE}'"],
        capture_output=True, text=True,
    )
    if check_db.stdout.strip() == "1":
        print(f"Database '{PG_DATABASE}' already exists, skipping.")
    else:
        run(["sudo", "-u", "postgres", "psql", "-c",
             f"CREATE DATABASE {PG_DATABASE} OWNER {PG_ROLE};"])


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
    if BENCHBASE_DIST.exists():
        print(f"{BENCHBASE_DIST} already built, skipping.")
        return

    if not BENCHBASE_REPO.exists():
        run(["git", "clone", "--depth", "1", BENCHBASE_URL, str(BENCHBASE_REPO)])

    _patch_benchbase_pom(BENCHBASE_REPO / "pom.xml")

    # `mvn` (not the bundled ./mvnw wrapper) so build errors show up directly in this
    # console instead of being interleaved with (or hidden by) the wrapper's own
    # download/bootstrap output - and no `-q`, for the same reason: full Maven output
    # visible here to debug plugin/dependency failures on sight.
    # BenchBase's pom.xml targets Java 23; override to whatever JDK is
    # actually installed (verified fine with 21 in prior runs) rather than
    # requiring a JDK 23 install.
    run([
        "mvn", "clean", "package", "-P", "postgres",
        "-DskipTests", "-Dmaven.compiler.source=21", "-Dmaven.compiler.target=21", "-Djava.version=21",
    ], cwd=BENCHBASE_REPO)

    tgz = BENCHBASE_REPO / "target" / "benchbase-postgres.tgz"
    if not tgz.exists():
        sys.exit(f"Build finished but {tgz} is missing - check the Maven output above.")
    run(["tar", "xzf", str(tgz)], cwd=BENCHBASE_REPO / "target")


def step_cmvbt() -> None:
    log("Setting up cMVBT")
    if not CMVBT_WORKSPACE_CLONE.exists():
        log(f"Attempting to clone {CMVBT_OSIC_URL} into {CMVBT_WORKSPACE_CLONE}")
        result = subprocess.run(["git", "clone", CMVBT_OSIC_URL, str(CMVBT_WORKSPACE_CLONE)])
        if result.returncode != 0:
            shutil.rmtree(CMVBT_WORKSPACE_CLONE, ignore_errors=True)  # drop any partial clone
            print(f"Clone failed (cMVBT-OSIC may be private, or this machine may lack "
                  f"access) - falling back to the checkout this script is already part of "
                  f"instead. If you expect access here (e.g. you've since been granted it, "
                  f"or you're on the real server with credentials configured), just re-run "
                  f"this script.")

    active_repo = (
        CMVBT_WORKSPACE_CLONE if (CMVBT_WORKSPACE_CLONE / "Cargo.toml").exists()
        else common.CMVBT_REPO
    )
    log(f"Building cMVBT ({active_repo})")
    run(["cargo", "build", "--release"], cwd=active_repo)


def step_python_venv() -> None:
    log("Setting up the Python plotting venv")
    venv_dir = common.CMVBT_REPO / "scripts" / ".venv"
    pip = venv_dir / "bin" / "pip"
    if pip.exists():
        print(f"{venv_dir} already set up, skipping.")
        return
    run([sys.executable, "-m", "venv", str(venv_dir)])
    run([str(pip), "install", "-q", "-r", str(common.CMVBT_REPO / "scripts" / "requirements.txt")])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--skip-apt", action="store_true")
    parser.add_argument("--skip-wiredtiger", action="store_true")
    parser.add_argument("--skip-leanstore", action="store_true")
    parser.add_argument("--skip-vweaver", action="store_true")
    parser.add_argument("--skip-hugepages", action="store_true")
    parser.add_argument("--skip-postgres", action="store_true")
    parser.add_argument("--skip-benchbase", action="store_true")
    parser.add_argument("--skip-cmvbt", action="store_true")
    parser.add_argument("--skip-venv", action="store_true")
    args = parser.parse_args()

    steps = [
        ("apt", args.skip_apt, step_apt_packages),
        ("wiredtiger", args.skip_wiredtiger, step_wiredtiger),
        ("leanstore", args.skip_leanstore, step_leanstore),
        ("hugepages", args.skip_hugepages, step_vweaver_hugepages),
        ("vweaver", args.skip_vweaver, step_vweaver_ermia),
        ("postgres", args.skip_postgres, step_postgres),
        ("benchbase", args.skip_benchbase, step_benchbase),
        ("cmvbt", args.skip_cmvbt, step_cmvbt),
        ("venv", args.skip_venv, step_python_venv),
    ]

    print("########## cross-engine benchmark environment setup ##########")
    print(f"workspace root: {WORKSPACE_ROOT}")
    for name, skip, fn in steps:
        if skip:
            print(f"\n>>> Skipping {name} (--skip-{name})")
            continue
        try:
            fn()
        except subprocess.CalledProcessError as e:
            sys.exit(f"\nStep '{name}' failed ({e}). Fix the issue above and re-run - "
                     f"earlier steps will be skipped since they're already done.")

    print("\n########## setup complete ##########")
    print(f"Run the comparison with: python3 scripts/compare_engines.py --tiny")
    print("################################################################\n")


if __name__ == "__main__":
    main()
