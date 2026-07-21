#!/usr/bin/env python3
"""Builds everything scripts/compare_engines.py needs: apt dependencies,
a from-source WiredTiger, LeanStore (native + WiredTiger adapter frontends),
PostgreSQL + BenchBase, cMVBT itself, and the Python plotting venv.

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

# Shared with scripts/engines/*.py (single source of truth - see common.py's module docs
# for why these are env-var overridable).
CMVBT_REPO = common.CMVBT_REPO
LEANSTORE_REPO = common.LEANSTORE_REPO
WIREDTIGER_BUILD_DIR = common.WIREDTIGER_BUILD_DIR
WIREDTIGER_REPO = WIREDTIGER_BUILD_DIR.parent
LEANSTORE_BUILD_DIR = LEANSTORE_REPO / "build"
BENCHBASE_DIST = common.BENCHBASE_HOME
BENCHBASE_REPO = BENCHBASE_DIST.parent.parent

# Everything LeanStore's own README asks for, minus librocksdb-dev/liblmdb-dev
# (only needed for the rocksdb_*/lmdb_* frontend targets, which
# scripts/engines/leanstore.py never builds), plus postgresql itself, plus numactl
# (every engine subprocess here runs under `numactl --cpubind=0 --membind=0` - see
# engines/common.py::run_and_track_rss - matching the real 2-NUMA-node server). No
# ninja-build: both cmake builds below go through `cmake --build`, which
# drives whatever generator got configured (default: Unix Makefiles via the
# system `make`, already required anyway) - one less dependency to install.
APT_PACKAGES = [
    "cmake", "libtbb-dev", "libaio-dev", "libsnappy-dev", "zlib1g-dev",
    "libbz2-dev", "liblz4-dev", "libzstd-dev", "liburing-dev", "numactl",
    "postgresql", "postgresql-contrib",
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


def step_wiredtiger() -> None:
    log("Building WiredTiger (from-source, Release, ENABLE_PYTHON=OFF)")
    lib = WIREDTIGER_BUILD_DIR / "libwiredtiger.so"
    if lib.exists():
        print(f"{lib} already exists, skipping.")
        return
    if not WIREDTIGER_REPO.exists():
        sys.exit(f"{WIREDTIGER_REPO} doesn't exist - clone/checkout the wiredtiger "
                  f"repo there first (this script doesn't manage that checkout).")

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


def shutil_cpu_count() -> int:
    import os
    return os.cpu_count() or 4


def step_leanstore() -> None:
    log("Building LeanStore (native tpcc/ycsb + WiredTiger-adapter frontends)")
    targets = ["tpcc", "ycsb", "wiredtiger_tpcc", "wiredtiger_ycsb"]
    binaries = [LEANSTORE_BUILD_DIR / "frontend" / t for t in targets]
    if all(b.exists() for b in binaries):
        print("All 4 frontend binaries already built, skipping.")
        return
    if not LEANSTORE_REPO.exists():
        sys.exit(f"{LEANSTORE_REPO} doesn't exist - clone/checkout the leanstore repo first.")

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
        run(["git", "clone", "--depth", "1", "https://github.com/cmu-db/benchbase.git", str(BENCHBASE_REPO)])

    _patch_benchbase_pom(BENCHBASE_REPO / "pom.xml")

    # BenchBase's pom.xml targets Java 23; override to whatever JDK is
    # actually installed (verified fine with 21 in prior runs) rather than
    # requiring a JDK 23 install.
    run([
        "./mvnw", "-q", "clean", "package", "-P", "postgres",
        "-DskipTests", "-Dmaven.compiler.source=21", "-Dmaven.compiler.target=21", "-Djava.version=21",
    ], cwd=BENCHBASE_REPO)

    tgz = BENCHBASE_REPO / "target" / "benchbase-postgres.tgz"
    if not tgz.exists():
        sys.exit(f"Build finished but {tgz} is missing - check the Maven output above.")
    run(["tar", "xzf", str(tgz)], cwd=BENCHBASE_REPO / "target")


def step_cmvbt() -> None:
    log("Building cMVBT")
    run(["cargo", "build", "--release"], cwd=CMVBT_REPO)


def step_python_venv() -> None:
    log("Setting up the Python plotting venv")
    venv_dir = CMVBT_REPO / "scripts" / ".venv"
    pip = venv_dir / "bin" / "pip"
    if pip.exists():
        print(f"{venv_dir} already set up, skipping.")
        return
    run([sys.executable, "-m", "venv", str(venv_dir)])
    run([str(pip), "install", "-q", "-r", str(CMVBT_REPO / "scripts" / "requirements.txt")])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--skip-apt", action="store_true")
    parser.add_argument("--skip-wiredtiger", action="store_true")
    parser.add_argument("--skip-leanstore", action="store_true")
    parser.add_argument("--skip-postgres", action="store_true")
    parser.add_argument("--skip-benchbase", action="store_true")
    parser.add_argument("--skip-cmvbt", action="store_true")
    parser.add_argument("--skip-venv", action="store_true")
    args = parser.parse_args()

    steps = [
        ("apt", args.skip_apt, step_apt_packages),
        ("wiredtiger", args.skip_wiredtiger, step_wiredtiger),
        ("leanstore", args.skip_leanstore, step_leanstore),
        ("postgres", args.skip_postgres, step_postgres),
        ("benchbase", args.skip_benchbase, step_benchbase),
        ("cmvbt", args.skip_cmvbt, step_cmvbt),
        ("venv", args.skip_venv, step_python_venv),
    ]

    print("########## cross-engine benchmark environment setup ##########")
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
