"""Shared build/env plumbing for the leanstore.py and wiredtiger.py engine
wrappers - both drive binaries from the same LeanStore CMake build
(`build/frontend/{tpcc,ycsb,wiredtiger_tpcc,wiredtiger_ycsb}`), which links
against a from-source WiredTiger build kept in a sibling repo.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

LEANSTORE_REPO = Path("/home/amir/CLionProjects/leanstore")
BUILD_DIR = LEANSTORE_REPO / "build"
WIREDTIGER_BUILD_DIR = Path("/home/amir/CLionProjects/wiredtiger/cmake-build-debug")


def ensure_built(targets=("tpcc", "ycsb", "wiredtiger_tpcc", "wiredtiger_ycsb")) -> None:
    subprocess.run(["make", "-j", str(os.cpu_count() or 4), *targets], cwd=BUILD_DIR, check=True)


def run_env() -> dict:
    """Environment with WiredTiger's shared library on LD_LIBRARY_PATH."""
    env = os.environ.copy()
    existing = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = f"{WIREDTIGER_BUILD_DIR}:{existing}" if existing else str(WIREDTIGER_BUILD_DIR)
    return env


def binary(name: str) -> Path:
    return BUILD_DIR / "frontend" / name


def ycsb_gflags(letter: str, max_scan_length: int = 100) -> list:
    """Maps standard YCSB A-F onto the read/insert/scan/rmw-ratio gflags added
    to frontend/ycsb/{ycsb,wiredtiger_ycsb}.cpp. D approximates YCSB's
    "latest" key distribution with the same Zipfian generator used
    everywhere else here (LeanStore has no separate recency-biased
    generator) - a documented approximation, not a faithful reproduction.
    """
    mapping = {
        "a": ["--ycsb_read_ratio=50"],
        "b": ["--ycsb_read_ratio=95"],
        "c": ["--ycsb_read_ratio=100"],
        "d": ["--ycsb_read_ratio=100", "--ycsb_insert_ratio=5"],
        "e": ["--ycsb_scan_ratio=95", "--ycsb_insert_ratio=5", f"--ycsb_max_scan_length={max_scan_length}"],
        "f": ["--ycsb_read_ratio=50", "--ycsb_rmw_ratio=50"],
    }
    return mapping[letter]
