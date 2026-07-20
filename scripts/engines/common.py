"""Shared types and helpers for the cross-engine benchmark harness.

Normalizes cMVBT/LeanStore/WiredTiger/PostgreSQL onto one comparable schema
(engine,workload,config_label,duration_secs,primary_metric_name,
primary_metric_value,peak_rss_mb,notes) so a single manifest.csv + plotting
script can overlay all four. See /home/amir/.claude/plans/iterative-spinning-origami.md
for the full design.
"""
from __future__ import annotations

import csv
import dataclasses
import subprocess
import threading
from pathlib import Path
from typing import Optional


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
    notes: str = ""


MANIFEST_HEADER = [
    "engine", "workload", "config_label", "duration_secs",
    "primary_metric_name", "primary_metric_value", "peak_rss_mb", "notes",
]

YCSB_WORKLOADS = ["ycsb_a", "ycsb_b", "ycsb_c", "ycsb_d", "ycsb_e", "ycsb_f"]
ALL_WORKLOADS = ["tpcc"] + YCSB_WORKLOADS
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
    """Runs `cmd` to completion, sampling peak RSS via /proc every 0.5s.

    Returns (returncode, peak_rss_mb). stdout+stderr are merged and written to
    stdout_path if given (for post-hoc debugging), else discarded.
    """
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


def write_manifest_header(manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    if not manifest_path.exists():
        with open(manifest_path, "w", newline="") as f:
            csv.writer(f).writerow(MANIFEST_HEADER)


def append_manifest_row(manifest_path: Path, result: NormalizedResult) -> None:
    with open(manifest_path, "a", newline="") as f:
        csv.writer(f).writerow([
            result.engine, result.workload, result.config_label, f"{result.duration_secs:.1f}",
            result.primary_metric_name, f"{result.primary_metric_value:.3f}",
            f"{result.peak_rss_mb:.2f}", result.notes,
        ])
