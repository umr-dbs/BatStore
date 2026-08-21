#!/usr/bin/env python3
"""Remove bulky LeanStore diagnostics from benchmark run directories.

The cross-engine plotters read the run-level ``manifest.csv``. This script also
keeps LeanStore's compact raw measurement CSVs so normalized values can be
audited or recomputed. Everything else below a run's ``leanstore`` directories
is considered disposable engine output.

The default is a dry run. Pass ``--delete`` to actually remove files.
"""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path


# Files consumed by plotters today, or compact raw measurements from which the
# manifest values are derived. Keep this deliberately explicit: LeanStore's
# log_bm.csv and similar profiling tables can be very large.
KEEP_FILES = {
    "log_cr.csv",
    "mem_stats.csv",
    "tpcc_oltp_timeseries.csv",
    "tpcc_scan.csv",
    "ycsb_timeseries.csv",
    "ycsb_scan_latency_summary.csv",
    "s_htap_timeseries.csv",
    "s_htap_scan_latency_summary.csv",
    "s_htap_staleness_summary.csv",
    "ch_query_latency_summary.csv",
}


@dataclass
class Totals:
    files: int = 0
    bytes: int = 0
    leanstore_dirs: int = 0

    def add(self, other: "Totals") -> None:
        self.files += other.files
        self.bytes += other.bytes
        self.leanstore_dirs += other.leanstore_dirs


def human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def is_run_dir(path: Path) -> bool:
    """Recognize complete and interrupted benchmark run directories."""
    return path.is_dir() and (
        (path / "manifest.csv").is_file() or (path / "run_config.json").is_file()
    )


def discover_runs(targets: list[Path]) -> list[Path]:
    runs: set[Path] = set()
    for target in targets:
        target = target.resolve()
        if not target.exists():
            raise SystemExit(f"Target does not exist: {target}")
        if not target.is_dir():
            raise SystemExit(f"Target is not a directory: {target}")
        if is_run_dir(target):
            runs.add(target)
            continue
        for marker_name in ("manifest.csv", "run_config.json"):
            for marker in target.rglob(marker_name):
                if is_run_dir(marker.parent):
                    runs.add(marker.parent.resolve())
    return sorted(runs)


def removable_files(leanstore_dir: Path) -> list[Path]:
    files: list[Path] = []
    for root, dirnames, filenames in os.walk(leanstore_dir, followlinks=False):
        root_path = Path(root)
        # Directory symlinks must be unlinked, never traversed.
        for dirname in list(dirnames):
            path = root_path / dirname
            if path.is_symlink():
                files.append(path)
                dirnames.remove(dirname)
        for filename in filenames:
            path = root_path / filename
            if path.name not in KEEP_FILES:
                files.append(path)
    return files


def prune_empty_dirs(root: Path) -> None:
    directories = [p for p in root.rglob("*") if p.is_dir() and not p.is_symlink()]
    for directory in sorted(directories, key=lambda p: len(p.parts), reverse=True):
        try:
            directory.rmdir()
        except OSError:
            pass


def clean_run(run_dir: Path, delete: bool, verbose: bool) -> Totals:
    totals = Totals()
    leanstore_dirs = sorted(
        path for path in run_dir.rglob("leanstore")
        if path.is_dir() and not path.is_symlink()
    )
    totals.leanstore_dirs = len(leanstore_dirs)

    for leanstore_dir in leanstore_dirs:
        for path in removable_files(leanstore_dir):
            try:
                size = path.lstat().st_size
            except OSError as error:
                print(f"warning: cannot inspect {path}: {error}")
                continue
            totals.files += 1
            totals.bytes += size
            if verbose:
                action = "delete" if delete else "would delete"
                print(f"  {action}: {path} ({human_size(size)})")
            if delete:
                try:
                    path.unlink()
                except OSError as error:
                    raise SystemExit(f"Failed to delete {path}: {error}") from error
        if delete:
            prune_empty_dirs(leanstore_dir)

    mode = "deleted" if delete else "would delete"
    print(
        f"{run_dir}: {mode} {totals.files} file(s), "
        f"{human_size(totals.bytes)} from {totals.leanstore_dirs} LeanStore directory tree(s)"
    )
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "targets", nargs="*", default=[Path("comparison_results")], type=Path,
        help=("Run directory or a root containing run directories "
              "(default: comparison_results)"),
    )
    parser.add_argument(
        "--delete", action="store_true",
        help="Actually delete unneeded files (without this flag, only preview)",
    )
    parser.add_argument("--verbose", action="store_true", help="List each candidate file")
    args = parser.parse_args()

    runs = discover_runs(args.targets)
    if not runs:
        targets = ", ".join(str(path) for path in args.targets)
        raise SystemExit(f"No benchmark run directories found below: {targets}")

    grand_total = Totals()
    for run_dir in runs:
        grand_total.add(clean_run(run_dir, args.delete, args.verbose))

    mode = "Deleted" if args.delete else "Dry run: would delete"
    print(
        f"{mode} {grand_total.files} file(s), {human_size(grand_total.bytes)} "
        f"across {len(runs)} run(s)."
    )
    if not args.delete:
        print("Re-run with --delete to apply the cleanup.")


if __name__ == "__main__":
    main()
