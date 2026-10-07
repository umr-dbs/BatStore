#!/usr/bin/env python3
"""Run the complete H1-H6 paper experiment suite, then regenerate all plots.

The focused experiment drivers remain the source of truth for the individual
measurements.  This script only gives them one reproducible execution order,
one result collection, fail-fast behavior, and one final plotting pass.

Full paper run:

    python3 scripts/run_paper_experiments.py

Short end-to-end validation:

    python3 scripts/run_paper_experiments.py --quick
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import platform
import shlex
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
EXPERIMENTS = tuple(f"h{number}" for number in range(1, 7))

# Keep the quick profile large enough to exercise every workload and plotting
# path while making it explicit that these settings are not paper measurements.
QUICK_ARGUMENTS = {
    "h1": ["--threads", "1", "--records", "10000", "--duration", "10"],
    "h2": ["--threads", "1", "--skews", "uniform,0.99", "--records", "10000", "--duration", "10"],
    "h3": ["--engines", "batstore", "--warehouses", "1", "--terminals", "1", "--duration", "10", "--buckets", "1"],
    "h4": ["--warehouses", "1", "--duration", "10", "--olap-threads", "0,1", "--fixed-oltp-terminals", "1", "--scan-pool-workers", "0"],
    "h5": ["--warehouses", "1", "--duration", "10", "--oltp-terminals", "1", "--fixed-olap-threads", "1", "--scan-pool-workers", "0"],
    "h6": ["--threads", "1", "--records", "10000", "--duration", "10"],
}


def parse_experiments(value: str) -> list[str]:
    selected = list(dict.fromkeys(part.strip().lower() for part in value.split(",") if part.strip()))
    unknown = sorted(set(selected) - set(EXPERIMENTS))
    if not selected or unknown:
        expected = ",".join(EXPERIMENTS)
        detail = f"; unknown: {','.join(unknown)}" if unknown else ""
        raise argparse.ArgumentTypeError(f"expected a comma-separated subset of {expected}{detail}")
    return selected


def default_collection_dir(output_root: Path, now: dt.datetime | None = None) -> Path:
    timestamp = (now or dt.datetime.now()).strftime("%Y%m%d_%H%M%S")
    return output_root / f"run_{timestamp}"


def experiment_command(
    experiment: str,
    collection_dir: Path,
    *,
    quick: bool,
    skip_build: bool,
) -> list[str]:
    command = [
        sys.executable,
        str(SCRIPTS_DIR / f"{experiment}.py"),
        "--output-root",
        str(collection_dir / f"{experiment}_results"),
        "--compact",
    ]
    if quick:
        command.extend(QUICK_ARGUMENTS[experiment])
    if skip_build:
        command.append("--skip-build")
    return command


def plot_command(collection_dir: Path) -> list[str]:
    return [
        sys.executable,
        str(SCRIPTS_DIR / "plot.py"),
        str(collection_dir),
        "--kind",
        "hypotheses",
        "--compact",
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--output-root", type=Path, default=Path("paper_results"),
        help="parent directory for timestamped paper runs (default: paper_results)",
    )
    parser.add_argument(
        "--only", type=parse_experiments, default=list(EXPERIMENTS), metavar="H1,H2,...",
        help="run only the listed experiments (default: h1,h2,h3,h4,h5,h6)",
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="short validation profile; verifies the workflow but does not produce paper measurements",
    )
    parser.add_argument(
        "--skip-build", action="store_true",
        help="reuse already-built benchmark binaries in every experiment",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="print the commands and write no results",
    )
    return parser.parse_args()


def printable(command: list[str]) -> str:
    return shlex.join(command)


def main() -> None:
    args = parse_args()
    collection_dir = default_collection_dir(args.output_root.resolve())
    experiment_commands = [
        experiment_command(
            experiment,
            collection_dir,
            quick=args.quick,
            skip_build=args.skip_build,
        )
        for experiment in args.only
    ]
    commands = experiment_commands + [plot_command(collection_dir)]

    print("Paper experiment plan")
    print(f"  profile : {'quick validation (not paper measurements)' if args.quick else 'full paper'}")
    print(f"  suites  : {', '.join(name.upper() for name in args.only)}")
    print(f"  output  : {collection_dir}")
    for command in commands:
        print(f"  $ {printable(command)}")

    if args.dry_run:
        return

    collection_dir.mkdir(parents=True, exist_ok=False)
    record = {
        "schema_version": 1,
        "profile": "quick" if args.quick else "full",
        "experiments": args.only,
        "repository": str(REPO_ROOT),
        "python": sys.version,
        "platform": platform.platform(),
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "commands": [printable(command) for command in commands],
    }
    record_path = collection_dir / "paper_run.json"
    record_path.write_text(json.dumps(record, indent=2) + "\n")

    try:
        for index, (experiment, command) in enumerate(zip(args.only, experiment_commands), start=1):
            print(f"\n[{index}/{len(experiment_commands)}] Running {experiment.upper()}", flush=True)
            subprocess.run(command, cwd=REPO_ROOT, check=True)

        print("\nAll experiments completed; regenerating individual and overview plots.", flush=True)
        subprocess.run(plot_command(collection_dir), cwd=REPO_ROOT, check=True)
    except (KeyboardInterrupt, subprocess.CalledProcessError) as exc:
        record["status"] = "interrupted" if isinstance(exc, KeyboardInterrupt) else "failed"
        record["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        if isinstance(exc, subprocess.CalledProcessError):
            record["failed_command"] = printable(list(exc.cmd))
            record["returncode"] = exc.returncode
        record_path.write_text(json.dumps(record, indent=2) + "\n")
        if isinstance(exc, KeyboardInterrupt):
            raise SystemExit("Interrupted; completed experiment outputs were kept.") from exc
        raise SystemExit(
            f"Paper run stopped after a failed command (exit {exc.returncode}). "
            f"Completed outputs were kept in {collection_dir}."
        ) from exc

    record["status"] = "complete"
    record["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    record_path.write_text(json.dumps(record, indent=2) + "\n")
    print(f"\nComplete paper results: {collection_dir}")
    print(f"Combined plots: {collection_dir / 'plots'}")


if __name__ == "__main__":
    main()
