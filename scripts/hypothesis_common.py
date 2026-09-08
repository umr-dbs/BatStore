"""Setup and input validation shared by the focused H1–H6 experiments."""
from pathlib import Path
import os


def configure_checkout() -> None:
    # Must run before importing engines.common, which otherwise prefers tx_tests.
    os.environ.setdefault("BATSTORE_REPO", str(Path(__file__).resolve().parent.parent))


def thread_counts(value: str, *, allow_zero: bool = False) -> list[int]:
    try:
        counts = list(dict.fromkeys(int(part.strip()) for part in value.split(",")))
    except ValueError:
        raise SystemExit("thread counts must be a comma-separated list of integers")
    if not counts or any(t < (0 if allow_zero else 1) for t in counts):
        raise SystemExit("thread counts must be positive (H4 also accepts zero)")
    return counts


def check_run(result, output_dir: Path) -> None:
    """Do not plot failed or silently clamped runs as successful measurements."""
    if result.notes and ("FAILED" in result.notes or "TIMEOUT" in result.notes):
        raise SystemExit(f"{output_dir}: {result.notes}")
    import math
    if not math.isfinite(result.primary_metric_value) or result.primary_metric_value <= 0:
        raise SystemExit(f"{output_dir}: no positive throughput measurement; inspect stdout.log")
    check_worker_log(output_dir)


def check_worker_log(output_dir: Path) -> None:
    log = output_dir / "stdout.log"
    if log.exists() and "clamping" in log.read_text(errors="replace"):
        raise SystemExit(f"{log}: driver clamped concurrency; select fewer threads for this machine")


def positive_int(value: str) -> int:
    import argparse
    try:
        result = int(value)
        if result > 0:
            return result
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("must be a positive integer")


def nonnegative_int(value: str) -> int:
    import argparse
    try:
        result = int(value)
        if result >= 0:
            return result
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("must be a nonnegative integer")


def read_op_latency(csv_path: Path, operation: str) -> dict:
    """Require measured latency rather than converting missing data to zero."""
    import csv
    import math
    try:
        with csv_path.open(newline="") as f:
            for row in csv.DictReader(f):
                if row.get("operation") == operation:
                    result = {key: float(row[f"{key}_us"]) for key in ("p50", "p95", "p99", "avg")}
                    result["count"] = int(row["count"])
                    if result["count"] <= 0 or any(not math.isfinite(v) or v < 0 for v in result.values()):
                        raise ValueError("missing or invalid latency samples")
                    return result
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise SystemExit(f"{csv_path}: invalid {operation} latency: {exc}")
    raise SystemExit(f"{csv_path}: no latency samples for {operation}")


def record_setup(run_dir: Path, hypothesis: str, args, *, varies: str, fixed: str, measures: str) -> None:
    """Save the settings and interpretation beside each run's results."""
    import json
    run_dir.mkdir(parents=True, exist_ok=True)
    details = dict(hypothesis=hypothesis, arguments=vars(args), varies=varies,
                   fixed=fixed, measures=measures, repository=os.environ["BATSTORE_REPO"],
                   allocator=os.environ.get("BATSTORE_ALLOCATOR", os.environ.get("CMVBT_ALLOCATOR", "jemalloc")))
    (run_dir / "configuration.json").write_text(json.dumps(details, indent=2) + "\n")
    print(f"{hypothesis} | vary: {varies}\nfixed: {fixed}\nmeasure: {measures}")
    print("Execution success does not imply support for the hypothesis.")


def check_scan_samples(result, output_dir: Path) -> None:
    import math
    if result.scan_count <= 0 or any(not math.isfinite(v) or v <= 0 for v in
                                    (result.scan_p50_us, result.scan_p95_us, result.scan_p99_us)):
        raise SystemExit(f"{output_dir}: no valid analytical query latency samples; increase duration or inspect stdout.log")
