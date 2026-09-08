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
    check_worker_log(output_dir)


def check_worker_log(output_dir: Path) -> None:
    log = output_dir / "stdout.log"
    if log.exists() and "clamping" in log.read_text(errors="replace"):
        raise SystemExit(f"{log}: driver clamped concurrency; select fewer threads for this machine")
