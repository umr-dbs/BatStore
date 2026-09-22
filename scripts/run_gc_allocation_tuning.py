#!/usr/bin/env python3
"""Run, plot, and select BatStore GC allocation parameters.

The script runs two sequential experiments:
  1. maximum non-local worker queues searched per allocation request;
  2. maximum reclaim/fresh-allocation batch size.

Example (full experiment):
    python3 scripts/run_gc_allocation_tuning.py

Small smoke run:
    python3 scripts/run_gc_allocation_tuning.py \
        --threads 2,4 --neighbors 0,1,all --batches 1,4 \
        --duration 2 --repeats 1 --records 20000

Regenerate summaries, plots, and the recommendation from an existing run:
    python3 scripts/run_gc_allocation_tuning.py \
        --plot-only --output experiments/gc_allocation_tuning_YYYYMMDD_HHMMSS
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import random
import re
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_NEIGHBORS = ["0", "1", "2", "4", "8", "16", "32", "all"]
DEFAULT_BATCHES = [1, 2, 4, 8, 16, 32, 64]
GC_SUM_COUNTERS = [
    "local_reuse",
    "steal",
    "fresh_alloc",
    "request_count",
    "latency_ns",
    "scan_count",
    "lists_checked",
]
GC_MAX_COUNTERS = ["latency_max_ns", "lists_checked_max"]
MEASUREMENT_FIELDS = [
    "experiment",
    "repeat",
    "threads",
    "max_neighbors",
    "batch_size",
    "throughput_ops_sec",
    "read_p99_us",
    "update_p50_us",
    "update_p95_us",
    "update_p99_us",
    "block_requests",
    "block_latency_mean_us",
    "whole_run_block_latency_max_us",
    "reclaim_searches",
    "neighbors_checked_mean",
    "whole_run_neighbors_checked_max",
    "local_reuse",
    "steal",
    "fresh_alloc",
    "local_reuse_share",
    "steal_share",
    "fresh_alloc_share",
    "peak_rss_mb",
]
METRIC_FIELDS = MEASUREMENT_FIELDS[5:]


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def parse_int_list(value: str, *, allow_zero: bool = False) -> list[int]:
    try:
        values = sorted(set(int(item.strip()) for item in value.split(",") if item.strip()))
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected a comma-separated integer list") from error
    minimum = 0 if allow_zero else 1
    if not values or values[0] < minimum:
        raise argparse.ArgumentTypeError(f"values must be >= {minimum}")
    return values


def affinity_cpus() -> set[int]:
    try:
        return set(os.sched_getaffinity(0))
    except AttributeError:
        return set(range(os.cpu_count() or 1))


def physical_core_count(cpus: set[int]) -> int:
    cores: set[tuple[str, str]] = set()
    for cpu in cpus:
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        try:
            package = (topology / "physical_package_id").read_text().strip()
            core = (topology / "core_id").read_text().strip()
        except OSError:
            continue
        cores.add((package, core))
    return len(cores) if cores else max(1, len(cpus) // 2)


def command_output(command: list[str], cwd: Path | None = None) -> str:
    try:
        return subprocess.run(
            command, cwd=cwd, check=True, capture_output=True, text=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"


def cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return "unavailable"


def parse_threads(value: str, physical: int, logical: int) -> list[int]:
    if value == "auto":
        return sorted(set([min(8, logical), min(physical, logical), logical]))
    return parse_int_list(value)


def parse_neighbors(value: str, shard_count: int) -> list[int]:
    maximum = shard_count - 1
    parsed: set[int] = set()
    for item in value.split(","):
        item = item.strip().lower()
        if not item:
            continue
        if item == "all":
            parsed.add(maximum)
            continue
        try:
            number = int(item)
        except ValueError as error:
            raise argparse.ArgumentTypeError(
                "neighbors must be comma-separated non-negative integers or 'all'"
            ) from error
        if number < 0:
            raise argparse.ArgumentTypeError("neighbor counts must be non-negative")
        parsed.add(min(number, maximum))
    if not parsed:
        raise argparse.ArgumentTypeError("at least one neighbor count is required")
    return sorted(parsed)


def read_gc_stats(path: Path) -> dict[str, int]:
    try:
        with path.open(newline="") as file:
            rows = list(csv.DictReader(file))
    except OSError as error:
        raise RuntimeError(f"cannot read {path}: {error}") from error
    if not rows or any(row.get("schema_version") != "2" for row in rows):
        raise RuntimeError(f"{path} is missing gc-stats schema 2 data")
    try:
        summed = {key: sum(int(row[key]) for row in rows) for key in GC_SUM_COUNTERS}
        maxima = {key: max(int(row[key]) for row in rows) for key in GC_MAX_COUNTERS}
    except (KeyError, ValueError) as error:
        raise RuntimeError(f"invalid GC statistics in {path}: {error}") from error
    return {**summed, **maxima}


def read_operation_latency(path: Path, operation: str) -> dict[str, float]:
    with path.open(newline="") as file:
        for row in csv.DictReader(file):
            if row["operation"] == operation:
                return {key: float(row[key]) for key in ("p50_us", "p95_us", "p99_us")}
    raise RuntimeError(f"{path} has no {operation!r} row")


def peak_rss_mb(path: Path) -> float:
    with path.open(newline="") as file:
        values = [float(row["rss_kb"]) / 1024.0 for row in csv.DictReader(file)]
    if not values:
        raise RuntimeError(f"{path} has no memory samples")
    return max(values)


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def append_measurement(path: Path, row: dict) -> None:
    new_file = not path.exists()
    with path.open("a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=MEASUREMENT_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow(row)
        file.flush()


def bootstrap_median_ci(values: list[float], seed: int) -> tuple[float, float]:
    if len(values) == 1:
        return values[0], values[0]
    rng = random.Random(seed)
    estimates = sorted(
        statistics.median(rng.choices(values, k=len(values))) for _ in range(4000)
    )
    return estimates[math.floor(0.025 * (len(estimates) - 1))], estimates[
        math.ceil(0.975 * (len(estimates) - 1))
    ]


def summarize(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        key = (
            row["experiment"],
            int(row["threads"]),
            int(row["max_neighbors"]),
            int(row["batch_size"]),
        )
        groups[key].append(row)

    summaries = []
    for index, (key, samples) in enumerate(sorted(groups.items())):
        experiment, threads, neighbors, batch = key
        throughputs = [float(row["throughput_ops_sec"]) for row in samples]
        ci_low, ci_high = bootstrap_median_ci(throughputs, 20260922 + index)
        summary = {
            "experiment": experiment,
            "threads": threads,
            "max_neighbors": neighbors,
            "batch_size": batch,
            "repeats": len(samples),
            "throughput_ci_low": ci_low,
            "throughput_ci_high": ci_high,
        }
        summary.update(
            {
                f"{metric}_median": statistics.median(float(row[metric]) for row in samples)
                for metric in METRIC_FIELDS
            }
        )
        summaries.append(summary)
    return summaries


def intervals_overlap(left: dict, right: dict) -> bool:
    return not (
        float(left["throughput_ci_high"]) < float(right["throughput_ci_low"])
        or float(right["throughput_ci_high"]) < float(left["throughput_ci_low"])
    )


def guardrail(value: float, reference: float, factor: float = 1.05) -> bool:
    return reference <= 0 or value <= reference * factor


def choose_setting(
    summaries: list[dict], experiment: str, target_threads: int
) -> tuple[int, str, dict[int, list[int]]]:
    relevant = [row for row in summaries if row["experiment"] == experiment]
    by_threads: dict[int, list[dict]] = defaultdict(list)
    for row in relevant:
        by_threads[int(row["threads"])].append(row)
    if not by_threads:
        raise RuntimeError(f"no completed {experiment} measurements")

    eligible: dict[int, list[int]] = {}
    setting_field = "max_neighbors" if experiment == "neighbors" else "batch_size"
    for threads, candidates in by_threads.items():
        best = max(candidates, key=lambda row: float(row["throughput_ops_sec_median"]))
        best_throughput = float(best["throughput_ops_sec_median"])
        best_update_p99 = float(best["update_p99_us_median"])
        best_rss = float(best["peak_rss_mb_median"])
        eligible[threads] = sorted(
            int(row[setting_field])
            for row in candidates
            if float(row["throughput_ops_sec_median"]) >= 0.99 * best_throughput
            and intervals_overlap(row, best)
            and guardrail(float(row["update_p99_us_median"]), best_update_p99)
            and guardrail(float(row["peak_rss_mb_median"]), best_rss)
        )

    common = set.intersection(*(set(values) for values in eligible.values()))
    if common:
        return min(common), "smallest eligible value at every tested concurrency", eligible

    actual_target = min(by_threads, key=lambda value: abs(value - target_threads))
    if eligible[actual_target]:
        return (
            min(eligible[actual_target]),
            f"no value was eligible everywhere; selected for {actual_target}-worker target",
            eligible,
        )

    best = max(
        by_threads[actual_target], key=lambda row: float(row["throughput_ops_sec_median"])
    )
    return (
        int(best[setting_field]),
        f"no candidate passed all guardrails; throughput winner at {actual_target} workers",
        eligible,
    )


def load_measurements(path: Path) -> list[dict]:
    with path.open(newline="") as file:
        rows = list(csv.DictReader(file))
    if not rows:
        raise RuntimeError(f"{path} contains no measurements")
    return rows


def plot_experiment(summaries: list[dict], experiment: str, output: Path) -> None:
    rows = [row for row in summaries if row["experiment"] == experiment]
    if not rows:
        return
    x_field = "max_neighbors" if experiment == "neighbors" else "batch_size"
    x_label = "Maximum non-local queues searched" if experiment == "neighbors" else "Maximum batch size"
    panels = [
        ("throughput_ops_sec_median", 1e6, "Throughput", "Million ops/s"),
        ("update_p99_us_median", 1.0, "Update tail latency", "p99 us"),
        ("fresh_alloc_share_median", 1.0, "Fresh allocation share", "% of block requests"),
        (
            "neighbors_checked_mean_median" if experiment == "neighbors" else "peak_rss_mb_median",
            1.0,
            "Actual neighbour search" if experiment == "neighbors" else "Peak resident memory",
            "Mean neighbours/search" if experiment == "neighbors" else "MiB",
        ),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(13, 8), constrained_layout=True)
    for ax, (metric, scale, title, ylabel) in zip(axes.flat, panels):
        for threads in sorted({int(row["threads"]) for row in rows}):
            series = sorted(
                (row for row in rows if int(row["threads"]) == threads),
                key=lambda row: int(row[x_field]),
            )
            x = [int(row[x_field]) for row in series]
            y = [float(row[metric]) / scale for row in series]
            if metric == "throughput_ops_sec_median":
                lower = [
                    (float(row[metric]) - float(row["throughput_ci_low"])) / scale
                    for row in series
                ]
                upper = [
                    (float(row["throughput_ci_high"]) - float(row[metric])) / scale
                    for row in series
                ]
                ax.errorbar(x, y, yerr=[lower, upper], marker="o", capsize=3, label=f"{threads} workers")
            else:
                ax.plot(x, y, marker="o", label=f"{threads} workers")
        ax.set_title(title, loc="left", weight="bold")
        ax.set_xlabel(x_label)
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", color="#d9e0e6", linewidth=0.7)
        ax.spines[["top", "right"]].set_visible(False)
    axes[0, 0].legend(frameon=False)
    fig.suptitle(
        "GC neighbour search sweep" if experiment == "neighbors" else "GC allocation batch sweep",
        fontsize=14,
    )
    stem = "neighbors" if experiment == "neighbors" else "batch_size"
    fig.savefig(output / f"{stem}_sweep.png", dpi=180)
    fig.savefig(output / f"{stem}_sweep.pdf")
    plt.close(fig)


def write_recommendation(
    output: Path,
    summaries: list[dict],
    selected_neighbors: int,
    neighbor_reason: str,
    selected_batch: int,
    batch_reason: str,
    neighbor_eligible: dict[int, list[int]],
    batch_eligible: dict[int, list[int]],
    target_threads: int,
) -> None:
    recommendation = {
        "max_neighbors": selected_neighbors,
        "batch_size": selected_batch,
        "target_threads": target_threads,
        "selection_rule": (
            "smallest value within 1% of the best median throughput, with overlapping "
            "bootstrap 95% throughput intervals, update p99 <= 105% of the throughput "
            "winner, and peak RSS <= 105% of the throughput winner"
        ),
        "neighbor_reason": neighbor_reason,
        "batch_reason": batch_reason,
        "eligible_neighbors_by_threads": neighbor_eligible,
        "eligible_batches_by_threads": batch_eligible,
    }
    (output / "recommendation.json").write_text(json.dumps(recommendation, indent=2) + "\n")

    nearest_target = min({int(row["threads"]) for row in summaries}, key=lambda n: abs(n - target_threads))
    lines = [
        "# GC allocation tuning recommendation",
        "",
        f"- Maximum neighbours: **{selected_neighbors}** ({neighbor_reason}).",
        f"- Maximum batch size: **{selected_batch}** ({batch_reason}).",
        f"- Requested target concurrency: {target_threads}; nearest measured concurrency: {nearest_target}.",
        "",
        "The selector chooses the smallest statistically equivalent throughput setting and rejects",
        "candidates whose update p99 latency or peak RSS is more than 5% above the throughput winner.",
        "Treat this as the best setting for the tested YCSB A workload and machine, then confirm the",
        "pair with longer runs and another workload before making it a universal default.",
        "",
        "## Eligible values",
        "",
        "| Workers | Neighbours | Batch sizes |",
        "| ---: | --- | --- |",
    ]
    for threads in sorted(set(neighbor_eligible) | set(batch_eligible)):
        neighbors = ", ".join(map(str, neighbor_eligible.get(threads, []))) or "none"
        batches = ", ".join(map(str, batch_eligible.get(threads, []))) or "none"
        lines.append(f"| {threads} | {neighbors} | {batches} |")
    (output / "recommendation.md").write_text("\n".join(lines) + "\n")


def analyze(output: Path, target_threads: int) -> tuple[int, int]:
    rows = load_measurements(output / "measurements.csv")
    summaries = summarize(rows)
    summary_fields = list(summaries[0])
    write_csv(output / "summary.csv", summaries, summary_fields)
    selected_neighbors, neighbor_reason, neighbor_eligible = choose_setting(
        summaries, "neighbors", target_threads
    )
    batch_rows = [row for row in summaries if row["experiment"] == "batch"]
    selected_batch = 0
    if batch_rows:
        selected_batch, batch_reason, batch_eligible = choose_setting(
            summaries, "batch", target_threads
        )
        write_recommendation(
            output,
            summaries,
            selected_neighbors,
            neighbor_reason,
            selected_batch,
            batch_reason,
            neighbor_eligible,
            batch_eligible,
            target_threads,
        )
    plot_experiment(summaries, "neighbors", output)
    plot_experiment(summaries, "batch", output)
    return selected_neighbors, selected_batch


def execute_run(
    binary: Path,
    run_dir: Path,
    records: int,
    threads: int,
    duration: int,
    neighbors: int,
    batch: int,
) -> dict:
    run_dir.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ)
    env["BATSTORE_GC_MAX_NEIGHBORS"] = str(neighbors)
    env["BATSTORE_GC_BATCH_SIZE"] = str(batch)
    command = [
        str(binary),
        "ycsb",
        "a",
        str(records),
        str(threads),
        str(duration),
        "zipfian",
        "0.99",
        "10",
        "100",
        "100",
        "fg",
        "true",
        "false",
        "false",
        str(run_dir / "wal.log"),
        "5",
        "false",
        "true",
        "atomic",
        "0",
        "0",
    ]
    log_path = run_dir / "run.log"
    with log_path.open("w") as log:
        completed = subprocess.run(
            command,
            cwd=run_dir,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=max(120, duration * 5),
        )
    if completed.returncode != 0:
        raise RuntimeError(f"benchmark failed with exit {completed.returncode}: {log_path}")

    text = log_path.read_text()
    match = re.search(r"throughput \(ops/sec\)\s+([\d.]+)", text)
    if not match:
        raise RuntimeError(f"throughput missing from {log_path}")
    before = read_gc_stats(run_dir / "gc_stats_after_load.csv")
    after = read_gc_stats(run_dir / "gc_stats.csv")
    delta = {key: after[key] - before[key] for key in GC_SUM_COUNTERS}
    if any(value < 0 for value in delta.values()):
        raise RuntimeError(f"GC counters went backwards in {run_dir}")
    requests = delta["request_count"]
    sources = delta["local_reuse"] + delta["steal"] + delta["fresh_alloc"]
    if requests != sources:
        raise RuntimeError(f"allocation sources ({sources}) != requests ({requests}) in {run_dir}")
    searches = delta["scan_count"]
    update = read_operation_latency(run_dir / "ycsb_operation_latency_summary.csv", "update")
    read = read_operation_latency(run_dir / "ycsb_operation_latency_summary.csv", "read")
    return {
        "throughput_ops_sec": float(match.group(1)),
        "read_p99_us": read["p99_us"],
        "update_p50_us": update["p50_us"],
        "update_p95_us": update["p95_us"],
        "update_p99_us": update["p99_us"],
        "block_requests": requests,
        "block_latency_mean_us": delta["latency_ns"] / requests / 1000.0 if requests else 0.0,
        "whole_run_block_latency_max_us": after["latency_max_ns"] / 1000.0,
        "reclaim_searches": searches,
        "neighbors_checked_mean": max(delta["lists_checked"] - searches, 0) / searches if searches else 0.0,
        "whole_run_neighbors_checked_max": max(after["lists_checked_max"] - 1, 0),
        "local_reuse": delta["local_reuse"],
        "steal": delta["steal"],
        "fresh_alloc": delta["fresh_alloc"],
        "local_reuse_share": 100.0 * delta["local_reuse"] / requests if requests else 0.0,
        "steal_share": 100.0 * delta["steal"] / requests if requests else 0.0,
        "fresh_alloc_share": 100.0 * delta["fresh_alloc"] / requests if requests else 0.0,
        "peak_rss_mb": peak_rss_mb(run_dir / "mem_stats.csv"),
    }


def run_sweep(
    experiment: str,
    settings: list[int],
    threads_values: list[int],
    repeats: int,
    fixed_neighbors: int,
    fixed_batch: int,
    args: argparse.Namespace,
    output: Path,
    measurements_path: Path,
) -> None:
    combinations = [
        (repeat, threads, setting)
        for repeat in range(1, repeats + 1)
        for threads in threads_values
        for setting in settings
    ]
    random.Random(args.seed + (0 if experiment == "neighbors" else 1)).shuffle(combinations)
    total = len(combinations)
    for index, (repeat, threads, setting) in enumerate(combinations, 1):
        neighbors = setting if experiment == "neighbors" else fixed_neighbors
        batch = fixed_batch if experiment == "neighbors" else setting
        run_dir = output / experiment / f"r{repeat}_t{threads}_n{neighbors}_b{batch}"
        print(
            f"[{experiment} {index}/{total}] repeat={repeat} workers={threads} "
            f"neighbors={neighbors} batch={batch}",
            flush=True,
        )
        metrics = execute_run(
            args.binary, run_dir, args.records, threads, args.duration, neighbors, batch
        )
        row = {
            "experiment": experiment,
            "repeat": repeat,
            "threads": threads,
            "max_neighbors": neighbors,
            "batch_size": batch,
            **metrics,
        }
        append_measurement(measurements_path, row)
        print(
            f"  {metrics['throughput_ops_sec'] / 1e6:.3f} Mops/s, "
            f"update p99={metrics['update_p99_us']:.1f} us, "
            f"fresh={metrics['fresh_alloc_share']:.3f}%, "
            f"RSS={metrics['peak_rss_mb']:.1f} MiB",
            flush=True,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--duration", type=positive_int, default=60, help="measured seconds per run")
    parser.add_argument("--repeats", type=positive_int, default=5)
    parser.add_argument("--records", type=positive_int, default=2_000_000)
    parser.add_argument(
        "--threads", default="auto", help="comma-separated worker counts, or auto (default)"
    )
    parser.add_argument("--target-threads", type=positive_int, help="concurrency used for fallback selection")
    parser.add_argument("--neighbors", default=",".join(DEFAULT_NEIGHBORS))
    parser.add_argument("--batches", default=",".join(map(str, DEFAULT_BATCHES)))
    parser.add_argument("--fixed-batch", type=positive_int, default=4, help="batch used in experiment 1")
    parser.add_argument("--seed", type=int, default=20260922)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "experiments" / f"gc_allocation_tuning_{dt.datetime.now():%Y%m%d_%H%M%S}",
    )
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/batstore")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--plot-only", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output = args.output.resolve()
    cpus = affinity_cpus()
    logical = len(cpus)
    physical = physical_core_count(cpus)

    if args.plot_only:
        settings_path = args.output / "settings.json"
        if not settings_path.exists():
            raise SystemExit(f"missing {settings_path}")
        settings = json.loads(settings_path.read_text())
        target = int(settings["target_threads"])
        selected_neighbors, selected_batch = analyze(args.output, target)
        print(f"Plots refreshed in {args.output}")
        print(f"Recommended: max_neighbors={selected_neighbors}, batch_size={selected_batch}")
        return

    threads_values = parse_threads(args.threads, physical, logical)
    if max(threads_values) > logical:
        raise SystemExit(
            f"requested {max(threads_values)} workers but process affinity exposes {logical} CPUs"
        )
    neighbors = parse_neighbors(args.neighbors, logical)
    batches = parse_int_list(args.batches)
    target_threads = args.target_threads or physical
    args.binary = args.binary.resolve()
    args.output.mkdir(parents=True, exist_ok=False)

    settings = {
        "duration_seconds": args.duration,
        "repeats": args.repeats,
        "records": args.records,
        "threads": threads_values,
        "target_threads": target_threads,
        "logical_cpus_in_affinity": logical,
        "physical_cores_in_affinity": physical,
        "cpu_affinity": sorted(cpus),
        "cpu_model": cpu_model(),
        "git_commit": command_output(["git", "rev-parse", "HEAD"], ROOT),
        "rustc_version": command_output(["rustc", "--version"]),
        "invocation": [sys.executable, *sys.argv],
        "neighbor_counts": neighbors,
        "batch_sizes": batches,
        "experiment_1_fixed_batch": args.fixed_batch,
        "seed": args.seed,
        "workload": "YCSB A",
        "distribution": "zipfian",
        "theta": 0.99,
        "execution_mode": "atomic",
        "gc": True,
        "idle_compaction": False,
        "binary": str(args.binary),
    }
    (args.output / "settings.json").write_text(json.dumps(settings, indent=2) + "\n")

    if not args.skip_build:
        subprocess.run(
            ["cargo", "build", "--release", "--offline", "--features", "gc-stats"],
            cwd=ROOT,
            check=True,
        )
    if not args.binary.exists():
        raise SystemExit(f"benchmark binary does not exist: {args.binary}")

    total_runs = args.repeats * len(threads_values) * (len(neighbors) + len(batches))
    print(f"Output: {args.output}")
    print(f"Workers: {threads_values}; target={target_threads}")
    print(f"Planned runs: {total_runs} ({total_runs * args.duration / 3600:.2f} measured hours)")
    measurements_path = args.output / "measurements.csv"

    run_sweep(
        "neighbors",
        neighbors,
        threads_values,
        args.repeats,
        fixed_neighbors=0,
        fixed_batch=args.fixed_batch,
        args=args,
        output=args.output,
        measurements_path=measurements_path,
    )
    selected_neighbors, _ = analyze(args.output, target_threads)
    settings["experiment_2_fixed_neighbors"] = selected_neighbors
    (args.output / "settings.json").write_text(json.dumps(settings, indent=2) + "\n")
    print(f"Experiment 1 selected max_neighbors={selected_neighbors}")

    run_sweep(
        "batch",
        batches,
        threads_values,
        args.repeats,
        fixed_neighbors=selected_neighbors,
        fixed_batch=args.fixed_batch,
        args=args,
        output=args.output,
        measurements_path=measurements_path,
    )
    selected_neighbors, selected_batch = analyze(args.output, target_threads)
    print(f"Recommended: max_neighbors={selected_neighbors}, batch_size={selected_batch}")
    print(f"Data, plots, and recommendation: {args.output}")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        raise SystemExit(str(error)) from error
