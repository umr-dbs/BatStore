#!/usr/bin/env python3
"""Run and plot the YCSB A GC batch-size / worker-scan sweep.

Example: python3 scripts/run_gc_sweep.py --duration 2 --repeats 2
The gc-stats build records block-request latency and GC-list probes.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import random
import re
import statistics
import subprocess
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
BATCHES = [1, 2, 4, 6, *range(8, 33, 2)]
PERCENTS = [20, 25, 30, 50, 70, 90, 95, 100]
COUNTERS = ["local_reuse", "steal", "fresh_alloc", "request_count", "latency_ns", "scan_count", "lists_checked"]
MAX_COUNTERS = ["latency_max_ns", "lists_checked_max"]


def stats(path: Path) -> dict[str, int]:
    with path.open(newline="") as file:
        rows = list(csv.DictReader(file))
    if not rows or any(row["schema_version"] != "2" for row in rows):
        raise ValueError(f"Invalid GC statistics: {path}")
    return {**{key: sum(int(row[key]) for row in rows) for key in COUNTERS},
            **{key: max(int(row[key]) for row in rows) for key in MAX_COUNTERS}}


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def plot(rows: list[dict], directory: Path) -> None:
    summaries = []
    for batch in BATCHES:
        for percent in PERCENTS:
            group = [r for r in rows if r["batch_size"] == batch and r["scan_percent"] == percent]
            summaries.append({
                "batch_size": batch, "scan_percent": percent, "repeats": len(group),
                **{key: statistics.mean(r[key] for r in group) for key in
                   ["throughput_ops_sec", "block_latency_us", "lists_per_scan", "fresh_share", "local_share", "steal_share", "block_requests"]},
                "throughput_stddev": statistics.stdev(r["throughput_ops_sec"] for r in group) if len(group) > 1 else 0,
            })
    write_csv(directory / "summary.csv", summaries)
    settings = json.loads((directory / "settings.json").read_text())
    subtitle = (f"YCSB A · {settings['duration_seconds']} s per run · "
                f"{settings['threads']} workers · {settings['repeats']} repeats per setting")

    def marginal_panel(ax, field, values, metric, scale, color, title, ylabel):
        means, errors = [], []
        for value in values:
            sample = [r[metric] / scale for r in rows if r[field] == value]
            means.append(statistics.mean(sample))
            errors.append(1.96 * statistics.stdev(sample) / len(sample) ** 0.5)
        ax.errorbar(values, means, yerr=errors, color=color, marker="o",
                    markersize=4, linewidth=1.5, capsize=2.5)
        ax.set_title(title, loc="left", fontsize=12, weight="bold")
        ax.set_ylabel(ylabel)
        ax.set_xticks(values)
        ax.set_xlabel("Batch size" if field == "batch_size" else "Worker-list scan limit (%)")
        ax.grid(axis="y", color="#d9e0e6", linewidth=0.7)
        ax.spines[["top", "right"]].set_visible(False)

    blue, teal = "#225c9b", "#138a76"
    panels = [
        ("throughput_ops_sec", 1e6, "YCSB throughput", "Million ops/s"),
        ("block_latency_us", 1, "Block request latency", "Mean µs/request"),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(14, 8), constrained_layout=True)
    for row, (metric, scale, title, ylabel) in enumerate(panels):
        marginal_panel(axes[row, 0], "batch_size", BATCHES, metric, scale, blue,
                       f"{title} by batch size", ylabel)
        marginal_panel(axes[row, 1], "scan_percent", PERCENTS, metric, scale, teal,
                       f"{title} by scan limit", ylabel)
    fig.suptitle(subtitle + "\nMeans across the other setting; whiskers show descriptive 95% normal intervals",
                 fontsize=13)
    fig.savefig(directory / "gc_sweep_marginals.png", dpi=180)
    fig.savefig(directory / "gc_sweep_marginals.pdf")
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.8), constrained_layout=True)
    baseline = next(r["throughput_ops_sec"] for r in summaries
                    if r["batch_size"] == 16 and r["scan_percent"] == 25)
    for ax, metric, title, cmap, scale in [
        (axes[0], "throughput_ops_sec", "Throughput vs. 16 / 25% baseline", "RdBu", 100 / baseline),
        (axes[1], "block_latency_us", "Mean block request latency", "viridis_r", 1),
    ]:
        values = np.array([[next(r[metric] for r in summaries
                                 if r["batch_size"] == batch and r["scan_percent"] == percent) * scale
                            for percent in PERCENTS] for batch in BATCHES])
        if metric == "throughput_ops_sec":
            values -= 100
            span = max(abs(values.min()), abs(values.max()))
            image = ax.imshow(values, origin="lower", aspect="auto", cmap=cmap,
                              vmin=-span, vmax=span)
        else:
            image = ax.imshow(values, origin="lower", aspect="auto", cmap=cmap)
        ax.set_title(title, loc="left", fontsize=12, weight="bold")
        ax.set_xticks(range(len(PERCENTS)), PERCENTS)
        ax.set_yticks(range(len(BATCHES)), BATCHES)
        ax.set_xlabel("Worker-list scan limit (%)")
        ax.set_ylabel("Batch size")
        ax.plot(1, BATCHES.index(16), marker="*", color="black", markersize=11)
        fig.colorbar(image, ax=ax, shrink=0.8, label="% change" if metric == "throughput_ops_sec" else "µs")
    fig.suptitle(subtitle + "\nCell values average two runs; star marks the original 16 / 25% setting",
                 fontsize=13)
    fig.savefig(directory / "gc_sweep.png", dpi=180)
    fig.savefig(directory / "gc_sweep.pdf")
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(13, 4.5), constrained_layout=True)
    marginal_panel(axes[0], "scan_percent", PERCENTS, "fresh_share", 1, teal,
                   "Fresh blocks served", "% of block requests")
    marginal_panel(axes[1], "scan_percent", PERCENTS, "lists_per_scan", 1, blue,
                   "Worker lists checked", "Mean lists / GC search")
    fig.suptitle(subtitle + "\nMeans across all batch sizes", fontsize=13)
    fig.savefig(directory / "gc_sweep_reuse.png", dpi=180)
    fig.savefig(directory / "gc_sweep_reuse.pdf")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--records", type=int, default=20_000)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--output", type=Path, default=ROOT / "experiments" / f"gc_sweep_{dt.datetime.now():%Y%m%d_%H%M%S}")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--plot-only", action="store_true", help="regenerate plots from an existing measurements.csv")
    args = parser.parse_args()
    if min(args.duration, args.repeats, args.records, args.threads) < 1:
        parser.error("duration, repeats, records, and threads must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    if args.plot_only:
        with (args.output / "measurements.csv").open(newline="") as file:
            rows = list(csv.DictReader(file))
        for row in rows:
            for key, value in row.items():
                row[key] = int(value) if key in ("repeat", "batch_size", "scan_percent") else float(value)
        plot(rows, args.output)
        return
    binary = ROOT / "target/release/batstore"
    if not args.skip_build:
        subprocess.run(["cargo", "build", "--release", "--offline", "--features", "gc-stats"], cwd=ROOT, check=True)
    if not binary.exists():
        parser.error(f"Missing benchmark binary: {binary}")
    (args.output / "settings.json").write_text(json.dumps({
        "duration_seconds": args.duration, "repeats": args.repeats,
        "records": args.records, "threads": args.threads,
        "batch_sizes": BATCHES, "scan_percentages": PERCENTS,
        "binary": str(binary), "distribution": "zipfian", "theta": 0.99,
        "execution_mode": "atomic", "gc": True, "idle_compaction": False,
    }, indent=2) + "\n")
    combinations = [(repeat, batch, percent) for repeat in range(1, args.repeats + 1)
                    for batch in BATCHES for percent in PERCENTS]
    random.Random(20260917).shuffle(combinations)
    rows = []
    import os
    for index, (repeat, batch, percent) in enumerate(combinations, 1):
        run_dir = args.output / f"r{repeat}_b{batch}_p{percent}"
        run_dir.mkdir(exist_ok=True)
        env = dict(os.environ, BATSTORE_GC_BATCH_SIZE=str(batch), BATSTORE_GC_SCAN_PERCENT=str(percent))
        command = [str(binary), "ycsb", "a", str(args.records), str(args.threads), str(args.duration),
                   "zipfian", "0.99", "10", "100", "100", "fg", "true", "false", "false",
                   str(run_dir / "wal.log"), "5", "false", "true", "atomic", "0", "0"]
        with (run_dir / "run.log").open("w") as log:
            subprocess.run(command, cwd=run_dir, env=env, stdout=log, stderr=subprocess.STDOUT,
                           timeout=max(60, args.duration * 5), check=True)
        log_text = (run_dir / "run.log").read_text()
        throughput_match = re.search(r"throughput \(ops/sec\)\s+([\d.]+)", log_text)
        if not throughput_match:
            raise ValueError(f"YCSB throughput missing: {run_dir / 'run.log'}")
        before, after = stats(run_dir / "gc_stats_after_load.csv"), stats(run_dir / "gc_stats.csv")
        delta = {key: after[key] - before[key] for key in COUNTERS}
        if any(value < 0 for value in delta.values()):
            raise ValueError(f"Counters went backwards: {run_dir}")
        requests = delta["request_count"]
        if requests != sum(delta[key] for key in ("local_reuse", "steal", "fresh_alloc")):
            raise ValueError(f"Allocation sources do not sum to requests: {run_dir}")
        row = {
            "repeat": repeat, "batch_size": batch, "scan_percent": percent,
            "throughput_ops_sec": float(throughput_match.group(1)),
            "block_requests": requests,
            "block_latency_us": delta["latency_ns"] / requests / 1000 if requests else 0,
            "lists_per_scan": delta["lists_checked"] / delta["scan_count"] if delta["scan_count"] else 0,
            "fresh_share": 100 * delta["fresh_alloc"] / requests if requests else 0,
            "local_share": 100 * delta["local_reuse"] / requests if requests else 0,
            "steal_share": 100 * delta["steal"] / requests if requests else 0,
            "whole_run_max_block_latency_us": after["latency_max_ns"] / 1000,
            "whole_run_max_lists_checked": after["lists_checked_max"],
            **delta,
        }
        rows.append(row)
        write_csv(args.output / "measurements.csv", rows)
        print(f"[{index}/{len(combinations)}] batch={batch:2} scan={percent:3}% repeat={repeat}: "
              f"{row['throughput_ops_sec']:.0f} ops/s, {row['block_latency_us']:.1f} µs, "
              f"{row['lists_per_scan']:.2f} lists", flush=True)
    plot(rows, args.output)
    print(f"Results: {args.output}")


if __name__ == "__main__":
    main()
