#!/usr/bin/env python3
"""Plot H1 YCSB A read p50 against update throughput or its 2x rescale.

Uses completed update counts from each run's stdout and the measured duration
in manifest.csv. For the fixed 50/50 YCSB A mix, the total-ops plot uses
exactly twice the same updates/s coordinates.
"""
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


MODES = {
    "atomic": ("Auto-commit", "#777777", "o"),
    "transaction": ("SI", "#111111", "s"),
}
UPDATE_COUNT = re.compile(r"^update\s+(\d+)\s*$", re.MULTILINE)


def plot(run_dir: Path, metric: str = "updates") -> list[Path]:
    if metric not in {"updates", "total_ops"}:
        raise ValueError(f"Unknown throughput metric: {metric}")
    read_p50 = {}
    with (run_dir / "h1_op_latency.csv").open(newline="") as source:
        for row in csv.DictReader(source):
            if row["workload"] == "ycsb_a" and row["operation"] == "read":
                read_p50[(row["mode"], int(row["threads"]))] = float(row["p50_us"])

    points = {mode: [] for mode in MODES}
    with (run_dir / "manifest.csv").open(newline="") as source:
        for row in csv.DictReader(source):
            if row["workload"] != "ycsb_a":
                continue
            mode = re.search(r"\bmode=(atomic|transaction)\b", row["config_label"])
            if mode is None:
                raise ValueError(f"Missing H1 mode in config_label: {row['config_label']}")
            mode = mode.group(1)
            threads = int(row["threads"])
            duration = float(row["duration_secs"])
            if duration <= 0:
                raise ValueError(f"Invalid measured duration for {mode}, {threads} workers")
            log = run_dir / "ycsb_a" / mode / f"threads_{threads}" / "stdout.log"
            match = UPDATE_COUNT.search(log.read_text())
            if match is None:
                raise ValueError(f"Missing completed update count in {log}")
            throughput = int(match.group(1)) / duration
            if metric == "total_ops":
                throughput *= 2
            points[mode].append((throughput, read_p50[(mode, threads)]))

    fig, ax = plt.subplots(figsize=(7.2, 4.7))
    for mode, (label, color, marker) in MODES.items():
        if not points[mode]:
            raise ValueError(f"No YCSB A measurements for {label}")
        ordered = sorted(points[mode])
        ax.plot([p[0] for p in ordered], [p[1] for p in ordered],
                color=color, marker=marker, markersize=6, linewidth=2,
                markeredgecolor="white", markeredgewidth=0.7, label=label)
    ax.set_xlabel("Achieved updates/s" if metric == "updates" else "Achieved total ops/s")
    ax.set_ylabel("Read p50 latency (µs)")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value / 1_000_000:g}M"))
    subject = "update throughput" if metric == "updates" else "total throughput"
    ax.set_title(f"H1 · YCSB A read latency vs. {subject}", pad=12)
    ax.grid(alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()

    out_dir = run_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = [out_dir / f"h1_read_p50_vs_{metric}.{ext}" for ext in ("png", "pdf")]
    for path in paths:
        fig.savefig(path, dpi=180)
    plt.close(fig)
    return paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path, help="H1 run directory containing manifest.csv")
    parser.add_argument("--metric", choices=("updates", "total_ops"), default="updates")
    args = parser.parse_args()
    for output in plot(args.run_dir, args.metric):
        print(output)
