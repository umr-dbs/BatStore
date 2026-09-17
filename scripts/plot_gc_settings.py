#!/usr/bin/env python3
"""Plot absolute throughput and block latency for every GC sweep setting."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Rectangle


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="GC sweep result directory")
    args = parser.parse_args()
    directory = args.directory.resolve()
    with (directory / "summary.csv").open(newline="") as file:
        rows = list(csv.DictReader(file))
    batches = sorted({int(row["batch_size"]) for row in rows})
    percents = sorted({int(row["scan_percent"]) for row in rows})
    indexed = {(int(row["batch_size"]), int(row["scan_percent"])): row for row in rows}
    if len(rows) != len(batches) * len(percents) or len(indexed) != len(rows):
        raise ValueError("summary.csv has missing or repeated settings")
    repeats = {int(row["repeats"]) for row in rows}
    if len(repeats) != 1:
        raise ValueError("settings have different repeat counts")

    throughput = np.array([[float(indexed[batch, percent]["throughput_ops_sec"]) / 1e6
                            for percent in percents] for batch in batches])
    latency = np.array([[float(indexed[batch, percent]["block_latency_us"])
                         for percent in percents] for batch in batches])
    fig, axes = plt.subplots(1, 2, figsize=(17, 10.4), constrained_layout=True)
    panels = [
        (axes[0], throughput, "Throughput", "Million operations per second", "YlGn"),
        (axes[1], latency, "Block request latency", "Microseconds per request", "YlOrRd"),
    ]
    for ax, values, title, colorbar_label, palette in panels:
        image = ax.imshow(values, origin="lower", aspect="auto", cmap=palette)
        ax.set_title(title, loc="left", fontsize=16, weight="bold", pad=12)
        ax.set_xticks(range(len(percents)), [f"{p}%" for p in percents])
        ax.set_yticks(range(len(batches)), batches)
        ax.set_xlabel("Worker-list scan limit", fontsize=11, labelpad=8)
        ax.set_ylabel("Reclaim / allocation batch size", fontsize=11, labelpad=8)
        ax.set_xticks(np.arange(-0.5, len(percents), 1), minor=True)
        ax.set_yticks(np.arange(-0.5, len(batches), 1), minor=True)
        ax.grid(which="minor", color="white", linewidth=1.5)
        ax.tick_params(which="minor", bottom=False, left=False)
        middle = (values.min() + values.max()) / 2
        for y in range(len(batches)):
            for x in range(len(percents)):
                ax.text(x, y, f"{values[y, x]:.2f}", ha="center", va="center",
                        fontsize=8.5, weight="medium",
                        color="white" if values[y, x] > middle else "#18232b")
        if 16 in batches and 25 in percents:
            ax.add_patch(Rectangle((percents.index(25) - 0.5, batches.index(16) - 0.5),
                                   1, 1, fill=False, edgecolor="#17212b", linewidth=2.4))
        fig.colorbar(image, ax=ax, shrink=0.77, pad=0.02, label=colorbar_label)
    fig.suptitle(f"YCSB A GC settings · each cell is the mean of {repeats.pop()} runs\n"
                 "Outlined cell: original batch 16 / scan 25% setting",
                 fontsize=15, weight="semibold")
    for extension in ("png", "pdf", "svg"):
        fig.savefig(directory / f"gc_settings_values.{extension}", dpi=220)
    plt.close(fig)
    print(directory / "gc_settings_values.png")


if __name__ == "__main__":
    main()
