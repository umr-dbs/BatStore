#!/usr/bin/env python3
"""Plot the summaries produced by the ``tpcc_tree_stats`` experiment."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from plot_styles import finalize_layout


SUMMARY_COLUMNS = {
    "checkpoint", "table", "nodes", "internal_nodes", "leaf_nodes",
    "live_entries", "dead_entries", "logical_fill_mean",
    "logical_fill_p05", "logical_fill_p50", "logical_fill_p95",
    "physical_fill_mean", "strict_weak_violations", "weak_boundary_nodes",
    "repair_due_nodes", "overflow_due_nodes",
}


def find_runs(root: Path) -> list[Path]:
    """Find tree-stat run directories below *root*, including *root* itself."""
    root = Path(root)
    if (root / "tree_summary.csv").is_file():
        return [root]
    return sorted({path.parent for path in root.rglob("tree_summary.csv")})


def is_stats_input(root: Path) -> bool:
    """Return true for a run or the conventional immediate collection layout.

    Keeping auto-detection shallow prevents ``plot.py .`` at repository root
    from unexpectedly selecting an unrelated archived stats run far below it.
    Explicit ``--kind stats`` remains recursive through :func:`find_runs`.
    """
    root = Path(root)
    if (root / "tree_summary.csv").is_file():
        return True
    candidates = list(root.glob("*/tree_summary.csv"))
    candidates += list(root.glob("experiments/*/tree_summary.csv"))
    return any(path.is_file() for path in candidates)


def _save(fig, out_dir: Path, stem: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    finalize_layout(fig)
    for extension in ("pdf", "png"):
        fig.savefig(out_dir / f"{stem}.{extension}", dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {out_dir / (stem + '.pdf')} and {out_dir / (stem + '.png')}")


def _load_summary(run_dir: Path) -> pd.DataFrame:
    path = run_dir / "tree_summary.csv"
    frame = pd.read_csv(path)
    missing = SUMMARY_COLUMNS.difference(frame.columns)
    if missing:
        raise SystemExit(f"{path} is missing required columns: {', '.join(sorted(missing))}")
    return frame


def _checkpoint_order(frame: pd.DataFrame) -> list[str]:
    preferred = ["after_load", "after_warmup", "after_run"]
    present = list(dict.fromkeys(frame["checkpoint"].astype(str)))
    return [name for name in preferred if name in present] + [
        name for name in present if name not in preferred
    ]


def plot_checkpoint_overview(frame: pd.DataFrame, out_dir: Path) -> None:
    checkpoints = _checkpoint_order(frame)
    totals = frame[frame["table"] == "__all__"].set_index("checkpoint").reindex(checkpoints)
    if totals.empty or totals.isna().all(axis=None):
        # Older experiment output may omit the aggregate row. Summing counts is
        # still useful; filling ratios are intentionally left as table means.
        groups = frame[frame["table"] != "__all__"].groupby("checkpoint", sort=False)
        totals = groups.agg({
            "internal_nodes": "sum", "leaf_nodes": "sum", "live_entries": "sum",
            "dead_entries": "sum", "logical_fill_mean": "mean",
            "physical_fill_mean": "mean", "strict_weak_violations": "sum",
            "weak_boundary_nodes": "sum", "repair_due_nodes": "sum",
            "overflow_due_nodes": "sum",
        }).reindex(checkpoints)

    x = np.arange(len(checkpoints))
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))

    ax = axes[0, 0]
    ax.plot(
        x, 100 * totals["logical_fill_mean"], marker="o",
        label="Live entries only",
    )
    ax.plot(
        x, 100 * totals["physical_fill_mean"], marker="s",
        label="Live + dead entries",
    )
    ax.set_ylabel("Mean filling (%)")
    ax.legend()

    ax = axes[0, 1]
    ax.bar(x, totals["leaf_nodes"], label="Leaf")
    ax.bar(x, totals["internal_nodes"], bottom=totals["leaf_nodes"], label="Internal")
    ax.set_ylabel("Current nodes")
    ax.legend()

    ax = axes[1, 0]
    ax.bar(x, totals["live_entries"], label="Live")
    ax.bar(x, totals["dead_entries"], bottom=totals["live_entries"], label="Dead")
    ax.set_ylabel("Entries")
    ax.legend()

    ax = axes[1, 1]
    for column, label in (
        ("strict_weak_violations", "Strict weak violations"),
        ("weak_boundary_nodes", "Weak boundary"),
        ("repair_due_nodes", "Repair due"),
        ("overflow_due_nodes", "Overflow due"),
    ):
        ax.plot(x, totals[column], marker="o", label=label)
    ax.set_ylabel("Nodes")
    ax.set_yscale("symlog", linthresh=1)
    ax.legend(fontsize=8)

    for ax in axes.flat:
        ax.set_xticks(x, [name.removeprefix("after_").replace("_", " ") for name in checkpoints])
        ax.grid(axis="y", alpha=0.25)
    fig.suptitle("TPC-C tree state by checkpoint")
    _save(fig, out_dir, "tree_stats_checkpoints")


def plot_table_state(frame: pd.DataFrame, out_dir: Path) -> None:
    checkpoints = _checkpoint_order(frame)
    if not checkpoints:
        return
    checkpoint = checkpoints[-1]
    current = frame[(frame["checkpoint"] == checkpoint) & (frame["table"] != "__all__")].copy()
    current = current.sort_values("logical_fill_mean")
    if current.empty:
        return

    y = np.arange(len(current))
    fig, axes = plt.subplots(1, 2, figsize=(14, max(6, 0.38 * len(current))))
    ax = axes[0]
    bar_height = 0.36
    ax.barh(
        y - bar_height / 2, 100 * current["logical_fill_mean"],
        height=bar_height, label="Live-entry filling", color="#0072B2",
    )
    ax.barh(
        y + bar_height / 2, 100 * current["physical_fill_mean"],
        height=bar_height, label="Occupied space (live + dead)", color="#E69F00",
    )
    ax.set_xlabel("Mean node filling (%)")
    ax.set_yticks(y, current["table"])
    ax.legend(fontsize=8)

    ax = axes[1]
    denominator = current["nodes"].replace(0, np.nan)
    for column, label, marker in (
        ("strict_weak_violations", "Strict weak violations", "o"),
        ("weak_boundary_nodes", "Weak boundary", "s"),
        ("repair_due_nodes", "Repair due", "^"),
        ("overflow_due_nodes", "Overflow due", "D"),
    ):
        percentages = 100 * current[column] / denominator
        ax.scatter(percentages, y, marker=marker, s=42, label=label)
    ax.set_xlabel("Share of table nodes (%)")
    ax.set_yticks(y, [])
    ax.legend(fontsize=8)

    for ax in axes:
        ax.grid(axis="x", alpha=0.25)
    fig.suptitle(f"TPC-C tree state after {checkpoint.removeprefix('after_').replace('_', ' ')}")
    _save(fig, out_dir, "tree_stats_by_table")


def _fill_histograms(path: Path, checkpoint: str, tables: list[str]):
    """Aggregate the large node-level CSV without retaining it in memory."""
    # The small epsilon keeps an exactly full node in the 90–100% band. The
    # final bin makes any documented overflow immediately visible.
    bins = np.concatenate((np.linspace(0, 1, 11)[:-1], [1.000000001, np.inf]))
    logical = {table: np.zeros(11, dtype=np.int64) for table in tables}
    physical = {table: np.zeros(11, dtype=np.int64) for table in tables}
    counts = {table: 0 for table in tables}
    usecols = ["checkpoint", "table", "logical_fill", "physical_fill"]
    for chunk in pd.read_csv(path, usecols=usecols, chunksize=100_000):
        chunk = chunk[(chunk["checkpoint"] == checkpoint) & chunk["table"].isin(tables)]
        for table, group in chunk.groupby("table", sort=False):
            logical[table] += np.histogram(group["logical_fill"], bins=bins)[0]
            physical[table] += np.histogram(group["physical_fill"], bins=bins)[0]
            counts[table] += len(group)
    return logical, physical, counts


def plot_fill_distribution(run_dir: Path, frame: pd.DataFrame, out_dir: Path) -> None:
    """Plot a table-by-filling-band overview from ``node_filling.csv``."""
    path = run_dir / "node_filling.csv"
    checkpoints = _checkpoint_order(frame)
    if not path.is_file() or not checkpoints:
        return
    checkpoint = checkpoints[-1]
    current = frame[(frame["checkpoint"] == checkpoint) & (frame["table"] != "__all__")]
    current = current.sort_values("logical_fill_mean", ascending=False)
    tables = current["table"].astype(str).tolist()
    logical, physical, counts = _fill_histograms(path, checkpoint, tables)
    if not any(counts.values()):
        print(f"Skipping {path}: no rows found for checkpoint {checkpoint}")
        return

    def percentages(values):
        return np.vstack([
            100 * values[table] / counts[table] if counts[table] else np.zeros(11)
            for table in tables
        ])

    logical_pct = percentages(logical)
    physical_pct = percentages(physical)
    vmax = max(1.0, float(max(logical_pct.max(), physical_pct.max())))
    labels = [f"{start}–{start + 10}" for start in range(0, 100, 10)] + [">100"]
    fig, axes = plt.subplots(1, 2, figsize=(16, max(6, 0.42 * len(tables))), sharey=True)
    images = []
    for ax, values, title in zip(
        axes, (logical_pct, physical_pct),
        ("Live-entry filling", "Occupied space (live + dead)"),
    ):
        image = ax.imshow(values, aspect="auto", cmap="Blues", vmin=0, vmax=vmax)
        images.append(image)
        ax.set_title(title)
        ax.set_xlabel("Node filling band (%)")
        ax.set_xticks(np.arange(len(labels)), labels, rotation=45, ha="right")
        ax.set_yticks(np.arange(len(tables)), tables)
        for row in range(len(tables)):
            for column in range(len(labels)):
                value = values[row, column]
                if value >= 0.5:
                    color = "white" if value > vmax * 0.55 else "#222222"
                    label = f"{value:.0f}" if value >= 1 else f"{value:.1f}"
                    ax.text(column, row, label, ha="center", va="center", fontsize=7, color=color)
    colorbar_axes = fig.add_axes((0.925, 0.20, 0.012, 0.60))
    colorbar = fig.colorbar(images[-1], cax=colorbar_axes)
    colorbar.set_label("Nodes in band (% of table)")
    fig.suptitle(
        f"Node filling distribution by table after "
        f"{checkpoint.removeprefix('after_').replace('_', ' ')}"
    )
    # A shared colorbar is deliberately outside the axes; tight_layout cannot
    # account for it reliably on all supported matplotlib versions.
    fig.subplots_adjust(left=0.14, right=0.90, bottom=0.14, top=0.89, wspace=0.10)
    for extension in ("pdf", "png"):
        fig.savefig(out_dir / f"tree_stats_fill_distribution.{extension}", dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(
        f"Wrote {out_dir / 'tree_stats_fill_distribution.pdf'} and "
        f"{out_dir / 'tree_stats_fill_distribution.png'}"
    )


def plot_memory(run_dir: Path, out_dir: Path) -> None:
    path = run_dir / "mem_stats.csv"
    if not path.is_file():
        return
    frame = pd.read_csv(path)
    if "elapsed_sec" not in frame or "rss_kb" not in frame:
        print(f"Skipping {path}: expected elapsed_sec and rss_kb columns")
        return

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(frame["elapsed_sec"], frame["rss_kb"] / (1024 ** 2), label="Process RSS")
    for column, label in (
        ("jemalloc_allocated_bytes", "jemalloc allocated"),
        ("jemalloc_active_bytes", "jemalloc active"),
        ("jemalloc_resident_bytes", "jemalloc resident"),
    ):
        if column in frame:
            ax.plot(frame["elapsed_sec"], frame[column] / (1024 ** 3), label=label)
    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel("Memory (GiB)")
    ax.set_title("TPC-C tree-stat experiment memory")
    ax.grid(alpha=0.25)
    ax.legend()
    _save(fig, out_dir, "tree_stats_memory")


def plot_run(run_dir: Path) -> None:
    run_dir = Path(run_dir)
    out_dir = run_dir / "plots"
    frame = _load_summary(run_dir)
    plot_checkpoint_overview(frame, out_dir)
    plot_table_state(frame, out_dir)
    plot_fill_distribution(run_dir, frame, out_dir)
    plot_memory(run_dir, out_dir)


def plot_all(root: Path) -> list[Path]:
    runs = find_runs(Path(root))
    if not runs:
        raise SystemExit(
            f"No TPC-C tree-stat runs found in {root}. Expected tree_summary.csv "
            "in that directory or one of its subdirectories."
        )
    for run_dir in runs:
        print(f"Plotting tree stats from {run_dir}")
        plot_run(run_dir)
    return runs
