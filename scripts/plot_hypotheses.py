#!/usr/bin/env python3
"""Plot the focused H1--H6 experiments from their saved CSV files.

Unlike the experiment drivers, this module never runs a benchmark.  It can
regenerate one hypothesis figure or assemble the latest H1--H6 runs below a
directory into a compact, paper-friendly overview.
"""
from __future__ import annotations

import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from plot_styles import (apply_compact_layout, compact_enabled, latency_line_style,
                         measurement_positions, measurement_values,
                         set_compact, set_measurement_axis)


HYPOTHESIS_FILES = {
    "h1": "h1_comparison.csv",
    "h2": "h2_skew_summary.csv",
    "h3": "h3_time_buckets.csv",
    "h4": "manifest.csv",
    "h5": "manifest.csv",
    "h6": "h6_gc_stats.csv",
}
COLORS = {"blue": "#0072B2", "orange": "#D55E00", "green": "#009E73", "black": "#222222"}


def hypothesis_id(run_dir: Path) -> str | None:
    """Identify an H1--H6 run using its summary files or configuration."""
    for hypothesis, filename in HYPOTHESIS_FILES.items():
        if filename != "manifest.csv" and (run_dir / filename).exists():
            return hypothesis
    config = run_dir / "configuration.json"
    if config.exists():
        try:
            import json
            value = str(json.loads(config.read_text()).get("hypothesis", "")).lower()
            if value in HYPOTHESIS_FILES:
                return value
        except (OSError, ValueError, TypeError):
            pass
    return None


def find_runs(root: Path) -> dict[str, Path]:
    """Return the newest complete run for every hypothesis found below *root*."""
    found: dict[str, list[Path]] = {h: [] for h in HYPOTHESIS_FILES}
    if hypothesis_id(root):
        candidates = [root]
    else:
        # Keep auto-detection local to the requested result collection.  A
        # recursive search would make ``plot.py .`` unexpectedly select an
        # unrelated archived H-run elsewhere in a repository.
        candidates = [p for p in root.glob("run_*") if p.is_dir()]
        candidates += [p for p in root.glob("h[1-6]_results/run_*") if p.is_dir()]
    for candidate in candidates:
        hypothesis = hypothesis_id(candidate)
        if hypothesis and (candidate / HYPOTHESIS_FILES[hypothesis]).exists():
            found[hypothesis].append(candidate)
    return {hypothesis: sorted(paths)[-1] for hypothesis, paths in found.items() if paths}


def _save(fig, out_dir: Path, stem: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    apply_compact_layout(fig)
    if not getattr(fig, "_compact_layout_applied", False):
        fig.tight_layout()
    for extension in ("pdf", "png"):
        fig.savefig(out_dir / f"{stem}.{extension}", dpi=180, bbox_inches="tight")
    plt.close(fig)


def _label_hypothesis(ax, hypothesis: str, title: str) -> None:
    ax.set_title(f"{hypothesis.upper()} · {title}", loc="left", fontweight="semibold")
    ax.grid(axis="y", alpha=0.25)


def _manifest_dimension(frame: pd.DataFrame, name: str) -> pd.Series:
    values = frame["config_label"].str.extract(rf"(?:^|\s){re.escape(name)}=(\d+)", expand=False)
    return pd.to_numeric(values, errors="coerce")


def _h1(ax, run_dir: Path, overview: bool = False) -> None:
    frame = pd.read_csv(run_dir / "h1_comparison.csv")
    if compact_enabled() and not overview:
        frame = frame[frame["threads"].isin({1, 2, 4, 8, 16, 32, 64, 128})]
    thread_values = sorted(frame["threads"].unique())
    positions = measurement_positions(thread_values, thread_values)
    for index, (workload, group) in enumerate(frame.groupby("workload", sort=True)):
        group = group.sort_values("threads")
        ax.plot(positions, group["si_throughput_loss_pct"], marker="o", linewidth=2,
                color=(COLORS["blue"], COLORS["orange"])[index % 2],
                label=workload.replace("_", " ").upper())
    ax.axhline(0, color="#888888", linewidth=1, linestyle=":")
    set_measurement_axis(ax, thread_values, "Workers")
    ax.set_ylabel("SI throughput loss (%)")
    _label_hypothesis(ax, "H1", "SI overhead")
    ax.legend(frameon=False)


def _h1_latency(ax, run_dir: Path) -> None:
    """Plot SI tail-latency overhead without mixing latency units and operations."""
    frame = pd.read_csv(run_dir / "h1_op_latency.csv")
    frame = frame[frame["workload"] == "ycsb_a"]
    for operation, group in frame.groupby("operation", sort=True):
        pivot = group.pivot(index="threads", columns="mode", values="p99_us").sort_index()
        if not {"atomic", "transaction"}.issubset(pivot.columns):
            continue
        overhead = (pivot["transaction"] / pivot["atomic"] - 1.0) * 100
        ax.plot(pivot.index, overhead, marker="o", linewidth=2,
                label=operation.capitalize())
    ax.axhline(0, color="#888888", linewidth=1, linestyle=":")
    ax.set_xlabel("OLTP threads")
    ax.set_ylabel("SI p99 latency overhead (%)")
    _label_hypothesis(ax, "H1", "Tail latency")
    ax.legend(frameon=False)


def _h2(ax, run_dir: Path, overview: bool = False) -> None:
    frame = pd.read_csv(run_dir / "h2_skew_summary.csv")
    order = list(dict.fromkeys(frame["skew"].astype(str)))
    summary = frame.groupby("skew", sort=False)["relative_to_uniform_pct"].agg(["median", "min", "max"]).reindex(order)
    x = range(len(summary))
    range_label = "_thread-count range" if overview else "range across thread counts"
    median_label = "_thread-count median" if overview else "median across thread counts"
    ax.fill_between(x, summary["min"], summary["max"], color=COLORS["blue"], alpha=0.16,
                    label=range_label)
    ax.plot(x, summary["median"], color=COLORS["blue"], marker="o", linewidth=2,
            label=median_label)
    ax.axhline(100, color="#888888", linewidth=1, linestyle=":")
    tick_labels = ["0" if value == "uniform" else value for value in summary.index]
    ax.set_xticks(list(x), tick_labels)
    ax.set_xlabel("Zipfian theta")
    ax.set_ylabel("throughput / uniform (%)")
    _label_hypothesis(ax, "H2", "Key skew" if overview else "Sensitivity to key skew")
    if not overview:
        ax.legend(frameon=False, fontsize=8)


def _h2_thread(ax, frame: pd.DataFrame, threads: int, y_max: float) -> None:
    """Plot H2 throughput relative to uniform for one thread count."""
    group = frame[frame["threads"] == threads]
    order = list(dict.fromkeys(group["skew"].astype(str)))
    values = group.set_index(group["skew"].astype(str))["relative_to_uniform_pct"].reindex(order)
    x = list(range(len(order)))
    ax.plot(x, values, color=COLORS["blue"], marker="o", linewidth=2)
    ax.axhline(100, color="#888888", linewidth=1, linestyle=":")
    ax.set_xticks(x, ["0" if value == "uniform" else value for value in order])
    ax.set_ylim(0, y_max)
    ax.set_xlabel("Zipfian theta")
    ax.set_ylabel("throughput relative to uniform (%)")
    _label_hypothesis(ax, "H2", f"Key skew · {threads} thread{'s' if threads != 1 else ''}")


def _plot_h2_by_thread(run_dir: Path) -> None:
    """Save comparable, normalized H2 plots for every measured thread count."""
    frame = pd.read_csv(run_dir / "h2_skew_summary.csv")
    # Use one zero-based scale for every figure so curves can be compared
    # without autoscaling making some thread counts look artificially volatile.
    observed_max = frame["relative_to_uniform_pct"].max()
    y_max = max(200.0, ((observed_max + 49.999) // 50) * 50)
    for threads in frame["threads"].drop_duplicates():
        fig, ax = plt.subplots(figsize=(8, 4.8))
        _h2_thread(ax, frame, int(threads), y_max)
        _save(fig, run_dir / "plots", f"h2_threads_{int(threads)}")

    fig, ax = plt.subplots(figsize=(9.5, 5.8))
    thread_counts = list(frame["threads"].drop_duplicates())
    colors = plt.get_cmap("viridis").resampled(len(thread_counts))
    for index, threads in enumerate(thread_counts):
        group = frame[frame["threads"] == threads]
        order = list(dict.fromkeys(group["skew"].astype(str)))
        values = group.set_index(group["skew"].astype(str))["relative_to_uniform_pct"].reindex(order)
        x = list(range(len(order)))
        ax.plot(x, values, color=colors(index), marker="o", linewidth=1.8,
                markersize=4.5, label=f"{int(threads)}")
    ax.axhline(100, color="#888888", linewidth=1, linestyle=":", zorder=0)
    ax.set_xticks(x, ["0" if value == "uniform" else value for value in order])
    ax.set_ylim(0, y_max)
    ax.set_xlabel("Zipfian theta")
    ax.set_ylabel("throughput relative to uniform (%)")
    _label_hypothesis(ax, "H2", "Key skew by thread count")
    ax.legend(title="OLTP threads", ncol=2, frameon=False, fontsize=8,
              title_fontsize=8, loc="upper right")
    _save(fig, run_dir / "plots", "h2_threads_combined")


def _h2_latency(ax, run_dir: Path) -> None:
    """Summarize tail latency relative to uniform at matching thread counts."""
    frame = pd.read_csv(run_dir / "h2_skew_summary.csv")
    order = list(dict.fromkeys(frame["skew"].astype(str)))
    x = list(range(len(order)))
    for operation, color in (("read", COLORS["blue"]), ("update", COLORS["orange"])):
        value_col = f"{operation}_p99_us"
        relative_parts = []
        for _, group in frame.groupby("threads", sort=False):
            baseline = group.loc[group["skew"].astype(str) == "uniform", value_col]
            if baseline.empty or baseline.iloc[0] == 0:
                continue
            part = group[["skew", value_col]].copy()
            part["relative"] = part[value_col] / baseline.iloc[0]
            relative_parts.append(part[["skew", "relative"]])
        if not relative_parts:
            continue
        relative = pd.concat(relative_parts)
        summary = relative.groupby("skew", sort=False)["relative"].median().reindex(order)
        ax.plot(x, summary, color=color, marker="o", linewidth=2,
                label=operation.capitalize())
    ax.axhline(1, color="#888888", linewidth=1, linestyle=":")
    ax.set_yscale("log")
    ax.set_xticks(x, ["0" if value == "uniform" else value for value in order])
    ax.set_xlabel("Zipfian theta")
    ax.set_ylabel("p99 latency / uniform (×)")
    _label_hypothesis(ax, "H2", "Tail latency")
    ax.legend(frameon=False)


def _h3(ax, run_dir: Path, overview: bool = False) -> None:
    frame = pd.read_csv(run_dir / "h3_time_buckets.csv")
    age = (frame["window_start_s"] + frame["window_end_s"]) / 2
    if not overview:
        raw_path = run_dir / "tpcc_historic_scan" / "tpcc_scan.csv"
        if raw_path.exists():
            raw = pd.read_csv(raw_path)
            raw = raw[raw.get("mode", "") == "historic_full_scan"]
            if not raw.empty:
                snapshot_age = raw["delay_secs"] if "delay_secs" in raw else raw["elapsed_secs"]
                ax.scatter(snapshot_age, raw["latency_ns"] / 1_000_000,
                           s=4, alpha=0.11, color=COLORS["blue"], edgecolors="none",
                           rasterized=True, label="_individual scans")
                ax.plot([], [], linestyle="none", marker="o", markersize=5,
                        color=COLORS["blue"], alpha=0.75, label="individual scan")
    median_label = "50 s window median" if not overview else None
    ax.plot(age, frame["median_latency_us"] / 1000, color=COLORS["orange"], marker="o",
            linewidth=2.2, label=median_label, zorder=3)
    ax.set_xlabel("snapshot age (s)")
    ax.set_ylabel("median latency (ms)")
    if overview:
        _label_hypothesis(ax, "H3", "Snapshot age")
        cardinality = frame["median_scanned_tuples"].median()
        ax.text(0.975, 0.05, f"{cardinality / 1_000_000:.2f}M tuples/scan",
                transform=ax.transAxes, ha="right", va="bottom", fontsize=8,
                color="#555555")
    else:
        ax.set_title("Snapshot age", loc="left", fontweight="semibold")
        ax.grid(axis="y", alpha=0.25)
        cardinality = frame["median_scanned_tuples"].median()
        ax.text(
            0.985, 0.04, f"{int(round(cardinality)):,} tuples per scan",
            transform=ax.transAxes, ha="right", va="bottom", fontsize=9,
            color="#444444",
            bbox={"boxstyle": "round,pad=0.25", "facecolor": "white",
                  "edgecolor": "#cccccc", "alpha": 0.9},
        )
    if not overview:
        ax.set_ylabel("full-scan latency (ms)")
        ax.legend(frameon=False)


def _h4(ax, run_dir: Path, overview: bool = False) -> None:
    frame = pd.read_csv(run_dir / "manifest.csv")
    frame["olap_threads"] = _manifest_dimension(frame, "olap_threads")
    frame = frame.dropna(subset=["olap_threads"]).sort_values("olap_threads")
    baseline = frame.loc[frame["olap_threads"] == 0, "primary_metric_value"]
    if baseline.empty or baseline.iloc[0] == 0:
        raise ValueError(f"{run_dir}: H4 has no non-zero OLAP-threads=0 baseline")
    relative = frame["primary_metric_value"] / baseline.iloc[0] * 100
    ax.plot(frame["olap_threads"], relative, color=COLORS["black"], marker="o", linewidth=2)
    ax.axhline(100, color="#888888", linewidth=1, linestyle=":")
    ax.set_xlabel("OLAP callers")
    ax.set_ylabel("OLTP / baseline (%)")
    _label_hypothesis(ax, "H4", "OLAP interference")


def _h5(ax, run_dir: Path, overview: bool = False) -> None:
    frame = pd.read_csv(run_dir / "manifest.csv").sort_values("threads")
    frame["warehouses"] = _manifest_dimension(frame, "warehouses")
    if frame["warehouses"].nunique() > 1:
        for warehouses, group in frame.groupby("warehouses", sort=True):
            ax.plot(group["threads"], group["scan_p99_us"] / 1000,
                    marker="o", linewidth=2, label=f"{int(warehouses)} warehouses")
        ylabel = "p99 query latency (ms)"
    else:
        for percentile in ("p50", "p95", "p99"):
            ax.plot(frame["threads"], frame[f"scan_{percentile}_us"] / 1000,
                    label=percentile, **latency_line_style(percentile))
        ylabel = "query latency (ms)"
    ax.set_xlabel("OLTP terminals")
    ax.set_ylabel(ylabel)
    _label_hypothesis(ax, "H5", "OLAP latency")
    ax.legend(frameon=False)


def _h6(ax, run_dir: Path, overview: bool = False) -> None:
    frame = pd.read_csv(run_dir / "h6_gc_stats.csv").sort_values("threads")
    thread_values = frame["threads"].astype(int).tolist()
    axis_values = measurement_values(thread_values)
    positions = measurement_positions(thread_values, axis_values)
    series = [
        ("local_reuse_share", "local reuse", COLORS["blue"]),
        ("steal_all_events_share", "cross-shard reuse", COLORS["orange"]),
        ("fresh_alloc_share", "global allocator", COLORS["green"]),
    ]
    ax.stackplot(positions, *[frame[column] * 100 for column, _, _ in series],
                 labels=[label for _, label, _ in series], colors=[color for _, _, color in series], alpha=0.82)
    ax.set_ylim(0, 100)
    set_measurement_axis(ax, thread_values, "OLTP threads")
    ax.set_ylabel("allocation share (%)")
    _label_hypothesis(ax, "H6", "Memory reuse")
    ax.legend(frameon=False, fontsize=8, loc="best")


def _plot_h6_compact(run_dir: Path) -> None:
    """Save a clean, paper-sized H6 allocation-source summary."""
    was_compact = compact_enabled()
    set_compact(True)
    try:
        fig, ax = plt.subplots(figsize=(7.2, 4.0))
        _h6(ax, run_dir)
        _save(fig, run_dir / "plots", "h6_summary_compact")
    finally:
        set_compact(was_compact)


PANEL_PLOTTERS = {"h1": _h1, "h2": _h2, "h3": _h3, "h4": _h4, "h5": _h5, "h6": _h6}


def plot_run(run_dir: Path) -> None:
    """Regenerate a clean summary plot for one H1--H6 run."""
    hypothesis = hypothesis_id(run_dir)
    if hypothesis is None:
        raise ValueError(f"{run_dir} is not a recognized H1--H6 run")
    # H3's compact layout includes thousands of raw scan samples and is
    # initialized larger because the shared compact transform scales latency
    # figures down to their final single-column paper dimensions.
    if hypothesis == "h3":
        figsize = (11.5, 7.5)
    elif hypothesis == "h1" and compact_enabled():
        figsize = (8, 2.8)
    else:
        figsize = (8, 4.8)
    fig, ax = plt.subplots(figsize=figsize)
    PANEL_PLOTTERS[hypothesis](ax, run_dir)
    _save(fig, run_dir / "plots", f"{hypothesis}_summary")
    if hypothesis == "h2":
        _plot_h2_by_thread(run_dir)
    elif hypothesis == "h6":
        _plot_h6_compact(run_dir)


def plot_overview(root: Path, runs: dict[str, Path] | None = None) -> Path:
    """Plot the latest available H1--H6 runs as one comparable overview."""
    runs = runs or find_runs(root)
    if not runs:
        raise ValueError(f"no H1--H6 result runs found below {root}")
    fig, axes = plt.subplots(2, 4, figsize=(17, 9.5))
    panels = [
        ("h1", _h1), ("h1", _h1_latency),
        ("h2", _h2), ("h2", _h2_latency),
        ("h3", _h3), ("h4", _h4), ("h5", _h5), ("h6", _h6),
    ]
    for ax, (hypothesis, plotter) in zip(axes.flat, panels):
        if hypothesis not in runs:
            ax.set_axis_off()
            ax.text(0.5, 0.5, f"{hypothesis.upper()} results not found", ha="center", va="center",
                    transform=ax.transAxes, color="#777777")
            continue
        if plotter in (_h1, _h2, _h3, _h4, _h5, _h6):
            plotter(ax, runs[hypothesis], overview=True)
        else:
            plotter(ax, runs[hypothesis])
    fig.suptitle("BatStore hypothesis results: throughput and latency", fontweight="semibold")
    out_dir = root / "plots"
    _save(fig, out_dir, "hypotheses_overview")
    return out_dir
