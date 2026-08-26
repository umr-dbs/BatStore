#!/usr/bin/env python3
"""Plot figures for a scripts/run_skew_sweep.py run: one figure per YCSB workload (a-f),
x-axis = skew factor (uniform, then Zipfian theta 0.1/0.4/0.8/0.99/1.4), y-axis =
throughput (ops/sec), with one line per thread count. One figure is produced per engine
(so BatStore/libmdbx/PostgreSQL/WiredTiger each get their own file and can also be
overlaid manually) plus an all-engines-overlaid variant per workload, at a single
reference thread count, for a quick cross-engine skew comparison.

Reads <run_dir>/manifest.csv, written by run_skew_sweep.py - same schema as
compare_engines.py's manifest, with the skew value stamped into config_label as
"<scale-label> skew=<label>" (see run_skew_sweep.py::main).

    python3 scripts/plot_skew_sweep.py --run-dir skew_sweep_results/run_20260101_120000

Requires: pandas, matplotlib.
"""
import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from plot_styles import ENGINE_LABELS, engine_line_style, engine_sort_key
YCSB_WORKLOADS = [f"ycsb_{w}" for w in "abcdef"]

_SKEW_RE = re.compile(r"skew=(\S+)")


def skew_label(config_label: str) -> str:
    m = _SKEW_RE.search(config_label)
    return m.group(1) if m else "?"


def skew_sort_key(skew: str):
    return (-1.0, "uniform") if skew == "uniform" else (float(skew), skew)


def load_manifest(run_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(run_dir / "manifest.csv")
    df = df[df["notes"].fillna("") == ""]
    df["skew"] = df["config_label"].map(skew_label)
    return df


def _save(fig, out_dir: Path, name: str) -> None:
    """Save PDFs in plots/ and SVGs in plots/svg/."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "svg"):
        destination = out_dir / "svg" if ext == "svg" else out_dir
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / f"{name}.{ext}"
        fig.savefig(path)
        print(f"Wrote {path}")


def plot_workload_per_engine(df: pd.DataFrame, workload: str, out_dir: Path) -> None:
    sub = df[df["workload"] == workload]
    if sub.empty:
        return
    for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
        esub = sub[sub["engine"] == engine]
        skews = sorted(esub["skew"].unique(), key=skew_sort_key)
        fig, ax = plt.subplots(figsize=(7, 5))
        for threads in sorted(esub["threads"].unique()):
            tsub = esub[esub["threads"] == threads].set_index("skew").reindex(skews)
            ax.plot(skews, tsub["primary_metric_value"], marker="o", label=f"{threads} threads")
        ax.set_xlabel("Skew factor (Zipfian theta; 'uniform' = theta 0.0)")
        ax.set_ylabel("Throughput (ops/sec)")
        ax.set_title(f"{ENGINE_LABELS.get(engine, engine)} - YCSB {workload.split('_')[1].upper()} vs. skew")
        ax.legend(title="Threads", fontsize="small")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        _save(fig, out_dir, f"skew_{workload}_{engine}")
        plt.close(fig)


def plot_workload_all_engines(df: pd.DataFrame, workload: str, out_dir: Path, ref_threads: int) -> None:
    sub = df[(df["workload"] == workload) & (df["threads"] == ref_threads)]
    if sub.empty:
        return
    fig, ax = plt.subplots(figsize=(7, 5))
    for engine in sorted(sub["engine"].unique(), key=engine_sort_key):
        esub = sub[sub["engine"] == engine]
        skews = sorted(esub["skew"].unique(), key=skew_sort_key)
        esub = esub.set_index("skew").reindex(skews)
        ax.plot(
            skews, esub["primary_metric_value"],
            label=ENGINE_LABELS.get(engine, engine), **engine_line_style(engine),
        )
    ax.set_xlabel("Skew factor (Zipfian theta; 'uniform' = theta 0.0)")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_title(f"YCSB {workload.split('_')[1].upper()} vs. skew (threads={ref_threads})")
    ax.legend(fontsize="small")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    _save(fig, out_dir, f"skew_{workload}_all_engines_threads{ref_threads}")
    plt.close(fig)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", required=True, type=Path)
    p.add_argument("--ref-threads", type=int, default=None,
                   help="thread count used for the all-engines-overlay plot (default: max present)")
    args = p.parse_args()

    df = load_manifest(args.run_dir)
    out_dir = args.run_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)

    for workload in YCSB_WORKLOADS:
        if workload not in df["workload"].unique():
            continue
        plot_workload_per_engine(df, workload, out_dir)
        ref_threads = args.ref_threads or int(df[df["workload"] == workload]["threads"].max())
        plot_workload_all_engines(df, workload, out_dir, ref_threads)

    print(f"Wrote plots to {out_dir}")


if __name__ == "__main__":
    main()
