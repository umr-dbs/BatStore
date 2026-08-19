#!/usr/bin/env python3
"""Plot figures for a scripts/run_htap_analytical_sweep.py run: x-axis = number of
analytical (OLAP) threads, with two separate y-axis series per engine - OLTP throughput
(new_order_per_sec, from the fixed-size TPC-C population/terminal pool) and OLAP
throughput (CH-benCHmark Q1/Q6 queries/sec, summed across all analytical threads) - drawn
as two side-by-side subplots (OLTP left, OLAP right) so the OLTP-interference and the
OLAP-scaling stories are each readable on their own axis, per htap_q1/htap_q6 workload,
one figure per engine plus an all-engines overlay.

Reads <run_dir>/manifest.csv, written by run_htap_analytical_sweep.py - same schema as
compare_engines.py's manifest, with the analytical thread count stamped into
config_label as "... olap_threads=<n>" (see that script's main()). OLAP throughput is
derived here as scan_count / duration_secs (not stored directly in the manifest).

    python3 scripts/plot_htap_analytical.py --run-dir htap_analytical_results/run_20260101_120000

Requires: pandas, matplotlib.
"""
import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ENGINE_LABELS = {
    "batstore": "BatStore", "leanstore": "LeanStore", "wiredtiger": "WiredTiger", "postgres": "PostgreSQL",
    "vweaver_ermia": "vWeaver/ERMIA", "vweaver_ermia_frugal": "Frugal/ERMIA", "libmdbx": "libmdbx",
}
ENGINE_COLORS = {
    "batstore": "tab:green", "leanstore": "tab:blue", "wiredtiger": "tab:orange", "postgres": "tab:red",
    "vweaver_ermia": "tab:purple", "vweaver_ermia_frugal": "tab:pink", "libmdbx": "tab:brown",
}
HTAP_WORKLOADS = ["htap_q1", "htap_q6"]
HTAP_LABELS = {"htap_q1": "CH-benCHmark Q1 (Pricing Summary Report)", "htap_q6": "CH-benCHmark Q6 (Forecasting Revenue Change)"}

_OLAP_RE = re.compile(r"olap_threads=(\d+)")


def olap_threads_of(config_label: str) -> int:
    m = _OLAP_RE.search(config_label)
    return int(m.group(1)) if m else -1


def load_manifest(run_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(run_dir / "manifest.csv")
    df = df[df["notes"].fillna("") == ""].copy()
    df["olap_threads"] = df["config_label"].map(olap_threads_of)
    df["olap_qps"] = df["scan_count"] / df["duration_secs"].replace(0, float("nan"))
    return df


def _plot_pair(ax_oltp, ax_olap, xs, oltp_series, olap_series, label, color=None):
    ax_oltp.plot(xs, oltp_series, marker="o", label=label, color=color)
    ax_olap.plot(xs, olap_series, marker="o", label=label, color=color)


def plot_workload_per_engine(df: pd.DataFrame, workload: str, out_dir: Path) -> None:
    sub = df[df["workload"] == workload]
    if sub.empty:
        return
    for engine in sorted(sub["engine"].unique()):
        esub = sub[sub["engine"] == engine].sort_values("olap_threads")
        fig, (ax_oltp, ax_olap) = plt.subplots(1, 2, figsize=(12, 5))
        _plot_pair(ax_oltp, ax_olap, esub["olap_threads"], esub["primary_metric_value"], esub["olap_qps"],
                   ENGINE_LABELS.get(engine, engine), ENGINE_COLORS.get(engine))
        ax_oltp.set_title("OLTP throughput (fixed OLTP terminals)")
        ax_oltp.set_ylabel("new_order/sec")
        ax_olap.set_title("OLAP throughput (all analytical threads)")
        ax_olap.set_ylabel("queries/sec")
        for ax in (ax_oltp, ax_olap):
            ax.set_xlabel("Number of analytical (OLAP) threads")
            ax.grid(True, alpha=0.3)
        fig.suptitle(f"{ENGINE_LABELS.get(engine, engine)} - {HTAP_LABELS.get(workload, workload)}")
        fig.tight_layout()
        for ext in ("pdf", "svg"):
            fig.savefig(out_dir / f"htap_analytical_{workload}_{engine}.{ext}")
        plt.close(fig)


def plot_workload_all_engines(df: pd.DataFrame, workload: str, out_dir: Path) -> None:
    sub = df[df["workload"] == workload]
    if sub.empty:
        return
    fig, (ax_oltp, ax_olap) = plt.subplots(1, 2, figsize=(12, 5))
    for engine in sorted(sub["engine"].unique()):
        esub = sub[sub["engine"] == engine].sort_values("olap_threads")
        _plot_pair(ax_oltp, ax_olap, esub["olap_threads"], esub["primary_metric_value"], esub["olap_qps"],
                   ENGINE_LABELS.get(engine, engine), ENGINE_COLORS.get(engine))
    ax_oltp.set_title("OLTP throughput (fixed OLTP terminals)")
    ax_oltp.set_ylabel("new_order/sec")
    ax_olap.set_title("OLAP throughput (all analytical threads)")
    ax_olap.set_ylabel("queries/sec")
    for ax in (ax_oltp, ax_olap):
        ax.set_xlabel("Number of analytical (OLAP) threads")
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize="small")
    fig.suptitle(HTAP_LABELS.get(workload, workload))
    fig.tight_layout()
    for ext in ("pdf", "svg"):
        fig.savefig(out_dir / f"htap_analytical_{workload}_all_engines.{ext}")
    plt.close(fig)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", required=True, type=Path)
    args = p.parse_args()

    df = load_manifest(args.run_dir)
    out_dir = args.run_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)

    for workload in HTAP_WORKLOADS:
        if workload not in df["workload"].unique():
            continue
        plot_workload_per_engine(df, workload, out_dir)
        plot_workload_all_engines(df, workload, out_dir)

    print(f"Wrote plots to {out_dir}")


if __name__ == "__main__":
    main()
