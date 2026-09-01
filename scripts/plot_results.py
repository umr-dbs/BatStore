#!/usr/bin/env python3
"""Plot BatStore benchmark results (TPC-C, CH-benCHmark/TPC-H, HTAP, YCSB).

Reads the CSV files written by the Rust benchmark drivers
(bat_bench::tpcc_driver, bat_bench::ycsb_driver) and renders them with
matplotlib. Each driver run overwrites its CSV(s) in the current directory,
so to compare multiple runs, copy each run's CSV to a distinct name before
running the next one, e.g.:

    cargo run -- tpcc 4 4 60 true false false fg none 0   # OLTP-only baseline
    cp tpcc_oltp_timeseries.csv oltp_baseline.csv
    cargo run -- htap 4 60 2 15                            # OLTP + OLAP
    cp tpcc_oltp_timeseries.csv oltp_mixed.csv
    cp tpcc_scan.csv htap_scan.csv

    python3 scripts/plot_results.py interference oltp_baseline.csv oltp_mixed.csv
    python3 scripts/plot_results.py ch htap_scan.csv

Quickest start — with no arguments, auto-detects whichever of the 3 known
CSV filenames exist in the current directory and plots each one:

    python3 scripts/plot_results.py

Requires: pandas, matplotlib (see requirements.txt).
"""
import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from plot_styles import apply_compact_layout

# The 4 CH-benCHmark queries implemented in bat_bench::tpch_queries, and the
# order they're always run in (bat_bench::olap_scan::ch_benchmark_queries_once).
CH_QUERY_ORDER = [
    "ch_q1_pricing_summary",
    "ch_q6_forecast_revenue",
    "ch_q4_order_priority",
    "ch_q5_revenue_by_nation",
]
CH_QUERY_LABELS = {
    "ch_q1_pricing_summary": "Q1 Pricing Summary",
    "ch_q6_forecast_revenue": "Q6 Forecast Revenue",
    "ch_q4_order_priority": "Q4 Order Priority",
    "ch_q5_revenue_by_nation": "Q5 Revenue by Nation",
}


def _labels_for(paths, labels):
    if labels:
        if len(labels) != len(paths):
            raise SystemExit(f"--labels must have exactly {len(paths)} entries, got {len(labels)}")
        return labels
    return [Path(p).stem for p in paths]


def _save(fig, out):
    fig.tight_layout()
    apply_compact_layout(fig)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Wrote {out}")


def plot_oltp(paths, labels, out):
    """tpcc_oltp_timeseries.csv (elapsed_sec,new_order_committed): New-Order
    throughput over time, from any tpcc/tpch/htap run. Pass several files to
    overlay runs (e.g. different thread counts, or with/without OLAP)."""
    labels = _labels_for(paths, labels)
    fig, ax = plt.subplots(figsize=(9, 5))
    for path, label in zip(paths, labels):
        df = pd.read_csv(path)
        ax.plot(df["elapsed_sec"], df["new_order_committed"], marker="o", markersize=3, label=label)
    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel("New-Order transactions / sec")
    ax.set_title("TPC-C OLTP throughput over time")
    ax.legend()
    ax.grid(alpha=0.3)
    _save(fig, out)


def plot_ycsb(paths, labels, out):
    """ycsb_timeseries.csv (elapsed_sec,ops_completed): aggregate op
    throughput over time, from any `ycsb <workload>` run. Pass several files
    (one per workload run) to compare workloads A-F side by side."""
    labels = _labels_for(paths, labels)
    fig, ax = plt.subplots(figsize=(9, 5))
    for path, label in zip(paths, labels):
        df = pd.read_csv(path)
        ax.plot(df["elapsed_sec"], df["ops_completed"], marker="o", markersize=3, label=label)
    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel("Operations / sec")
    ax.set_title("YCSB throughput over time")
    ax.legend()
    ax.grid(alpha=0.3)
    _save(fig, out)


def plot_scan_sweep(path, out):
    """tpcc_scan.csv, 'scan_after_delay' rows only (produced by `tpcc`'s
    default/'sweep' OLAP mode): scan throughput vs. how stale the snapshot
    was when the scan ran — the classic "scan throughput vs. delay" plot."""
    df = pd.read_csv(path)
    df = df[df["mode"] == "scan_after_delay"]
    if df.empty:
        print(f"No scan_after_delay rows in {path} (wrong OLAP mode?) — skipping scan-sweep plot.")
        return
    df = df.sort_values("delay_secs")
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(df["delay_secs"], df["tuples_per_sec"], marker="o")
    ax.set_xlabel("Snapshot age at scan time (s)")
    ax.set_ylabel("Scanned tuples / sec")
    ax.set_title("OLAP scan throughput vs. snapshot delay")
    ax.grid(alpha=0.3)
    _save(fig, out)


def plot_ch_benchmark(path, out):
    """tpcc_scan.csv, 'ch_*' rows only (produced by `tpch`/`htap`, or `tpcc`
    with OLAP mode 'ch'): one row of 3 panels per CH-benCHmark query —
    latency, HTAP staleness (how many logical-clock versions behind by the
    time the query finished), and the query's own characteristic result
    value — all over elapsed wall-clock time. Separate y-axes per query
    since e.g. Q4's result is an order count while Q1/Q5/Q6's are revenue,
    wildly different scales."""
    df = pd.read_csv(path)
    df = df[df["mode"].str.startswith("ch_")]
    if df.empty:
        print(f"No CH-benCHmark (ch_*) rows in {path} (wrong OLAP mode?) — skipping.")
        return

    modes_present = [m for m in CH_QUERY_ORDER if m in df["mode"].unique()]
    fig, axes = plt.subplots(len(modes_present), 3, figsize=(14, 3.2 * len(modes_present)), squeeze=False)

    for row, mode in enumerate(modes_present):
        group = df[df["mode"] == mode].sort_values("elapsed_secs")
        label = CH_QUERY_LABELS.get(mode, mode)

        ax = axes[row][0]
        ax.plot(group["elapsed_secs"], group["latency_ns"] / 1e6, marker="o", markersize=3, color="tab:blue")
        ax.set_ylabel(f"{label}\nlatency (ms)")
        ax.grid(alpha=0.3)

        ax = axes[row][1]
        ax.plot(group["elapsed_secs"], group["staleness_versions"], marker="o", markersize=3, color="tab:orange")
        ax.set_ylabel("staleness (versions)")
        ax.grid(alpha=0.3)

        ax = axes[row][2]
        ax.plot(group["elapsed_secs"], group["summary"], marker="o", markersize=3, color="tab:green")
        ax.set_ylabel("result value")
        ax.grid(alpha=0.3)

    for ax in axes[-1]:
        ax.set_xlabel("Elapsed time (s)")
    fig.suptitle("CH-benCHmark query latency, staleness, and result value over time")
    _save(fig, out)


def plot_interference(baseline_csv, mixed_csv, out):
    """Two tpcc_oltp_timeseries.csv files: an OLTP-only baseline run overlaid
    against an OLTP+concurrent-OLAP ('tpch'/'htap') run — visualizes HTAP
    interference directly as the gap between the two curves. For the
    single-number version of this metric, see the 'OLTP interference from
    OLAP (%)' line `htap`'s own console report already prints when run with
    a baseline phase enabled."""
    df_b = pd.read_csv(baseline_csv)
    df_m = pd.read_csv(mixed_csv)
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(df_b["elapsed_sec"], df_b["new_order_committed"], marker="o", markersize=3, label="OLTP only (baseline)")
    ax.plot(df_m["elapsed_sec"], df_m["new_order_committed"], marker="o", markersize=3, label="OLTP + concurrent OLAP")
    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel("New-Order transactions / sec")
    ax.set_title("HTAP interference: OLTP throughput with vs. without concurrent OLAP")
    ax.legend()
    ax.grid(alpha=0.3)
    _save(fig, out)


def auto(directory, out_dir):
    """No-argument convenience mode: looks for the 3 known CSV filenames in
    `directory` and plots whichever are present."""
    directory = Path(directory)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    oltp = directory / "tpcc_oltp_timeseries.csv"
    scan = directory / "tpcc_scan.csv"
    ycsb = directory / "ycsb_timeseries.csv"

    found = False
    if oltp.exists():
        found = True
        plot_oltp([str(oltp)], None, str(out_dir / "oltp_throughput.png"))
    if scan.exists():
        found = True
        plot_scan_sweep(str(scan), str(out_dir / "scan_sweep.png"))
        plot_ch_benchmark(str(scan), str(out_dir / "ch_benchmark.png"))
    if ycsb.exists():
        found = True
        plot_ycsb([str(ycsb)], None, str(out_dir / "ycsb_throughput.png"))

    if not found:
        print(f"No known benchmark CSVs found in {directory}. Expected one of: "
              f"tpcc_oltp_timeseries.csv, tpcc_scan.csv, ycsb_timeseries.csv")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command")

    p_auto = sub.add_parser("auto", help="Auto-detect and plot every known CSV in a directory (default command)")
    p_auto.add_argument("--dir", default=".", help="Directory to look in (default: current directory)")
    p_auto.add_argument("--out-dir", default="plots", help="Directory to write PNGs to (default: ./plots)")

    p_oltp = sub.add_parser("oltp", help="Plot TPC-C OLTP throughput over time (one or more runs overlaid)")
    p_oltp.add_argument("csv", nargs="+", help="tpcc_oltp_timeseries.csv file(s), e.g. from tpcc/tpch/htap runs")
    p_oltp.add_argument("--labels", nargs="+", help="One label per CSV (default: filename)")
    p_oltp.add_argument("-o", "--out", default="oltp_throughput.png")

    p_ycsb = sub.add_parser("ycsb", help="Plot YCSB throughput over time (one or more workload runs overlaid)")
    p_ycsb.add_argument("csv", nargs="+", help="ycsb_timeseries.csv file(s), one per workload run")
    p_ycsb.add_argument("--labels", nargs="+", help="One label per CSV (default: filename), e.g. A B C D E F")
    p_ycsb.add_argument("-o", "--out", default="ycsb_throughput.png")

    p_scan = sub.add_parser("scan-sweep", help="Plot OLAP scan throughput vs. snapshot delay (scan_delay_sweep mode)")
    p_scan.add_argument("csv", help="tpcc_scan.csv")
    p_scan.add_argument("-o", "--out", default="scan_sweep.png")

    p_ch = sub.add_parser("ch", help="Plot CH-benCHmark/HTAP query latency, staleness, and result value over time")
    p_ch.add_argument("csv", help="tpcc_scan.csv (from a tpch/htap/ch-mode run)")
    p_ch.add_argument("-o", "--out", default="ch_benchmark.png")

    p_interference = sub.add_parser("interference", help="Overlay an OLTP-only baseline vs. OLTP+OLAP mixed run's throughput")
    p_interference.add_argument("baseline_csv", help="tpcc_oltp_timeseries.csv from an OLTP-only run (e.g. `tpcc ... none 0`)")
    p_interference.add_argument("mixed_csv", help="tpcc_oltp_timeseries.csv from the mixed `htap`/`tpch` run")
    p_interference.add_argument("-o", "--out", default="interference.png")

    args = parser.parse_args()

    if args.command is None or args.command == "auto":
        auto(getattr(args, "dir", "."), getattr(args, "out_dir", "plots"))
    elif args.command == "oltp":
        plot_oltp(args.csv, args.labels, args.out)
    elif args.command == "ycsb":
        plot_ycsb(args.csv, args.labels, args.out)
    elif args.command == "scan-sweep":
        plot_scan_sweep(args.csv, args.out)
    elif args.command == "ch":
        plot_ch_benchmark(args.csv, args.out)
    elif args.command == "interference":
        plot_interference(args.baseline_csv, args.mixed_csv, args.out)


if __name__ == "__main__":
    main()
