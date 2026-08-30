#!/usr/bin/env python3
"""Plot every supported benchmark result found in a directory.

The directory format is detected automatically.  This replaces the need to
remember which plotting script belongs to comparison, suite, HTAP analytical,
skew-sweep, or direct Rust CSV output.

Examples:
    python3 scripts/plot.py comparison_results/run_20260101_120000
    python3 scripts/plot.py benchmark_results/run_20260101_120000
    python3 scripts/plot.py .
"""

import argparse
import csv
import json
from pathlib import Path

import plot_compare
import plot_htap_analytical
import plot_results
import plot_skew_sweep
import plot_suite


KINDS = ("auto", "comparison", "htap", "skew", "suite", "raw")


def _manifest_sample(manifest_path: Path) -> tuple[set[str], list[str]]:
    with manifest_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        fields = set(reader.fieldnames or [])
        labels = []
        for row in reader:
            labels.append(row.get("config_label", ""))
            if len(labels) == 100:
                break
    return fields, labels


def _run_config(run_dir: Path) -> dict:
    path = run_dir / "run_config.json"
    if not path.exists():
        return {}
    try:
        with path.open() as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def detect_kind(run_dir: Path) -> str:
    """Return the plotting format used by *run_dir*."""
    manifest_path = run_dir / "manifest.csv"
    if not manifest_path.exists():
        return "raw"

    fields, labels = _manifest_sample(manifest_path)
    config = _run_config(run_dir)
    if "experiment" in fields:
        return "suite"
    if "skews" in config or any("skew=" in label for label in labels):
        return "skew"
    if "olap_threads" in config or any("olap_threads=" in label for label in labels):
        return "htap"
    return "comparison"


def plot_comparison(run_dir: Path, engine: str | None) -> None:
    manifest = plot_compare.load_manifest(run_dir)
    if engine:
        available = sorted(manifest["engine"].dropna().unique())
        if engine not in available:
            raise SystemExit(
                f"Engine '{engine}' is not present in {run_dir / 'manifest.csv'} "
                f"(available: {', '.join(available) or 'none'})"
            )
        manifest = manifest[manifest["engine"] == engine].copy()

    out_dir = run_dir / "plots"
    plot_compare.prepare_output_dir(out_dir)
    overview_name = f"single_engine_overview_{engine}" if engine else "single_engine_overview"
    if plot_compare.plot_single_engine_overview(manifest, out_dir, overview_name):
        return

    plot_compare.plot_all_engines_workload_overview(manifest, out_dir)
    ref_threads = 0
    for gc_choice in ("on", "off"):
        ref_slice, ref_threads = plot_compare.pick_reference_slice(manifest, gc_choice)
        plot_compare.plot_tpcc_throughput(ref_slice, ref_threads, gc_choice, out_dir)
        plot_compare.plot_ycsb_throughput(ref_slice, ref_threads, gc_choice, out_dir)
        plot_compare.plot_memory_usage(ref_slice, ref_threads, gc_choice, out_dir)
        plot_compare.plot_summary_all(ref_slice, ref_threads, gc_choice, out_dir)
    plot_compare.plot_throughput_vs_threads_ycsb(manifest, out_dir)
    plot_compare.plot_throughput_vs_threads_tpcc(manifest, out_dir)
    plot_compare.plot_throughput_vs_threads_htap(manifest, out_dir)
    plot_compare.plot_gc_comparison(manifest, ref_threads, out_dir)
    plot_compare.plot_scan_latency(manifest, ref_threads, out_dir)
    plot_compare.plot_batstore_olap_scan_latency(run_dir, ref_threads, out_dir)
    plot_compare.plot_htap_interference(manifest, ref_threads, out_dir)
    plot_compare.plot_ch_query_latency(manifest, ref_threads, out_dir)


def plot_htap(run_dir: Path) -> None:
    manifest = plot_htap_analytical.load_manifest(run_dir)
    out_dir = run_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    for workload in plot_htap_analytical.HTAP_WORKLOADS:
        workload_df = manifest[manifest["workload"] == workload]
        for gc_choice, gc_df in plot_htap_analytical.gc_slices(workload_df):
            plot_htap_analytical.plot_workload_per_engine(
                gc_df, workload, gc_choice, out_dir
            )
            plot_htap_analytical.plot_workload_all_engines(
                gc_df, workload, gc_choice, out_dir
            )
            plot_htap_analytical.plot_workload_latency_per_engine(
                gc_df, workload, gc_choice, out_dir
            )
            plot_htap_analytical.plot_workload_latency_all_engines(
                gc_df, workload, gc_choice, out_dir
            )


def plot_skew(run_dir: Path, requested_ref_threads: int | None) -> None:
    manifest = plot_skew_sweep.load_manifest(run_dir)
    out_dir = run_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    for workload in plot_skew_sweep.YCSB_WORKLOADS:
        workload_df = manifest[manifest["workload"] == workload]
        if workload_df.empty:
            continue
        ref_threads = requested_ref_threads or int(workload_df["threads"].max())
        for gc_choice, gc_df in plot_skew_sweep.gc_slices(workload_df):
            plot_skew_sweep.plot_workload_per_engine(
                gc_df, workload, gc_choice, out_dir,
            )
            plot_skew_sweep.plot_workload_all_engines(
                gc_df, workload, gc_choice, out_dir, ref_threads,
            )
    if not manifest.empty:
        plot_skew_sweep.plot_overviews(manifest, out_dir)


def plot_benchmark_suite(run_dir: Path) -> None:
    manifest = plot_suite.load_manifest(run_dir)
    out_dir = run_dir / "plots"
    plot_suite.plot_oltp_throughput(run_dir, out_dir)
    plot_suite.plot_htap_interference(manifest, out_dir)
    plot_suite.plot_ch_benchmark(run_dir, out_dir)
    plot_suite.plot_ycsb_by_workload(manifest, out_dir)
    plot_suite.plot_memory_usage(run_dir, manifest, out_dir)
    plot_suite.plot_summary_all(manifest, out_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directory", nargs="?", default=Path("."), type=Path,
        help="result/run directory to plot (default: current directory)",
    )
    parser.add_argument(
        "--kind", choices=KINDS, default="auto",
        help="input format override (default: detect automatically)",
    )
    parser.add_argument("--engine", help="comparison plots: select one engine")
    parser.add_argument(
        "--ref-threads", type=int,
        help="skew plots: thread count for the all-engine overlay (default: maximum)",
    )
    args = parser.parse_args()

    run_dir = args.directory.resolve()
    if not run_dir.is_dir():
        raise SystemExit(f"{run_dir} is not a directory")

    kind = detect_kind(run_dir) if args.kind == "auto" else args.kind
    print(f"Detected {kind} results in {run_dir}")
    if kind == "comparison":
        plot_comparison(run_dir, args.engine)
    elif kind == "htap":
        plot_htap(run_dir)
    elif kind == "skew":
        plot_skew(run_dir, args.ref_threads)
    elif kind == "suite":
        plot_benchmark_suite(run_dir)
    else:
        out_dir = run_dir / "plots"
        out_dir.mkdir(parents=True, exist_ok=True)
        plot_results.auto(run_dir, out_dir)
    print(f"All applicable figures written to {run_dir / 'plots'}")


if __name__ == "__main__":
    main()
