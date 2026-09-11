#!/usr/bin/env python3
"""H6: Memory reuse.

Measure local reuse, cross-thread stealing and fresh global-allocator requests
under ycsb_a across OLTP thread counts. gc_stats.csv contains whole-run totals;
gc_stats_after_load.csv is subtracted to exclude initial population.

These are event counts, not allocation timings. A fresh global-allocator
request is not necessarily an OS allocation (jemalloc can reuse memory).
Frequency shares alone cannot establish the fraction of allocation time.
Each handed-out page is counted once. Prefetched pages keep their original
local/cross-shard source while cached and are counted only when consumed.
The plot uses all allocation events as the denominator for all three sources.
CSV steal_share retains the legacy reuse-only denominator; use
steal_all_events_share to compare against local_reuse_share/fresh_alloc_share.

Usage: python3 scripts/h6.py --threads 1,2,4,8,16,32,48,64,80,96,112,128 --duration 60
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib.pyplot as plt

from hypothesis_common import configure_checkout, thread_counts, check_run, positive_int, record_setup

configure_checkout()
from engines import batstore, common
from plot_styles import (compact_enabled, finalize_layout, measurement_positions,
                         measurement_values, set_compact, set_measurement_axis)

DEFAULT_THREADS = [1, 2, 4, 8, 16, 32, 48, 64, 80, 96, 112, 128]
EVENT_COLORS = {
    "local_reuse": "#2A9D8F",  # teal
    "steal": "#E9C46A",        # gold
    "fresh_alloc": "#E76F51",  # coral
    "throughput": "#B07AA1",   # purple
}  # color-vision-friendly palette


def ensure_built_with_gc_stats() -> None:
    """Same as batstore.ensure_built(), plus the `gc-stats` feature (off by default - see
    its doc in Cargo.toml) so gc_stats.csv actually gets written.
    """
    import json
    metadata = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=batstore.REPO_ROOT, check=True, capture_output=True, text=True,
    )
    packages = json.loads(metadata.stdout)["packages"]
    if not any(p["name"] == "BatStore" and "gc-stats" in p["features"] for p in packages):
        sys.exit(f"{batstore.REPO_ROOT}: missing gc-stats feature; update the complete checkout, "
                 "including Cargo.toml and src/, or correct BATSTORE_REPO.")
    subprocess.run(
        common.batstore_cargo_build_args("mdbx-backend", "gc-stats"),
        cwd=batstore.REPO_ROOT, check=True,
    )


def read_gc_stats(csv_path: Path) -> tuple:
    """Sums the per-shard gc_stats.csv (see ycsb_driver.rs::write_gc_stats) into totals.
    Returns (totals, found) - `found` is False if the file doesn't exist at all, which
    means the binary wasn't built with the `gc-stats` feature (as opposed to a real run
    that simply had zero reclaim activity).
    """
    totals = {"local_reuse": 0, "steal": 0, "fresh_alloc": 0}
    if not csv_path.exists():
        return totals, False
    seen = set()
    try:
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                if row.get("schema_version") != "2":
                    raise ValueError("requires allocation-source counter schema 2; rebuild Rust")
                shard = int(row["shard"])
                if shard in seen:
                    raise ValueError("duplicate shard")
                seen.add(shard)
                for key in totals:
                    value = int(row[key])
                    if value < 0:
                        raise ValueError("negative counter")
                    totals[key] += value
        if not seen:
            raise ValueError("empty counter file")
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise SystemExit(f"{csv_path}: invalid GC counters: {exc}")
    return totals, True


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="h6_results")
    p.add_argument("--threads", default=",".join(str(t) for t in DEFAULT_THREADS))
    p.add_argument("--records", type=positive_int, default=200_000)
    p.add_argument("--duration", type=positive_int, default=60)
    p.add_argument("--mode", choices=["atomic", "transaction"], default="atomic", help="fixed YCSB execution mode")
    p.add_argument("--skip-build", action="store_true")
    p.add_argument("--compact", action="store_true", help="use a paper-friendly layout with a shared legend")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    set_compact(args.compact)
    os.environ["BATSTORE_YCSB_MODE"] = args.mode
    threads_list = thread_counts(args.threads)
    if not args.skip_build:
        print("[build] batstore (with --features gc-stats)...")
        ensure_built_with_gc_stats()

    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    record_setup(run_dir, "H6", args, varies=f"OLTP threads={threads_list}",
                 fixed=f"YCSB A, mode={args.mode}, records={args.records}, duration={args.duration}s, GC=on, theta=0.99",
                 measures="allocation-source counts excluding load; shares are frequencies, not allocation-time fractions")
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)
    gc_summary_path = run_dir / "h6_gc_stats.csv"
    with open(gc_summary_path, "w", newline="") as f:
        csv.writer(f).writerow([
            "threads", "throughput_ops_sec", "local_reuse", "steal", "fresh_alloc",
            "steal_share", "fresh_alloc_share", "steal_per_thread", "fresh_alloc_per_thread", "local_reuse_share", "steal_all_events_share", "all_events",
        ])

    print("\n########## H6: Memory reuse ##########")
    print(f"run directory : {run_dir}")
    print(f"threads       : {threads_list}")
    print(f"records={args.records} duration={args.duration}s workload=ycsb_a gc=on")
    print("###################################################\n")

    scale_base = common.Scale(ycsb_records=args.records, ycsb_duration=args.duration)

    rows = []
    for threads in threads_list:
        scale = dataclasses.replace(scale_base, ycsb_threads=threads, label=f"h6 threads={threads}")
        out_dir = run_dir / f"threads_{threads}"
        result = batstore.run("ycsb_a", scale, out_dir, gc="on")
        common.append_manifest_row(manifest_path, result)
        check_run(result, out_dir)

        gc, found = read_gc_stats(out_dir / "gc_stats.csv")
        if not found:
            sys.exit(f"{out_dir / 'gc_stats.csv'} not found - was the binary built with "
                     f"--features gc-stats? (pass --skip-build only if you already did this yourself)")
        loaded, baseline_found = read_gc_stats(out_dir / "gc_stats_after_load.csv")
        if not baseline_found:
            sys.exit("Missing gc_stats_after_load.csv; rebuild with the updated Rust sources.")
        gc = {key: gc[key] - loaded[key] for key in gc}
        if any(value < 0 for value in gc.values()):
            sys.exit("Invalid GC counters: final totals are below the post-load baseline.")
        reclaimed = gc["local_reuse"] + gc["steal"]
        all_events = reclaimed + gc["fresh_alloc"]
        if all_events == 0:
            sys.exit("No allocation events after loading; increase duration or workload size")
        steal_share = gc["steal"] / reclaimed if reclaimed else 0.0
        fresh_alloc_share = gc["fresh_alloc"] / all_events if all_events else 0.0
        row = {
            "threads": threads, "throughput": result.primary_metric_value,
            "local_reuse": gc["local_reuse"], "steal": gc["steal"], "fresh_alloc": gc["fresh_alloc"],
            "local_reuse_share": gc["local_reuse"] / all_events,
            "steal_all_events_share": gc["steal"] / all_events,
            "steal_share": steal_share, "fresh_alloc_share": fresh_alloc_share,
            "steal_per_thread": gc["steal"] / threads, "fresh_alloc_per_thread": gc["fresh_alloc"] / threads,
        }
        rows.append(row)
        with open(gc_summary_path, "a", newline="") as f:
            csv.writer(f).writerow([
                threads, f"{row['throughput']:.2f}", gc["local_reuse"], gc["steal"], gc["fresh_alloc"],
                f"{steal_share:.5f}", f"{fresh_alloc_share:.5f}",
                f"{row['steal_per_thread']:.2f}", f"{row['fresh_alloc_per_thread']:.2f}",
                f"{gc['local_reuse'] / all_events:.5f}",
                f"{gc['steal'] / all_events:.5f}", all_events,
            ])
        status = result.notes or "OK"
        print(f"threads={threads:3d}  throughput={result.primary_metric_value:10.1f} ops/s  "
              f"local_reuse={gc['local_reuse']:9d}  steal={gc['steal']:6d} ({row['steal_all_events_share']:.3%} of allocations)  "
              f"fresh_alloc={gc['fresh_alloc']:6d} ({fresh_alloc_share:.3%})  [{status}]")

    print(f"\nmanifest  : {manifest_path}")
    print(f"gc stats  : {gc_summary_path}")
    plot(rows, run_dir / "plots", duration_s=args.duration)


def plot(rows: list, out_dir: Path, duration_s: int = 60) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if compact_enabled():
        compact_threads = {1, 2, 4, 8, 16, 32, 64, 128}
        rows = [row for row in rows if int(row["threads"]) in compact_threads]
    threads_list = [r["threads"] for r in rows]
    axis_values = measurement_values(threads_list)
    positions = measurement_positions(threads_list, axis_values)

    figsize = (10.5, 2.6) if compact_enabled() else (13, 5)
    fig, (ax_share, ax_throughput) = plt.subplots(1, 2, figsize=figsize)
    local_share = [r["local_reuse_share"] * 100.0 for r in rows]
    cross_share = [r["steal_all_events_share"] * 100.0 for r in rows]
    ax_share.bar(positions, local_share, width=0.72, color=EVENT_COLORS["local_reuse"],
                 edgecolor="white", linewidth=0.6, label="Local reuse")
    ax_share.bar(positions, cross_share, width=0.72, bottom=local_share,
                 color=EVENT_COLORS["steal"], edgecolor="white", linewidth=0.6,
                 label="Cross-worker reuse")
    reuse_share = [local + cross for local, cross in zip(local_share, cross_share)]
    # Draw allocator last and force it to end exactly at 100%; it is the
    # remainder after both reuse sources, not a layer between them.
    allocator_remainder = [max(0.0, 100.0 - reuse) for reuse in reuse_share]
    ax_share.bar(positions, allocator_remainder, width=0.72, bottom=reuse_share,
                 color=EVENT_COLORS["fresh_alloc"], edgecolor="white", linewidth=0.6,
                 label="_Allocator")
    set_measurement_axis(ax_share, threads_list, "OLTP threads")
    ax_share.set_ylim(0, 100)
    ax_share.set_ylabel("Allocation share (%)")
    ax_share.set_title("Allocation-source composition", pad=9)
    ax_share.grid(axis="y", alpha=0.25)

    throughput_ops_sec = [
        float(r.get("throughput_ops_sec", r.get("throughput"))) for r in rows
    ]
    allocator_per_million_ops = [
        r["fresh_alloc"] / (throughput * duration_s) * 1_000_000
        for r, throughput in zip(rows, throughput_ops_sec)
    ]
    bars = ax_throughput.bar(positions, allocator_per_million_ops, width=0.68,
                             color=EVENT_COLORS["fresh_alloc"],
                             edgecolor="white", linewidth=0.6, label="Allocator")
    for bar, value in zip(bars, allocator_per_million_ops):
        ax_throughput.text(bar.get_x() + bar.get_width() / 2, value,
                           f"{value:.2f}", ha="center", va="bottom", fontsize=7,
                           color="#333333")
    set_measurement_axis(ax_throughput, threads_list, "OLTP threads")
    ax_throughput.set_ylim(0, max(allocator_per_million_ops) * 1.16)
    ax_throughput.set_ylabel("Allocator requests\nper million ops")
    ax_throughput.set_title("Global allocator pressure", pad=9)
    ax_throughput.grid(axis="y", alpha=0.25)

    if compact_enabled():
        share_handles, share_labels = ax_share.get_legend_handles_labels()
        allocator_handles, allocator_labels = ax_throughput.get_legend_handles_labels()
        fig.legend(share_handles, share_labels, loc="upper center",
                   bbox_to_anchor=(0.255, 0.995), ncol=2, frameon=False,
                   fontsize=9, columnspacing=1.2,
                   handletextpad=0.5)
        fig.legend(allocator_handles, allocator_labels, loc="upper center",
                   bbox_to_anchor=(0.755, 0.995), ncol=1, frameon=False,
                   fontsize=9, handletextpad=0.5)
        fig.tight_layout(rect=(0, 0, 1, 0.92), w_pad=2.8)
        fig.text(0.5, 0.075, "YCSB A", ha="center", va="center",
                 fontsize=10, fontweight="semibold")
    else:
        ax_share.legend(frameon=False, loc="upper right")
        ax_throughput.legend(frameon=False, loc="upper right")
        fig.suptitle("H6: YCSB A memory reuse (GC on)")
        finalize_layout(fig)
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h6_memory_reuse.{ext}", dpi=150)
    plt.close(fig)
    print(f"plots     : {out_dir}")


if __name__ == "__main__":
    main()
