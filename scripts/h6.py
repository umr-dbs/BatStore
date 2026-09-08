#!/usr/bin/env python3
"""H6: Memory reuse.

Measure local reuse, cross-thread stealing and fresh global-allocator requests
under ycsb_a across OLTP thread counts. gc_stats.csv contains whole-run totals;
gc_stats_after_load.csv is subtracted to exclude initial population.

These are event counts, not allocation timings. A fresh global-allocator
request is not necessarily an OS allocation (jemalloc can reuse memory).
Frequency shares alone cannot establish the fraction of allocation time.

Usage: python3 scripts/h6.py --threads 1,2,4,8,16,32,48,64,80,96,112,128 --duration 60
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib.pyplot as plt

from hypothesis_common import configure_checkout, thread_counts, check_run

configure_checkout()
from engines import batstore, common
from plot_styles import measurement_positions, measurement_values, set_measurement_axis

DEFAULT_THREADS = [1, 2, 4, 8, 16, 32, 48, 64, 80, 96, 112, 128]
EVENT_COLORS = {"steal": "#D55E00", "fresh_alloc": "#009E73"}  # Okabe-Ito vermillion/bluish-green


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
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            for key in totals:
                try:
                    totals[key] += int(row[key])
                except (KeyError, ValueError):
                    continue
    return totals, True


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="h6_results")
    p.add_argument("--threads", default=",".join(str(t) for t in DEFAULT_THREADS))
    p.add_argument("--records", type=int, default=200_000)
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--skip-build", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.skip_build:
        print("[build] batstore (with --features gc-stats)...")
        ensure_built_with_gc_stats()

    threads_list = thread_counts(args.threads)
    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)
    gc_summary_path = run_dir / "h6_gc_stats.csv"
    with open(gc_summary_path, "w", newline="") as f:
        csv.writer(f).writerow([
            "threads", "throughput_ops_sec", "local_reuse", "steal", "fresh_alloc",
            "steal_share", "fresh_alloc_share", "steal_per_thread", "fresh_alloc_per_thread", "local_reuse_share",
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
        steal_share = gc["steal"] / reclaimed if reclaimed else 0.0
        fresh_alloc_share = gc["fresh_alloc"] / all_events if all_events else 0.0
        row = {
            "threads": threads, "throughput": result.primary_metric_value,
            "local_reuse": gc["local_reuse"], "steal": gc["steal"], "fresh_alloc": gc["fresh_alloc"],
            "steal_share": steal_share, "fresh_alloc_share": fresh_alloc_share,
            "steal_per_thread": gc["steal"] / threads, "fresh_alloc_per_thread": gc["fresh_alloc"] / threads,
        }
        rows.append(row)
        with open(gc_summary_path, "a", newline="") as f:
            csv.writer(f).writerow([
                threads, f"{row['throughput']:.2f}", gc["local_reuse"], gc["steal"], gc["fresh_alloc"],
                f"{steal_share:.5f}", f"{fresh_alloc_share:.5f}",
                f"{row['steal_per_thread']:.2f}", f"{row['fresh_alloc_per_thread']:.2f}",
                f"{gc['local_reuse'] / all_events if all_events else 0.0:.5f}",
            ])
        status = result.notes or "OK"
        print(f"threads={threads:3d}  throughput={result.primary_metric_value:10.1f} ops/s  "
              f"local_reuse={gc['local_reuse']:9d}  steal={gc['steal']:6d} ({steal_share:.3%})  "
              f"fresh_alloc={gc['fresh_alloc']:6d} ({fresh_alloc_share:.3%})  [{status}]")

    print(f"\nmanifest  : {manifest_path}")
    print(f"gc stats  : {gc_summary_path}")
    plot(rows, run_dir / "plots")


def plot(rows: list, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    threads_list = [r["threads"] for r in rows]
    axis_values = measurement_values(threads_list)
    positions = measurement_positions(threads_list, axis_values)

    fig, (ax_share, ax_per_thread) = plt.subplots(1, 2, figsize=(13, 5))

    ax_share.plot(positions, [r["steal_share"] * 100.0 for r in rows], color=EVENT_COLORS["steal"],
                  marker="o", markersize=6, markeredgecolor="white", markeredgewidth=0.7, linewidth=2.0,
                  label="steal share (of reclaims)")
    ax_share.plot(positions, [r["fresh_alloc_share"] * 100.0 for r in rows], color=EVENT_COLORS["fresh_alloc"],
                  marker="s", markersize=6, markeredgecolor="white", markeredgewidth=0.7, linewidth=2.0,
                  label="fresh-alloc share (of all events)")
    set_measurement_axis(ax_share, threads_list, "OLTP threads")
    ax_share.set_ylabel("share of GC events (%)")
    ax_share.set_title("Steal / fresh-alloc share vs. thread count")
    ax_share.legend(frameon=False)

    ax_per_thread.plot(positions, [r["steal_per_thread"] for r in rows], color=EVENT_COLORS["steal"],
                        marker="o", markersize=6, markeredgecolor="white", markeredgewidth=0.7, linewidth=2.0,
                        label="steals / thread")
    ax_per_thread.plot(positions, [r["fresh_alloc_per_thread"] for r in rows], color=EVENT_COLORS["fresh_alloc"],
                        marker="s", markersize=6, markeredgecolor="white", markeredgewidth=0.7, linewidth=2.0,
                        label="fresh allocs / thread")
    set_measurement_axis(ax_per_thread, threads_list, "OLTP threads")
    ax_per_thread.set_ylabel("events per thread")
    ax_per_thread.set_yscale("symlog", linthresh=1)
    ax_per_thread.set_title("Per-thread steal/fresh-allocation events")
    ax_per_thread.legend(frameon=False)

    fig.suptitle("H6: Memory reuse (ycsb_a, gc=on)")
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h6_memory_reuse.{ext}", dpi=150)
    plt.close(fig)
    print(f"plots     : {out_dir}")


if __name__ == "__main__":
    main()
