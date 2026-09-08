#!/usr/bin/env python3
"""H6: GC cost is a small, roughly-fixed cost per OLTP thread.

    H6) Die Allokation von Speicher kann bei BatStore durch lokale
        GC-Listen erfolgen. Die Anzahl der Aufrufe fuer Stealing (also
        Entnahme von Seiten aus der GC anderer Threads) und die Allokation
        ueber das Betriebssystem ist niedrig und hat wenig Einfluss auf die
        Gesamtkosten.

BatStore's block allocator (src/bat_gc/tracker_handle.rs) frees a page by
trying, in order: (1) its own per-shard reuse cache, (2) its own shard's
dead-page queue, (3) *stealing* from another shard's dead-page queue
(src/bat_gc/block_tracer.rs::reclaim_batch), and only if all three come up
empty, (4) a real allocation from the global allocator
(src/bat_sync/block_sync.rs::into_cell). Until this script's own change,
none of this was counted anywhere - `local_reuse`/`steal`/`fresh_alloc`
atomic counters (one triple per shard) and a `gc_stats.csv` dump were added
to bat_gc/tracker_handle.rs, bat_block/block_handle.rs, and
bat_bench/ycsb_driver.rs specifically to make this hypothesis measurable.

These counters sit behind the `gc-stats` Cargo feature (off by default - see
its doc in Cargo.toml): a normal release build never pays the extra atomic
RMWs on the block-alloc/reclaim hot path. This script therefore builds its
own binary with `--features mdbx-backend,gc-stats` instead of using
`batstore.ensure_built()` (which only asks for `mdbx-backend`) - every other
h*.py script's binary is unaffected/unchanged by this.

We run ycsb_a (50% read / 50% update - update is what dies+reuses pages)
with GC on across an increasing OLTP thread count, and check:

  - steal_share = steal / (local_reuse + steal)          - should stay LOW
  - fresh_alloc_share = fresh_alloc / all reclaim events  - should stay LOW
  - steal / threads, fresh_alloc / threads                - should stay ~FLAT
    (not grow superlinearly) as threads increases - the "fixed cost per
    thread" claim.

Caveat: gc_stats.csv accumulates over the WHOLE run including the initial
population/load phase (which is a single-threaded burst of fresh
allocations, independent of the timed phase's thread count) - we use a long
enough timed-phase duration relative to record_count that this fixed,
one-time cost doesn't dominate the totals.

Usage:
    python3 scripts/h6.py
    python3 scripts/h6.py --threads 1,2,4,8,16,32 --duration 30 --records 200000
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

from engines import batstore, common
from plot_styles import measurement_positions, measurement_values, set_measurement_axis

DEFAULT_THREADS = [1, 2, 4, 8, 16, 32]
EVENT_COLORS = {"steal": "#D55E00", "fresh_alloc": "#009E73"}  # Okabe-Ito vermillion/bluish-green


def ensure_built_with_gc_stats() -> None:
    """Same as batstore.ensure_built(), plus the `gc-stats` feature (off by default - see
    its doc in Cargo.toml) so gc_stats.csv actually gets written.
    """
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
    p.add_argument("--duration", type=int, default=30)
    p.add_argument("--skip-build", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.skip_build:
        print("[build] batstore (with --features gc-stats)...")
        ensure_built_with_gc_stats()

    threads_list = [int(t) for t in args.threads.split(",") if t.strip()]
    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)
    gc_summary_path = run_dir / "h6_gc_stats.csv"
    with open(gc_summary_path, "w", newline="") as f:
        csv.writer(f).writerow([
            "threads", "throughput_ops_sec", "local_reuse", "steal", "fresh_alloc",
            "steal_share", "fresh_alloc_share", "steal_per_thread", "fresh_alloc_per_thread",
        ])

    print("\n########## H6: GC cost per OLTP thread ##########")
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

        gc, found = read_gc_stats(out_dir / "gc_stats.csv")
        if not found:
            sys.exit(f"{out_dir / 'gc_stats.csv'} not found - was the binary built with "
                     f"--features gc-stats? (pass --skip-build only if you already did this yourself)")
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
    ax_per_thread.set_yscale("log")
    ax_per_thread.set_title("Per-thread steal/fresh-alloc cost vs. thread count")
    ax_per_thread.legend(frameon=False)

    fig.suptitle("H6: GC cost per OLTP thread (ycsb_a, gc=on)")
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"h6_gc_cost.{ext}", dpi=150)
    plt.close(fig)
    print(f"plots     : {out_dir}")


if __name__ == "__main__":
    main()
