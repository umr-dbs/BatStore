#!/usr/bin/env python3
"""Cross-engine benchmark harness: runs TPC-C, YCSB A-F, and HTAP/CH-benCHmark
(htap_q1/htap_q6) against cMVBT, LeanStore, LeanStore's WiredTiger adapter,
and PostgreSQL (via BenchBase), sweeping thread/terminal count and GC on/off,
all NUMA-pinned to one node (`numactl --cpubind=0 --membind=0`, see
engines/common.py::run_and_track_rss), normalizing every result into one
manifest.csv for scripts/plot_compare.py.

Sized for the real server (2x AMD EPYC 7742, 64 cores/128 threads per socket,
2 NUMA nodes) - the default thread sweep (1..128) is one socket's worth of
SMT threads, matching --cpubind=0. Not every engine has a real GC toggle: see
engines/{leanstore,wiredtiger}.py's SUPPORTS_GC_TOGGLE = False (LeanStore's
--pgc flag is dead code in this checkout, and the WiredTiger adapter has no
equivalent at all) - those two engines run once per (workload, threads) with
gc_enabled="n/a" regardless of --gc. cMVBT (real --gc flag) and PostgreSQL
(via autovacuum, see engines/postgres_benchbase.py) get a real on/off compare.

htap_q1/htap_q6 (see common.py's HTAP_WORKLOADS) run TPC-C OLTP concurrently
with one dedicated thread repeatedly executing CH-benCHmark Q1 ("Pricing
Summary Report") or Q6 ("Forecasting Revenue Change") - the only 2 of
CH-benCHmark's 22 queries genuinely implemented across every engine here
(cmvbt, leanstore, wiredtiger, postgres, libmdbx, and both vweaver_ermia
variants - see manual.txt section 5; vweaver_ermia_frugal has a separate,
pre-existing KNOWN ISSUE there that crashes it on any sustained workload).
Compare
htap_q1/htap_q6's OLTP throughput against plain "tpcc" at the same
threads/gc (see plot_compare.py::plot_htap_interference) for the interference
analytics puts on OLTP - that derived comparison *is* this harness's "HTAP"
measurement, no separate baseline sub-phase needed.

Usage:
    python3 scripts/compare_engines.py
    python3 scripts/compare_engines.py --tiny --threads 2,4
    python3 scripts/compare_engines.py --engines cmvbt,leanstore --workloads tpcc,ycsb_e
    python3 scripts/compare_engines.py --threads 1,4,16,64 --gc on
    python3 scripts/compare_engines.py --warehouses 16 --tpcc-duration 120
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from engines import (
    cmvbt, common, leanstore, libmdbx, postgres_benchbase, vweaver_ermia,
    vweaver_ermia_frugal, wiredtiger,
)

ENGINE_MODULES = {
    "cmvbt": cmvbt,
    "leanstore": leanstore,
    "wiredtiger": wiredtiger,
    "postgres": postgres_benchbase,
    "vweaver_ermia": vweaver_ermia,
    "vweaver_ermia_frugal": vweaver_ermia_frugal,
    "libmdbx": libmdbx,
}

# One socket's worth of SMT threads on the real server (2x AMD EPYC 7742, 64 cores/128
# threads per socket) - matches --cpubind=0 pinning to a single node. Starts at 2 (not 1)
# per the user's own thread-sweep spec.
DEFAULT_THREADS = [2, 4, 8, 16, 32, 64, 128]
DEFAULT_GC = ["on", "off"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="comparison_results")
    p.add_argument("--engines", default=",".join(common.ENGINES),
                   help=f"comma-separated subset of {common.ENGINES}")
    p.add_argument("--workloads", default=",".join(common.ALL_WORKLOADS),
                   help=f"comma-separated subset of {common.ALL_WORKLOADS}")
    p.add_argument("--tiny", action="store_true", help="use TINY_SCALE (smoke test) instead of the server scale")
    p.add_argument("--skip-build", action="store_true", help="skip each engine's ensure_built() step")
    p.add_argument("--cmvbt-allocator", choices=["jemalloc", "mimalloc"], default="jemalloc",
                   help="global allocator cMVBT's binary is built with (see Cargo.toml's `mimalloc` "
                        "feature) - 'jemalloc' is the crate's own default; 'mimalloc' measured a few %% "
                        "faster on YCSB's WAL path but a few %% slower on TPC-C, so it's opt-in here too. "
                        "Only affects the cmvbt/libmdbx engines, which share one binary.")

    p.add_argument("--threads", default=None,
                   help=f"comma-separated thread/terminal counts to sweep (default {DEFAULT_THREADS}, "
                        f"or [2,4] with --tiny)")
    p.add_argument("--gc", default=",".join(DEFAULT_GC),
                   help="comma-separated subset of on,off - ignored for engines with no working "
                        "GC toggle (see SUPPORTS_GC_TOGGLE in each engines/*.py)")

    p.add_argument("--warehouses", type=int)
    p.add_argument("--tpcc-duration", type=int)
    p.add_argument("--ycsb-records", type=int)
    p.add_argument("--ycsb-duration", type=int)
    p.add_argument("--theta", type=float)
    p.add_argument("--dram-gib", type=float)
    return p.parse_args()


def build_scale(args: argparse.Namespace) -> common.Scale:
    scale = dataclasses.replace(common.TINY_SCALE) if args.tiny else common.Scale()
    overrides = {
        "tpcc_warehouses": args.warehouses,
        "tpcc_duration": args.tpcc_duration,
        "ycsb_records": args.ycsb_records,
        "ycsb_duration": args.ycsb_duration,
        "ycsb_theta": args.theta,
        "dram_gib": args.dram_gib,
    }
    for field, value in overrides.items():
        if value is not None:
            setattr(scale, field, value)
    return scale


def _workload_duration(workload: str, scale: common.Scale) -> float:
    # htap_q1/htap_q6 run TPC-C's driver underneath (with a concurrent CH-benCHmark OLAP
    # thread) - same duration knob as plain "tpcc", not YCSB's.
    if workload in (["tpcc"] + common.HTAP_WORKLOADS):
        return scale.tpcc_duration
    return scale.ycsb_duration


def main() -> None:
    args = parse_args()
    # Read fresh by common.cmvbt_cargo_build_args() inside cmvbt.py/libmdbx.py's own
    # ensure_built() - see that function's doc for why this is an env var, not a direct
    # module attribute, and why it's set unconditionally here even if "cmvbt"/"libmdbx"
    # aren't in --engines (harmless: the var is simply never read in that case).
    os.environ["CMVBT_ALLOCATOR"] = args.cmvbt_allocator
    scale = build_scale(args)
    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]
    gc_list = [g.strip() for g in args.gc.split(",") if g.strip()]
    if args.threads:
        thread_list = [int(t.strip()) for t in args.threads.split(",") if t.strip()]
    else:
        thread_list = [2, 4] if args.tiny else list(DEFAULT_THREADS)

    for e in engines:
        if e not in ENGINE_MODULES:
            sys.exit(f"unknown engine '{e}' (expected one of {list(ENGINE_MODULES)})")
    for w in workloads:
        if w not in common.ALL_WORKLOADS:
            sys.exit(f"unknown workload '{w}' (expected one of {common.ALL_WORKLOADS})")
    for g in gc_list:
        if g not in ("on", "off"):
            sys.exit(f"unknown --gc value '{g}' (expected 'on' and/or 'off')")

    # Must be absolute: each engine wrapper spawns its subprocess with a different cwd
    # (output_dir for leanstore/wiredtiger, BENCHBASE_HOME for postgres), so a relative
    # run_dir would have its derived paths (ssd_path, config_path, ...) re-resolved
    # against the wrong directory when passed as a command-line argument.
    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)

    total_runs = 0
    total_secs = 0.0
    for workload in workloads:
        duration = _workload_duration(workload, scale)
        for engine_name in engines:
            supports_gc = getattr(ENGINE_MODULES[engine_name], "SUPPORTS_GC_TOGGLE", False)
            n_gc = len(gc_list) if supports_gc else 1
            total_runs += len(thread_list) * n_gc
            total_secs += len(thread_list) * n_gc * duration

    print("\n########## cross-engine benchmark comparison ##########")
    print(f"run directory : {run_dir}")
    print(f"scale         : {scale.label} (warehouses={scale.tpcc_warehouses}, "
          f"ycsb_records={scale.ycsb_records})")
    print(f"engines       : {engines}")
    print(f"cmvbt alloc   : {args.cmvbt_allocator} (ignored unless 'cmvbt'/'libmdbx' is in --engines)")
    print(f"workloads     : {workloads}")
    print(f"threads sweep : {thread_list}")
    print(f"gc sweep      : {gc_list} (engines with no working GC toggle always run once, gc=n/a)")
    print(f"NUMA pinning  : numactl --cpubind={common.NUMA_NODE} --membind={common.NUMA_NODE} "
          f"(every engine subprocess; the Postgres *server* itself is not pinned - see "
          f"engines/postgres_benchbase.py's module docstring)")
    dram_gib_desc = (
        f"--dram-gib={args.dram_gib} (explicit, applied to every workload/threads point unchanged)"
        if args.dram_gib is not None else
        f"auto, sized per-workload to ~4x its own estimated dataset (see common.dram_gib_for), "
        f"capped at {common.default_dram_gib()} (this machine's NUMA-node-safe ceiling)"
    )
    print(f"in-memory only: SCRATCH_ROOT={common.SCRATCH_ROOT} (tmpfs-verified; LeanStore/WiredTiger/"
          f"libmdbx/vWeaver_ermia/cmvbt data never touches a real disk) dram_gib: {dram_gib_desc} "
          f"(LeanStore/WiredTiger buffer pool - not used by cmvbt, whose WAL is forced on but "
          f"unbounded like every other in-memory structure here, or postgres, whose shared_buffers "
          f"isn't managed by this script)")
    print(f"planned runs  : {total_runs} (>= {total_secs / 60:.1f} min of measured time alone, "
          f"excluding load/build/BenchBase-client overhead)")
    print("#########################################################\n")

    if not args.skip_build:
        failed_to_build = []
        for name in engines:
            ensure_built = getattr(ENGINE_MODULES[name], "ensure_built", None)
            if not ensure_built:
                continue
            print(f"[build] {name}...")
            try:
                ensure_built()
            except SystemExit as e:
                # An engine's own ensure_built() can deliberately refuse to build (e.g.
                # vweaver_ermia.py's known, documented upstream blocker - see manual.txt)
                # rather than emit a possibly-wrong binary. One engine's build failure
                # shouldn't abort the whole comparison matrix, same reasoning as run()'s
                # own try/except below.
                print(f"    [build] {name} FAILED - excluding it from this run: {e}")
                failed_to_build.append(name)
            except subprocess.CalledProcessError as e:
                print(f"    [build] {name} FAILED - excluding it from this run: {e}")
                failed_to_build.append(name)
        engines = [e for e in engines if e not in failed_to_build]
        if not engines:
            sys.exit("No engines left to run after build failures - see above.")

    for workload in workloads:
        for engine_name in engines:
            module = ENGINE_MODULES[engine_name]
            supports_gc = getattr(module, "SUPPORTS_GC_TOGGLE", False)
            gc_variants = gc_list if supports_gc else ["n/a"]
            # Only meaningful for postgres_benchbase.py: True exactly once per
            # (workload, engine), so its expensive --create/--load only runs on the first
            # sweep point and every later (threads, gc) combo reuses that loaded data - see
            # postgres_benchbase.run()'s `reload` docstring. Every other engine ignores it.
            first_call = True
            for threads in thread_list:
                # TPC-C spec sizes populations at ~10 terminals per warehouse; a fixed
                # warehouse count while terminals sweep up to 128 would push the
                # terminals/warehouse ratio to 16:1 at the top end - far more contention
                # than the spec's intended range, and a likely contributor to cMVBT's
                # observed panic at threads=128 (see task_89ebda8a). Scaling warehouses
                # with threads keeps contention roughly constant across the sweep, so it
                # measures throughput vs. concurrency without confounding it with
                # ever-increasing contention.
                tpcc_warehouses = max(scale.tpcc_warehouses, -(-threads // 10))
                scale_variant = dataclasses.replace(
                    scale, tpcc_terminals=threads, tpcc_warehouses=tpcc_warehouses, ycsb_threads=threads,
                )
                if args.dram_gib is None:
                    # No explicit --dram-gib: re-size the buffer pool to THIS workload's own
                    # (tiny) dataset instead of leaving it at default_dram_gib()'s flat,
                    # machine-wide ceiling - see common.dram_gib_for's docstring for why an
                    # oversized buffer pool makes peak_rss_mb stop reflecting genuine usage.
                    # Recomputed every threads point since tpcc_warehouses grows with it above.
                    scale_variant = dataclasses.replace(
                        scale_variant, dram_gib=common.dram_gib_for(workload, scale_variant),
                    )
                for gc_variant in gc_variants:
                    # gc_variant is the literal string "n/a" for engines without a GC
                    # toggle - the "/" is a path separator, so f"gc_{gc_variant}" used
                    # unsanitized would silently split into two nested directories
                    # (gc_n/a/) instead of one, leaving gc_n/ looking empty at a glance.
                    gc_dir_name = f"gc_{gc_variant}".replace("/", "_")
                    out_dir = run_dir / workload / engine_name / f"threads_{threads}" / gc_dir_name
                    print(f"=== {workload} / {engine_name} / threads={threads} / gc={gc_variant} / "
                          f"dram_gib={scale_variant.dram_gib} ===")
                    try:
                        result = module.run(workload, scale_variant, out_dir, gc=gc_variant, reload=first_call)
                    except Exception as e:  # noqa: BLE001 - one engine's failure shouldn't abort the whole matrix
                        result = common.NormalizedResult(
                            engine_name, workload, scale_variant.label, _workload_duration(workload, scale_variant),
                            "error", 0.0, 0.0, threads=threads, gc_enabled=gc_variant, notes=f"EXCEPTION: {e}",
                        )
                    first_call = False
                    common.append_manifest_row(manifest_path, result)
                    status = "OK" if not result.notes else result.notes
                    print(f"    {result.primary_metric_name}={result.primary_metric_value:.2f}  "
                          f"peak_rss={result.peak_rss_mb:.1f}MB  "
                          f"scan_p99={result.scan_p99_us:.1f}us (n={result.scan_count})  [{status}]")

    print("\n########## comparison complete ##########")
    print(f"manifest : {manifest_path}")
    print(f"plot with: python3 scripts/plot_compare.py --run-dir {run_dir}")
    print("############################################\n")


if __name__ == "__main__":
    main()
