#!/usr/bin/env python3
"""HTAP analytical-thread sweep: a small, fixed-size TPC-C OLTP workload (scaled down -
few warehouses/terminals, the point is analytical pressure, not OLTP scale) runs
concurrently with a growing pool of dedicated analytical (OLAP) threads, each repeatedly
executing CH-benCHmark Q1 ("Pricing Summary Report") or Q6 ("Forecasting Revenue Change")
in a loop for the run's whole duration (see htap_q1/htap_q6 in common.HTAP_WORKLOADS, and
Scale.htap_olap_threads - the knob this script sweeps).

x-axis = number of analytical threads; produces two throughput series per engine: the
OLTP side (new_order_per_sec, held against a FIXED OLTP terminal count so the x-axis is
purely the analytical side) and the OLAP side (CH-benCHmark queries/sec, summed across all
analytical threads) - see plot_htap_analytical.py.

Usage:
    python3 scripts/run_htap_analytical_sweep.py
    python3 scripts/run_htap_analytical_sweep.py --engines batstore,libmdbx,postgres,wiredtiger,leanstore
    python3 scripts/run_htap_analytical_sweep.py --olap-threads 1,2,4,8,16 --oltp-terminals 4 --warehouses 2
    python3 scripts/run_htap_analytical_sweep.py --workloads htap_q1
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from engines import batstore, common, leanstore, libmdbx, postgres_benchbase, vweaver_ermia, vweaver_ermia_frugal, wiredtiger
from clean_leanstore_runs import clean_run as clean_engine_run

ENGINE_MODULES = {
    "batstore": batstore,
    "leanstore": leanstore,
    "wiredtiger": wiredtiger,
    "postgres": postgres_benchbase,
    "vweaver_ermia": vweaver_ermia,
    "vweaver_ermia_frugal": vweaver_ermia_frugal,
    "libmdbx": libmdbx,
}

DEFAULT_OLAP_THREADS = [1, 2, 4, 8, 16, 32]
# Scaled-down HTAP focus: a small, fixed OLTP population/terminal count (not swept), so the
# only thing changing across this sweep is analytical pressure - matches the "TPC-C part
# should be scaled down, more focus on the analytical part" ask this script implements.
DEFAULT_OLTP_TERMINALS = 4
# At least four populated warehouse ranges are needed for BatStore's default per-query
# scan-pool fanout to have four useful jobs. Eight also keeps this analytical-focused
# sweep smaller than a full server-scale run while avoiding the old two-way partition cap.
DEFAULT_WAREHOUSES = 8
DEFAULT_GC = ["on", "off"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="htap_analytical_results")
    p.add_argument("--engines", default=",".join(common.ENGINES),
                   help=f"comma-separated subset of {common.ENGINES}")
    p.add_argument("--workloads", default=",".join(common.HTAP_WORKLOADS),
                   help=f"comma-separated subset of {common.HTAP_WORKLOADS}")
    p.add_argument("--olap-threads", default=",".join(str(t) for t in DEFAULT_OLAP_THREADS),
                   help=f"comma-separated analytical thread counts to sweep (default {DEFAULT_OLAP_THREADS})")
    p.add_argument("--oltp-terminals", type=int, default=DEFAULT_OLTP_TERMINALS,
                   help=f"fixed OLTP terminal count, held constant across the sweep (default {DEFAULT_OLTP_TERMINALS})")
    p.add_argument("--warehouses", type=int, default=DEFAULT_WAREHOUSES,
                   help=f"fixed, scaled-down TPC-C warehouse count (default {DEFAULT_WAREHOUSES})")
    p.add_argument("--tpcc-duration", type=int, default=60)
    p.add_argument("--gc", default=",".join(DEFAULT_GC),
                   help="comma-separated subset of on,off - engines with no working GC "
                        "toggle (see SUPPORTS_GC_TOGGLE in each engines/*.py) are only run "
                        "once and report that same result for every requested gc label")
    p.add_argument("--scan-pool-workers", type=int, default=None,
                   help="BatStore only (ignored by every other engine): assigns ORDER_LINE a "
                        "shared scan-worker pool of this many total threads (src/bat_tree/"
                        "scan_pool.rs, DriverConfig::scan_pool_workers). Each htap_q1/htap_q6 "
                        "query only ever asks for its fair share of the pool "
                        "(ScanWorkerPool::fair_query_fanout: pool size / OLAP thread count, "
                        "floored at 2 workers), not the whole pool, so several concurrently- "
                        "querying OLAP threads can each get serviced by the pool at once; a query "
                        "runs on its own OLAP thread instead if its fair share has no spare "
                        "capacity right now, or if there are too many OLAP threads sharing the "
                        "pool to give each one a fair share of at least 2. Omitted (default): "
                        "main_tpcc's own CLI parsing decides, auto-enabling the pool (sized to "
                        "the machine's own core count) whenever the population is large enough "
                        "for it to pay off - pass `0` to disable it entirely instead (the plain "
                        "sequential path). Any other value is floored to 2 by "
                        "ScanWorkerPool::spawn (a 1-worker 'pool' buys no parallelism). Pool "
                        "worker threads never register a WorkerId (see bat_sync::worker::"
                        "READ_ONLY_SCAN_WORKER_ID) since they never write, so this is NOT counted "
                        "against your core count the way oltp_terminals/olap_threads are - free to "
                        "oversubscribe this past nproc (main_tpcc's own auto-sizing does exactly "
                        "that, defaulting to the machine's own core count).")
    p.add_argument("--skip-build", action="store_true")
    p.add_argument("--batstore-allocator", "--cmvbt-allocator", dest="batstore_allocator",
                   choices=["jemalloc", "mimalloc"], default="jemalloc")
    p.add_argument("--dram-gib", type=float)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    os.environ["BATSTORE_ALLOCATOR"] = args.batstore_allocator
    os.environ["YCSB_PAYLOAD_BYTES"] = "1000"
    os.environ["YCSB_FIELD_COUNT"] = "10"
    os.environ["YCSB_FIELD_LENGTH"] = "100"
    os.environ["YCSB_WRITE_ALL_FIELDS"] = "false"
    os.environ["BATSTORE_YCSB_MODE"] = "atomic"

    base_scale = common.Scale(tpcc_warehouses=args.warehouses, tpcc_terminals=args.oltp_terminals,
                               tpcc_duration=args.tpcc_duration,
                               label=f"htap_analytical(warehouses={args.warehouses},oltp_terminals={args.oltp_terminals})")
    if args.dram_gib is not None:
        base_scale.dram_gib = args.dram_gib

    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]
    olap_thread_list = [int(t.strip()) for t in args.olap_threads.split(",") if t.strip()]
    gc_list = [g.strip() for g in args.gc.split(",") if g.strip()]

    for e in engines:
        if e not in ENGINE_MODULES:
            sys.exit(f"unknown engine '{e}'")
    for w in workloads:
        if w not in common.HTAP_WORKLOADS:
            sys.exit(f"unknown workload '{w}' (this sweep is HTAP-only: {common.HTAP_WORKLOADS})")
    for g in gc_list:
        if g not in ("on", "off"):
            sys.exit(f"unknown --gc value '{g}' (expected 'on' and/or 'off')")
    if not gc_list:
        sys.exit("--gc must contain at least one value")

    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)
    (run_dir / "run_config.json").write_text(json.dumps({
        "olap_threads": olap_thread_list, "oltp_terminals": args.oltp_terminals,
        "warehouses": args.warehouses, "workloads": workloads, "engines": engines,
        "scan_pool_workers": args.scan_pool_workers,
        "batstore_initial_history_days": 30,
        "batstore_q1_history_slice": "oldest_50_percent",
        "batstore_q6_history_slice": "25_to_50_percent",
    }, indent=2) + "\n")

    print("\n########## HTAP analytical-thread sweep ##########")
    print(f"run directory   : {run_dir}")
    print(f"engines         : {engines}")
    print(f"workloads       : {workloads}")
    print(f"OLTP (fixed)    : warehouses={args.warehouses}, terminals={args.oltp_terminals}, "
          f"duration={args.tpcc_duration}s")
    print(f"OLAP threads    : {olap_thread_list}")
    print("BatStore dates  : 30-day ordered history; Q1 oldest 50%; Q6 25%-50% slice")
    print(f"gc sweep        : {gc_list} (engines with no working GC toggle always run once)")
    if args.scan_pool_workers is None:
        print("scan pool       : auto (BatStore default - enabled when the population is large "
              "enough to pay off; every other engine ignores this)")
    elif args.scan_pool_workers == 0:
        print("scan pool       : disabled (explicit override; every other engine ignores this)")
    else:
        print(f"scan pool       : {args.scan_pool_workers} workers (BatStore only - every other engine ignores this)")
    print("####################################################\n")

    if not args.skip_build:
        for name in engines:
            ensure_built = getattr(ENGINE_MODULES[name], "ensure_built", None)
            if ensure_built:
                print(f"[build] {name}...")
                ensure_built()

    for workload in workloads:
        for engine_name in engines:
            module = ENGINE_MODULES[engine_name]
            for olap_threads in olap_thread_list:
                scale_variant = dataclasses.replace(
                    base_scale, htap_olap_threads=olap_threads, htap_scan_pool_workers=args.scan_pool_workers,
                )
                if args.dram_gib is None:
                    scale_variant = dataclasses.replace(
                        scale_variant, dram_gib=common.dram_gib_for(workload, scale_variant),
                    )
                out_dir_base = run_dir / workload / engine_name / f"olap_threads_{olap_threads}"
                print(f"=== {workload} / {engine_name} / olap_threads={olap_threads} "
                      f"(oltp_terminals={args.oltp_terminals}) ===")
                run_kwargs = dict(reload=(engine_name == "postgres"))
                # scan_pool_workers is a batstore.py-only kwarg (see its `run()` doc) - every
                # other engine's run() has no such parameter and would raise TypeError if passed.
                # `None` is left out of run_kwargs entirely (not even as `None`) so batstore.py's
                # own default applies and the binary auto-decides; `0`/a positive count is
                # passed through as an explicit override either way.
                if engine_name == "batstore" and args.scan_pool_workers is not None:
                    run_kwargs["scan_pool_workers"] = args.scan_pool_workers
                try:
                    gc_results = common.run_gc_variants(module, workload, scale_variant, out_dir_base, gc_list, run_kwargs)
                except Exception as e:  # noqa: BLE001 - one point's failure shouldn't abort the sweep
                    gc_results = [(gc, common.NormalizedResult(
                        engine_name, workload, scale_variant.label, args.tpcc_duration,
                        "error", 0.0, 0.0, threads=args.oltp_terminals, gc_enabled="n/a",
                        notes=f"EXCEPTION: {e}",
                    )) for gc in gc_list]
                for gc, result in gc_results:
                    # Stamp the analytical thread count into config_label (parsed back out
                    # by plot_htap_analytical.py) - same convention as
                    # run_skew_sweep.py's "skew=".
                    result.config_label = f"{result.config_label} olap_threads={olap_threads}"
                    if args.scan_pool_workers is not None:
                        result.config_label = f"{result.config_label} scan_pool_workers={args.scan_pool_workers}"
                    common.append_manifest_row(manifest_path, result)
                    olap_qps = result.scan_count / result.duration_secs if result.duration_secs else 0.0
                    status = result.notes or "OK"
                    print(f"    gc={gc}  oltp={result.primary_metric_value:.2f} {result.primary_metric_name}  "
                          f"olap={olap_qps:.3f} queries/sec (n={result.scan_count})  [{status}]")

    print("\nCleaning unneeded engine run artifacts...")
    clean_engine_run(run_dir, delete=True, verbose=False)

    print("\n########## HTAP analytical sweep complete ##########")
    print(f"manifest : {manifest_path}")
    print(f"plot with: python3 scripts/plot.py {run_dir}")
    print("######################################################\n")


if __name__ == "__main__":
    main()
