#!/usr/bin/env python3
"""Cross-engine benchmark harness: runs TPC-C, YCSB A-F, and HTAP/CH-benCHmark
(htap_q1/htap_q6) against BatStore, LeanStore, LeanStore's WiredTiger adapter,
and PostgreSQL (via BenchBase), sweeping thread/terminal count and GC on/off,
with engines NUMA-pinned to one node and external clients placed on another node when
available (see engines/common.py), normalizing every result into one
manifest.csv for scripts/plot_compare.py.

Sized for the real server (2x AMD EPYC 7742, 64 cores/128 threads per socket,
2 NUMA nodes) - the default thread sweep (1..128) is one socket's worth of
SMT threads, matching --cpubind=0. Not every engine has a real GC toggle: see
engines/{leanstore,wiredtiger}.py's SUPPORTS_GC_TOGGLE = False (LeanStore's
--pgc flag is dead code in this checkout, and the WiredTiger adapter has no
equivalent at all) - those two engines run once per (workload, threads) with
gc_enabled="n/a" regardless of --gc. BatStore (real --gc flag) and PostgreSQL
(via autovacuum, see engines/postgres_benchbase.py) get a real on/off compare.

htap_q1/htap_q6 (see common.py's HTAP_WORKLOADS) run TPC-C OLTP concurrently
with one dedicated thread repeatedly executing CH-benCHmark Q1 ("Pricing
Summary Report") or Q6 ("Forecasting Revenue Change") - the only 2 of
CH-benCHmark's 22 queries genuinely implemented across every engine here
(batstore, leanstore, wiredtiger, postgres, libmdbx, and both vweaver_ermia
variants - see manual.txt section 5; vweaver_ermia_frugal has a separate,
pre-existing KNOWN ISSUE there that crashes it on any sustained workload).
Compare
htap_q1/htap_q6's OLTP throughput against plain "tpcc" at the same
threads/gc (see plot_compare.py::plot_htap_interference) for the interference
analytics puts on OLTP - that derived comparison *is* this harness's "HTAP"
measurement, no separate baseline sub-phase needed.

For BatStore, that one analytical query fans out through a shared scan pool.
Unless overridden, the pool receives the CPU budget left after the current
OLTP terminal count: max(0, HTAP_CPU_BUDGET - OLTP_THREADS). The default
budget is the largest value in --threads.

Usage:
    python3 scripts/compare_engines.py
    python3 scripts/compare_engines.py --tiny --threads 2,4
    python3 scripts/compare_engines.py --engines batstore,leanstore --workloads tpcc,ycsb_e
    python3 scripts/compare_engines.py --threads 1,4,16,64 --gc on
    python3 scripts/compare_engines.py --warehouses 16 --tpcc-duration 120
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime
import math
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from engines import (
    batstore, common, leanstore, libmdbx, postgres_benchbase, umbra_benchbase, vweaver_ermia,
    vweaver_ermia_frugal, wiredtiger,
)
from clean_leanstore_runs import clean_run as clean_engine_run

ENGINE_MODULES = {
    "batstore": batstore,
    "leanstore": leanstore,
    "wiredtiger": wiredtiger,
    "postgres": postgres_benchbase,
    "umbra": umbra_benchbase,
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
    p.add_argument("--workloads", default=",".join(common.DEFAULT_WORKLOADS),
                   help=f"comma-separated subset of {common.ALL_WORKLOADS}")
    p.add_argument("--tiny", action="store_true", help="use TINY_SCALE (smoke test) instead of the server scale")
    p.add_argument("--skip-build", action="store_true", help="skip each engine's ensure_built() step")
    p.add_argument("--batstore-allocator", "--cmvbt-allocator", dest="batstore_allocator",
                   choices=["jemalloc", "mimalloc"], default="jemalloc",
                   help="global allocator BatStore's binary is built with (see Cargo.toml's `mimalloc` "
                        "feature) - 'jemalloc' is the crate's own default; 'mimalloc' measured a few %% "
                        "faster on YCSB's WAL path but a few %% slower on TPC-C, so it's opt-in here too. "
                        "Only affects the batstore/libmdbx engines, which share one binary.")

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
    p.add_argument("--htap-olap-threads", type=int,
                   help="number of dedicated analytical (OLAP) threads for htap_q1/htap_q6, "
                        "run concurrently with the (fixed) --threads-sized OLTP terminal pool "
                        "- sweep this via a shell loop for a throughput-vs-analytical-threads plot")
    p.add_argument("--ycsb-payload", choices=["standard", "u64"], default="standard",
                   help="standard=10x100-byte YCSB row (default); u64=one 8-byte value")
    p.add_argument("--ycsb-key-only", action="store_true",
                   help="match/validate keys but do not consume payload bytes (default reads payload)")
    p.add_argument("--ycsb-write-all-fields", action="store_true",
                   help="LeanStore/WiredTiger YCSB updates replace all fields (default updates one random field)")
    p.add_argument("--batstore-ycsb-mode", "--cmvbt-ycsb-mode", dest="batstore_ycsb_mode",
                   choices=["atomic", "transaction"], default="atomic",
                   help="BatStore YCSB path: commit-before-publish auto-commit (default), or ordinary transaction lifecycle")
    p.add_argument("--scan-pool-workers", type=int, default=None,
                   help="BatStore only (ignored by every other engine): force one fixed shared "
                        "scan-pool size at every point. Omitted (default): htap_q1/htap_q6 use "
                        "the capacity left by --htap-cpu-budget after OLTP terminals; YCSB "
                        "retains its binary auto-sizing. Pass 0 to disable the pool.")
    p.add_argument("--htap-cpu-budget", type=int,
                   help="BatStore HTAP OLTP+scan CPU budget; default is the largest --threads "
                        "value. With no fixed --scan-pool-workers override, each htap_q1/q6 "
                        "point gets max(0, budget - OLTP terminals) scan-pool workers.")
    p.add_argument("--dram-gib", type=float)

    p.add_argument("--s-htap-record-count", type=int,
                   help="cold historical corpus loaded before the S-YCSB workload's timed phase")
    p.add_argument("--s-htap-duration", type=int)
    p.add_argument("--s-htap-hot-window", type=int,
                   help="width, in keys, of the recency-biased hot-update tail")
    p.add_argument("--s-htap-theta", type=float, help="zipf skew of hot-tail updates")
    p.add_argument("--s-htap-arrival-ratio", type=float,
                   help="fraction of write-thread ops that are new arrivals vs. hot-tail updates")
    p.add_argument("--s-htap-max-lateness", type=int,
                   help="bound on how far behind the arrival ticket a late event's key can land")
    p.add_argument("--s-htap-olap-threads", type=int,
                   help="OLAP scanner threads carved out of each --threads sweep point "
                        "(the remainder are write threads)")
    p.add_argument("--s-htap-olap-lag", type=int,
                   help="how far behind the current tail an OLAP scan's newest edge sits")
    p.add_argument("--s-htap-olap-span", type=int, help="width, in keys, of each OLAP scan")
    return p.parse_args()


def build_scale(args: argparse.Namespace) -> common.Scale:
    scale = dataclasses.replace(common.TINY_SCALE) if args.tiny else common.Scale()
    overrides = {
        "tpcc_warehouses": args.warehouses,
        "tpcc_duration": args.tpcc_duration,
        "ycsb_records": args.ycsb_records,
        "ycsb_duration": args.ycsb_duration,
        "ycsb_theta": args.theta,
        "htap_olap_threads": args.htap_olap_threads,
        "dram_gib": args.dram_gib,
        "s_htap_record_count": args.s_htap_record_count,
        "s_htap_duration": args.s_htap_duration,
        "s_htap_hot_window": args.s_htap_hot_window,
        "s_htap_theta": args.s_htap_theta,
        "s_htap_arrival_ratio": args.s_htap_arrival_ratio,
        "s_htap_max_lateness": args.s_htap_max_lateness,
        "s_htap_olap_threads": args.s_htap_olap_threads,
        "s_htap_olap_lag": args.s_htap_olap_lag,
        "s_htap_olap_span": args.s_htap_olap_span,
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
    if workload == "s_htap":
        return scale.s_htap_duration
    return scale.ycsb_duration


def _dynamic_htap_scan_pool_workers(cpu_budget: int, oltp_threads: int) -> int:
    """Leave OLTP its requested terminals and give remaining capacity to one scan query.

    A one-worker pool has dispatch overhead without parallelism, so a remainder of zero or
    one selects the sequential OLAP-thread path instead.
    """
    remaining = max(0, cpu_budget - oltp_threads)
    return remaining if remaining >= 2 else 0


def main() -> None:
    args = parse_args()
    # Read fresh by common.batstore_cargo_build_args() inside batstore.py/libmdbx.py's own
    # ensure_built() - see that function's doc for why this is an env var, not a direct
    # module attribute, and why it's set unconditionally here even if "batstore"/"libmdbx"
    # aren't in --engines (harmless: the var is simply never read in that case).
    os.environ["BATSTORE_ALLOCATOR"] = args.batstore_allocator
    os.environ["YCSB_PAYLOAD_BYTES"] = "8" if args.ycsb_payload == "u64" else "1000"
    os.environ["YCSB_FIELD_COUNT"] = "1" if args.ycsb_payload == "u64" else "10"
    os.environ["YCSB_FIELD_LENGTH"] = "8" if args.ycsb_payload == "u64" else "100"
    os.environ["YCSB_WRITE_ALL_FIELDS"] = "true" if args.ycsb_write_all_fields else "false"
    os.environ["BATSTORE_YCSB_MODE"] = args.batstore_ycsb_mode
    scale = build_scale(args)
    engines = [
        "batstore" if e.strip() == "cmvbt" else e.strip()
        for e in args.engines.split(",") if e.strip()
    ]
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
    if not engines or not workloads or not gc_list:
        sys.exit("--engines, --workloads, and --gc must each contain at least one value")
    if not thread_list or any(t <= 0 for t in thread_list):
        sys.exit("--threads must contain positive integers")
    if args.htap_cpu_budget is not None and args.htap_cpu_budget <= 0:
        sys.exit("--htap-cpu-budget must be a positive integer")
    if args.scan_pool_workers is not None and args.scan_pool_workers < 0:
        sys.exit("--scan-pool-workers must be zero or a positive integer")
    if scale.tpcc_warehouses <= 0 or scale.tpcc_duration <= 0:
        sys.exit("--warehouses and --tpcc-duration must be positive")
    if scale.ycsb_records <= 0 or scale.ycsb_duration <= 0:
        sys.exit("--ycsb-records and --ycsb-duration must be positive")
    if scale.s_htap_record_count <= 0 or scale.s_htap_duration <= 0:
        sys.exit("--s-htap-record-count and --s-htap-duration must be positive")
    if scale.s_htap_hot_window <= 0 or scale.s_htap_olap_span <= 0:
        sys.exit("--s-htap-hot-window and --s-htap-olap-span must be positive")
    if not math.isfinite(scale.ycsb_theta) or scale.ycsb_theta < 0.0:
        sys.exit("--theta must be a finite, non-negative number")
    if scale.dram_gib <= 0:
        sys.exit("--dram-gib must be positive")

    htap_cpu_budget = args.htap_cpu_budget or max(thread_list)

    # Must be absolute: each engine wrapper spawns its subprocess with a different cwd
    # (output_dir for leanstore/wiredtiger, BENCHBASE_HOME for postgres), so a relative
    # run_dir would have its derived paths (ssd_path, config_path, ...) re-resolved
    # against the wrong directory when passed as a command-line argument.
    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)
    (run_dir / "run_config.json").write_text(json.dumps({
        "ycsb_payload": args.ycsb_payload,
        "ycsb_payload_bytes": 8 if args.ycsb_payload == "u64" else 1000,
        "ycsb_read_payload": not args.ycsb_key_only,
        "ycsb_write_all_fields": args.ycsb_write_all_fields,
        "htap_cpu_budget": htap_cpu_budget,
        "batstore_scan_pool_workers_override": args.scan_pool_workers,
        "no_durability": common.NO_DURABILITY,
    }, indent=2) + "\n")

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
    print(f"batstore alloc   : {args.batstore_allocator} (ignored unless 'batstore'/'libmdbx' is in --engines)")
    print(f"workloads     : {workloads}")
    print(f"YCSB payload  : {args.ycsb_payload} ({'key-only' if args.ycsb_key_only else 'payload-read'})")
    print(f"YCSB updates  : writeallfields={'true' if args.ycsb_write_all_fields else 'false'} "
          f"(LeanStore/WiredTiger: {'all fields' if args.ycsb_write_all_fields else 'one random field'})")
    print(f"threads sweep : {thread_list}")
    scan_pool_desc = (
        f"fixed at {args.scan_pool_workers} workers"
        if args.scan_pool_workers is not None else
        f"dynamic: max(0, {htap_cpu_budget} CPU budget - OLTP terminals)"
    )
    print(f"BatStore HTAP scan pool: {scan_pool_desc}")
    print(f"gc sweep      : {gc_list} (engines with no working GC toggle always run once, gc=n/a)")
    pg_client_node = common.external_client_numa_node()
    print(f"NUMA pinning  : engines use node {common.NUMA_NODE}; PostgreSQL's cluster service and "
          f"Umbra's container use matching cgroup CPU/memory-node constraints (verified per run). "
          f"The external BenchBase client uses node {pg_client_node}")
    dram_gib_desc = (
        f"--dram-gib={args.dram_gib} (explicit, applied to every workload/threads point unchanged)"
        if args.dram_gib is not None else
        f"auto, sized per-workload to ~4x its own estimated dataset (see common.dram_gib_for), "
        f"capped at {common.default_dram_gib()} (this machine's NUMA-node-safe ceiling)"
    )
    if common.NO_DURABILITY:
        print(f"durability    : OFF via each engine's own config (BATSTORE_BENCH_NO_DURABILITY=1, "
              f"see compare_engines_new.py) - BatStore WAL disabled outright, LeanStore "
              f"--wal_pwrite=false --wal_fsync=false, libmdbx/vWeaver_ermia already sync-free by "
              f"default, PostgreSQL fsync/synchronous_commit/full_page_writes off (WiredTiger's own "
              f"log module stays on - see compare_engines_new.py's docstring); scratch data lives on "
              f"real disk at {common.NO_DURABILITY_SCRATCH_ROOT}, not tmpfs. dram_gib: {dram_gib_desc}")
    else:
        print(f"in-memory only: SCRATCH_ROOT={common.SCRATCH_ROOT} (tmpfs-verified; LeanStore/WiredTiger/"
              f"libmdbx/vWeaver_ermia/batstore/Umbra data never touches a real disk - for Umbra this is "
              f"the ONLY durability lever this script has, since its own fsync/synchronous_commit/"
              f"autovacuum aren't software-toggleable, see engines/umbra_benchbase.py) dram_gib: "
              f"{dram_gib_desc} (LeanStore/WiredTiger buffer pool - not used by batstore, whose WAL is "
              f"forced on but unbounded like every other in-memory structure here, or postgres/umbra, "
              f"whose buffer sizing is configured by setup_environment.py's fixed PostgreSQL "
              f"memory budget)")
        print("PostgreSQL durability: performance mode is active for every run: fsync=off, "
              "synchronous_commit=off, full_page_writes=off (verified before execution)")
    print(f"PostgreSQL    : {common.POSTGRES_MEMORY_BUDGET_GIB:g}GiB service memory budget; load is "
          f"excluded from timed throughput and peak memory; peak memory is cgroup-charged total "
          f"when cgroup v2 is available")
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
            for threads in thread_list:
                # TPC-C spec sizes populations at ~10 terminals per warehouse; a fixed
                # warehouse count while terminals sweep up to 128 would push the
                # terminals/warehouse ratio to 16:1 at the top end - far more contention
                # than the spec's intended range, and a likely contributor to BatStore's
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
                    batstore_scan_pool_workers = None
                    if engine_name == "batstore":
                        if args.scan_pool_workers is not None:
                            batstore_scan_pool_workers = args.scan_pool_workers
                        elif workload in common.HTAP_WORKLOADS:
                            batstore_scan_pool_workers = _dynamic_htap_scan_pool_workers(
                                htap_cpu_budget, threads,
                            )
                    scan_pool_note = (
                        f" / scan_pool_workers={batstore_scan_pool_workers}"
                        if batstore_scan_pool_workers is not None else ""
                    )
                    print(f"=== {workload} / {engine_name} / threads={threads} / gc={gc_variant} / "
                          f"dram_gib={scale_variant.dram_gib}{scan_pool_note} ===")
                    try:
                        # PostgreSQL is also recreated and loaded for every point. This is
                        # slower than reusing BenchBase tables across the thread/GC sweep,
                        # but guarantees that mutations and vacuum state from a prior point
                        # cannot contaminate the next measurement.
                        reload_data = engine_name == "postgres"
                        run_kwargs = dict(
                            gc=gc_variant, reload=reload_data,
                            ycsb_payload=args.ycsb_payload, read_payload=not args.ycsb_key_only,
                        )
                        # scan_pool_workers is a batstore.py-only kwarg (see its `run()` doc) -
                        # every other engine's run() has no such parameter. For BatStore HTAP,
                        # the dynamic CPU-budget calculation supplies it even without an explicit
                        # CLI override; for BatStore YCSB it remains absent so binary auto-sizing
                        # still applies.
                        if batstore_scan_pool_workers is not None:
                            run_kwargs["scan_pool_workers"] = batstore_scan_pool_workers
                        result = module.run(workload, scale_variant, out_dir, **run_kwargs)
                    except Exception as e:  # noqa: BLE001 - one engine's failure shouldn't abort the whole matrix
                        result = common.NormalizedResult(
                            engine_name, workload, scale_variant.label, _workload_duration(workload, scale_variant),
                            "error", 0.0, 0.0, threads=threads, gc_enabled=gc_variant, notes=f"EXCEPTION: {e}",
                        )
                    common.append_manifest_row(manifest_path, result)
                    status = "OK" if not result.notes else result.notes
                    print(f"    {result.primary_metric_name}={result.primary_metric_value:.2f}  "
                          f"peak_rss={result.peak_rss_mb:.1f}MB  "
                          f"scan_p99={result.scan_p99_us:.1f}us (n={result.scan_count})  [{status}]")

    print("\nCleaning unneeded engine run artifacts...")
    clean_engine_run(run_dir, delete=True, verbose=False)

    print("\n########## comparison complete ##########")
    print(f"manifest : {manifest_path}")
    print(f"plot with: python3 scripts/plot_compare.py --run-dir {run_dir}")
    print("############################################\n")


if __name__ == "__main__":
    main()
