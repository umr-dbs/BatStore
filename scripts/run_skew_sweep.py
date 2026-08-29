#!/usr/bin/env python3
"""YCSB skew (Zipfian theta) sweep: runs each requested YCSB workload (a-f) at a fixed set
of skew factors - starting from a uniform distribution (theta=0.0, which BatStore's/
LeanStore's/WiredTiger's/BenchBase's Zipfian generators all degenerate to a uniform
distribution at), then Zipfian theta 0.1, 0.4, 0.8, 0.99 (the harness's own default,
common.Scale.ycsb_theta), and 1.4 - crossed with a thread-count sweep, against whichever
engines are requested (BatStore, libmdbx, PostgreSQL, WiredTiger, ...).

This reuses compare_engines.py's engine wrappers and manifest schema unchanged - the skew
value for each run is stamped into NormalizedResult.config_label as "<scale.label>
skew=<label>" (see build_scale_variant below), so plot_skew_sweep.py can recover it from
manifest.csv without any schema change of its own.

Usage:
    python3 scripts/run_skew_sweep.py
    python3 scripts/run_skew_sweep.py --engines batstore,libmdbx,postgres,wiredtiger,leanstore
    python3 scripts/run_skew_sweep.py --workloads ycsb_a,ycsb_b --threads 4,16,64
    python3 scripts/run_skew_sweep.py --tiny --skews uniform,0.99
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime
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

# "uniform" is not a separate distribution string here - it's theta=0.0, which every
# engine's own Zipfian generator (BatStore's ycsb_random.rs, LeanStore/WiredTiger's zipf
# generator, BenchBase's skewFactor) degenerates to a uniform distribution at. Keeping this
# as a single theta sweep (rather than a separate "uniform" distribution mode on BatStore's
# side only) is what makes the four engines' "uniform" points genuinely comparable.
DEFAULT_SKEWS = ["uniform", "0.1", "0.4", "0.8", "0.99", "1.4"]
DEFAULT_THREADS = [2, 4, 8, 16, 32, 64, 128]
DEFAULT_GC = ["on", "off"]


def skew_to_theta(skew: str) -> float:
    return 0.0 if skew == "uniform" else float(skew)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="skew_sweep_results")
    p.add_argument("--engines", default=",".join(common.ENGINES),
                   help=f"comma-separated subset of {common.ENGINES}")
    p.add_argument("--workloads", default=",".join(common.YCSB_WORKLOADS),
                   help=f"comma-separated subset of {common.YCSB_WORKLOADS}")
    p.add_argument("--skews", default=",".join(DEFAULT_SKEWS),
                   help=f"comma-separated skew points: 'uniform' or a Zipfian theta "
                        f"(default {DEFAULT_SKEWS})")
    p.add_argument("--threads", default=None,
                   help=f"comma-separated thread counts to sweep (default {DEFAULT_THREADS}, "
                        f"or [2,4] with --tiny)")
    p.add_argument("--gc", default=",".join(DEFAULT_GC),
                   help="comma-separated subset of on,off - engines with no working GC "
                        "toggle (see SUPPORTS_GC_TOGGLE in each engines/*.py) are only run "
                        "once and report that same result for every requested gc label")
    p.add_argument("--tiny", action="store_true", help="use TINY_SCALE (smoke test)")
    p.add_argument("--skip-build", action="store_true")
    p.add_argument("--batstore-allocator", "--cmvbt-allocator", dest="batstore_allocator",
                   choices=["jemalloc", "mimalloc"], default="jemalloc")
    p.add_argument("--ycsb-records", type=int)
    p.add_argument("--ycsb-duration", type=int)
    p.add_argument("--ycsb-payload", choices=["standard", "u64"], default="standard")
    p.add_argument("--ycsb-key-only", action="store_true")
    p.add_argument("--batstore-ycsb-mode", "--cmvbt-ycsb-mode", dest="batstore_ycsb_mode",
                   choices=["atomic", "transaction"], default="atomic")
    p.add_argument("--dram-gib", type=float)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    os.environ["BATSTORE_ALLOCATOR"] = args.batstore_allocator
    os.environ["YCSB_PAYLOAD_BYTES"] = "8" if args.ycsb_payload == "u64" else "1000"
    os.environ["YCSB_FIELD_COUNT"] = "1" if args.ycsb_payload == "u64" else "10"
    os.environ["YCSB_FIELD_LENGTH"] = "8" if args.ycsb_payload == "u64" else "100"
    os.environ["YCSB_WRITE_ALL_FIELDS"] = "false"
    os.environ["BATSTORE_YCSB_MODE"] = args.batstore_ycsb_mode

    base_scale = dataclasses.replace(common.TINY_SCALE) if args.tiny else common.Scale()
    for field, value in (("ycsb_records", args.ycsb_records), ("ycsb_duration", args.ycsb_duration),
                         ("dram_gib", args.dram_gib)):
        if value is not None:
            setattr(base_scale, field, value)

    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]
    skews = [s.strip() for s in args.skews.split(",") if s.strip()]
    gc_list = [g.strip() for g in args.gc.split(",") if g.strip()]
    for g in gc_list:
        if g not in ("on", "off"):
            sys.exit(f"unknown --gc value '{g}' (expected 'on' and/or 'off')")
    if not gc_list:
        sys.exit("--gc must contain at least one value")
    thread_list = (
        [int(t.strip()) for t in args.threads.split(",") if t.strip()] if args.threads
        else ([2, 4] if args.tiny else list(DEFAULT_THREADS))
    )

    for e in engines:
        if e not in ENGINE_MODULES:
            sys.exit(f"unknown engine '{e}'")
    for w in workloads:
        if w not in common.YCSB_WORKLOADS:
            sys.exit(f"unknown workload '{w}' (this sweep is YCSB-only: {common.YCSB_WORKLOADS})")

    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)
    (run_dir / "run_config.json").write_text(json.dumps({
        "skews": skews, "threads": thread_list, "workloads": workloads, "engines": engines,
    }, indent=2) + "\n")

    print("\n########## YCSB skew sweep ##########")
    print(f"run directory : {run_dir}")
    print(f"engines       : {engines}")
    print(f"workloads     : {workloads}")
    print(f"skews         : {skews} (theta={[skew_to_theta(s) for s in skews]})")
    print(f"threads sweep : {thread_list}")
    print(f"gc sweep      : {gc_list} (engines with no working GC toggle always run once)")
    print("######################################\n")

    if not args.skip_build:
        for name in engines:
            ensure_built = getattr(ENGINE_MODULES[name], "ensure_built", None)
            if ensure_built:
                print(f"[build] {name}...")
                ensure_built()

    for workload in workloads:
        for engine_name in engines:
            module = ENGINE_MODULES[engine_name]
            for skew in skews:
                theta = skew_to_theta(skew)
                for threads in thread_list:
                    scale_variant = dataclasses.replace(
                        base_scale, ycsb_threads=threads, ycsb_theta=theta,
                        label=f"{base_scale.label} skew={skew}",
                    )
                    if args.dram_gib is None:
                        scale_variant = dataclasses.replace(
                            scale_variant, dram_gib=common.dram_gib_for(workload, scale_variant),
                        )
                    out_dir_base = run_dir / workload / engine_name / f"skew_{skew}" / f"threads_{threads}"
                    print(f"=== {workload} / {engine_name} / skew={skew} (theta={theta}) / threads={threads} ===")
                    run_kwargs = dict(
                        reload=(engine_name == "postgres"),
                        ycsb_payload=args.ycsb_payload, read_payload=not args.ycsb_key_only,
                    )
                    try:
                        gc_results = common.run_gc_variants(module, workload, scale_variant, out_dir_base, gc_list, run_kwargs)
                    except Exception as e:  # noqa: BLE001 - one point's failure shouldn't abort the sweep
                        gc_results = [(gc, common.NormalizedResult(
                            engine_name, workload, scale_variant.label, scale_variant.ycsb_duration,
                            "error", 0.0, 0.0, threads=threads, gc_enabled="n/a", notes=f"EXCEPTION: {e}",
                        )) for gc in gc_list]
                    for gc, result in gc_results:
                        common.append_manifest_row(manifest_path, result)
                        status = result.notes or "OK"
                        print(f"    gc={gc}  {result.primary_metric_name}={result.primary_metric_value:.2f}  [{status}]")

    print("\nCleaning unneeded engine run artifacts...")
    clean_engine_run(run_dir, delete=True, verbose=False)

    print("\n########## skew sweep complete ##########")
    print(f"manifest : {manifest_path}")
    print(f"plot with: python3 scripts/plot.py {run_dir}")
    print("##########################################\n")


if __name__ == "__main__":
    main()
