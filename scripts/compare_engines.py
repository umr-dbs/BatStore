#!/usr/bin/env python3
"""Cross-engine benchmark harness: runs TPC-C and YCSB A-F against cMVBT,
LeanStore, LeanStore's WiredTiger adapter, and PostgreSQL (via BenchBase)
with matched scale parameters, normalizing all four into one manifest.csv
for scripts/plot_compare.py.

Mirrors src/mv_bench/suite.rs's one-command design (see that file's module
docs) but across engines instead of GC on/off. See
/home/amir/.claude/plans/iterative-spinning-origami.md for the full design
and the machine-scale rationale (this is a 24-core/32GB workstation, not the
source paper's 64-core/512GB server - results are for relative cross-engine
comparison here, not a reproduction of the paper's absolute numbers).

Usage:
    python3 scripts/compare_engines.py
    python3 scripts/compare_engines.py --tiny
    python3 scripts/compare_engines.py --engines cmvbt,leanstore --workloads tpcc,ycsb_a
    python3 scripts/compare_engines.py --warehouses 16 --tpcc-duration 120
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from engines import cmvbt, common, leanstore, postgres_benchbase, wiredtiger

ENGINE_MODULES = {
    "cmvbt": cmvbt,
    "leanstore": leanstore,
    "wiredtiger": wiredtiger,
    "postgres": postgres_benchbase,
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output-root", default="comparison_results")
    p.add_argument("--engines", default=",".join(common.ENGINES),
                   help=f"comma-separated subset of {common.ENGINES}")
    p.add_argument("--workloads", default=",".join(common.ALL_WORKLOADS),
                   help=f"comma-separated subset of {common.ALL_WORKLOADS}")
    p.add_argument("--tiny", action="store_true", help="use TINY_SCALE (smoke test) instead of the workstation scale")
    p.add_argument("--skip-build", action="store_true", help="skip each engine's ensure_built() step")

    p.add_argument("--warehouses", type=int)
    p.add_argument("--terminals", type=int)
    p.add_argument("--tpcc-duration", type=int)
    p.add_argument("--ycsb-records", type=int)
    p.add_argument("--ycsb-threads", type=int)
    p.add_argument("--ycsb-duration", type=int)
    p.add_argument("--theta", type=float)
    p.add_argument("--dram-gib", type=float)
    return p.parse_args()


def build_scale(args: argparse.Namespace) -> common.Scale:
    scale = dataclasses.replace(common.TINY_SCALE) if args.tiny else common.Scale()
    overrides = {
        "tpcc_warehouses": args.warehouses,
        "tpcc_terminals": args.terminals,
        "tpcc_duration": args.tpcc_duration,
        "ycsb_records": args.ycsb_records,
        "ycsb_threads": args.ycsb_threads,
        "ycsb_duration": args.ycsb_duration,
        "ycsb_theta": args.theta,
        "dram_gib": args.dram_gib,
    }
    for field, value in overrides.items():
        if value is not None:
            setattr(scale, field, value)
    return scale


def main() -> None:
    args = parse_args()
    scale = build_scale(args)
    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]

    for e in engines:
        if e not in ENGINE_MODULES:
            sys.exit(f"unknown engine '{e}' (expected one of {list(ENGINE_MODULES)})")
    for w in workloads:
        if w not in common.ALL_WORKLOADS:
            sys.exit(f"unknown workload '{w}' (expected one of {common.ALL_WORKLOADS})")

    # Must be absolute: each engine wrapper spawns its subprocess with a different cwd
    # (output_dir for leanstore/wiredtiger, BENCHBASE_HOME for postgres), so a relative
    # run_dir would have its derived paths (ssd_path, config_path, ...) re-resolved
    # against the wrong directory when passed as a command-line argument.
    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)

    print("\n########## cross-engine benchmark comparison ##########")
    print(f"run directory : {run_dir}")
    print(f"scale         : {scale.label} (warehouses={scale.tpcc_warehouses}, "
          f"ycsb_records={scale.ycsb_records})")
    print(f"engines       : {engines}")
    print(f"workloads     : {workloads}")
    print("#########################################################\n")

    if not args.skip_build:
        for name in engines:
            ensure_built = getattr(ENGINE_MODULES[name], "ensure_built", None)
            if ensure_built:
                print(f"[build] {name}...")
                ensure_built()

    for workload in workloads:
        for engine_name in engines:
            module = ENGINE_MODULES[engine_name]
            out_dir = run_dir / workload / engine_name
            print(f"=== {workload} / {engine_name} ===")
            try:
                result = module.run(workload, scale, out_dir)
            except Exception as e:  # noqa: BLE001 - one engine's failure shouldn't abort the whole matrix
                result = common.NormalizedResult(
                    engine_name, workload, scale.label, 0.0, "error", 0.0, 0.0, notes=f"EXCEPTION: {e}",
                )
            common.append_manifest_row(manifest_path, result)
            status = "OK" if not result.notes else result.notes
            print(f"    {result.primary_metric_name}={result.primary_metric_value:.2f}  "
                  f"peak_rss={result.peak_rss_mb:.1f}MB  [{status}]")

    print("\n########## comparison complete ##########")
    print(f"manifest : {manifest_path}")
    print(f"plot with: python3 scripts/plot_compare.py --run-dir {run_dir}")
    print("############################################\n")


if __name__ == "__main__":
    main()
