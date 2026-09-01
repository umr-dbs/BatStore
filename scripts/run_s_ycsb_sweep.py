#!/usr/bin/env python3
"""Run an S-YCSB hot-update-skew x thread-count sweep in one result directory.

S-YCSB is retained as ``s_htap`` in the engine-wrapper and manifest interfaces for
backward compatibility.  User-facing text and options use the S-YCSB name.  Each total
thread count is split into the configured OLAP scanner count and the remaining write
threads, exactly as in compare_engines.py.

Examples:
    python3 scripts/run_s_ycsb_sweep.py
    python3 scripts/run_s_ycsb_sweep.py --gc on
    python3 scripts/run_s_ycsb_sweep.py --tiny --skews uniform,0.99 --threads 2,4
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime
import json
import math
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from clean_leanstore_runs import clean_run as clean_engine_run
from engines import (
    batstore, common, leanstore, libmdbx, postgres_benchbase, umbra_benchbase, wiredtiger,
)


ENGINE_MODULES = {
    "batstore": batstore,
    "leanstore": leanstore,
    "wiredtiger": wiredtiger,
    "postgres": postgres_benchbase,
    "umbra": umbra_benchbase,
    "libmdbx": libmdbx,
}
DEFAULT_ENGINES = ["batstore", "leanstore", "wiredtiger", "postgres", "libmdbx"]
DEFAULT_SKEWS = ["uniform", "0.1", "0.4", "0.8", "0.99", "1.4"]
DEFAULT_THREADS = [2, 4, 8, 16, 32, 64, 128]


def skew_to_theta(skew: str) -> float:
    return 0.0 if skew == "uniform" else float(skew)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--output-root", default="s_ycsb_sweep_results")
    p.add_argument(
        "--engines", default=",".join(DEFAULT_ENGINES),
        help=f"comma-separated subset of {list(ENGINE_MODULES)}",
    )
    p.add_argument(
        "--skews", default=",".join(DEFAULT_SKEWS),
        help="comma-separated hot-update skew points; use 'uniform' for theta 0",
    )
    p.add_argument(
        "--threads", default=None,
        help=f"comma-separated total thread counts (default {DEFAULT_THREADS}, or [2,4] with --tiny)",
    )
    p.add_argument(
        "--gc", default="on",
        help="comma-separated subset of on,off (default: on); engines without a toggle run once",
    )
    p.add_argument("--tiny", action="store_true", help="use the smoke-test scale")
    p.add_argument("--skip-build", action="store_true")
    p.add_argument(
        "--batstore-allocator", "--cmvbt-allocator", dest="batstore_allocator",
        choices=["jemalloc", "mimalloc"], default="jemalloc",
    )
    p.add_argument("--record-count", "--s-ycsb-record-count", type=int)
    p.add_argument("--duration", "--s-ycsb-duration", type=int)
    p.add_argument("--hot-window", "--s-ycsb-hot-window", type=int)
    p.add_argument("--arrival-ratio", "--s-ycsb-arrival-ratio", type=float)
    p.add_argument("--max-lateness", "--s-ycsb-max-lateness", type=int)
    p.add_argument("--olap-threads", "--s-ycsb-olap-threads", type=int)
    p.add_argument("--olap-lag", "--s-ycsb-olap-lag", type=int)
    p.add_argument("--olap-span", "--s-ycsb-olap-span", type=int)
    p.add_argument("--ycsb-payload", choices=["standard", "u64"], default="standard")
    p.add_argument("--ycsb-key-only", action="store_true")
    p.add_argument("--dram-gib", type=float)
    return p.parse_args()


def _validate(args: argparse.Namespace, scale: common.Scale, engines: list[str], skews: list[str],
              threads: list[int], gc_list: list[str]) -> None:
    unknown = [engine for engine in engines if engine not in ENGINE_MODULES]
    if unknown:
        sys.exit(f"unknown engine(s) {unknown}; expected a subset of {list(ENGINE_MODULES)}")
    if not engines:
        sys.exit("--engines must contain at least one engine")
    if not threads or any(thread <= 1 for thread in threads):
        sys.exit("--threads must contain integers >= 2 (S-YCSB needs a writer and a scanner)")
    if not gc_list or any(gc not in ("on", "off") for gc in gc_list):
        sys.exit("--gc must contain one or both of: on,off")
    if not skews:
        sys.exit("--skews must contain at least one value")
    try:
        theta_values = [skew_to_theta(skew) for skew in skews]
    except ValueError as exc:
        sys.exit(f"invalid --skews value: {exc}")
    if any(not math.isfinite(theta) or theta < 0.0 for theta in theta_values):
        sys.exit("--skews values must be finite and non-negative")
    if scale.s_htap_record_count <= 0 or scale.s_htap_duration <= 0:
        sys.exit("--record-count and --duration must be positive")
    if scale.s_htap_hot_window <= 0 or scale.s_htap_olap_span <= 0:
        sys.exit("--hot-window and --olap-span must be positive")
    if not 0.0 <= scale.s_htap_arrival_ratio <= 1.0:
        sys.exit("--arrival-ratio must be between 0 and 1")
    if scale.s_htap_max_lateness < 0 or scale.s_htap_olap_lag < 0:
        sys.exit("--max-lateness and --olap-lag must be non-negative")
    if scale.s_htap_olap_threads <= 0:
        sys.exit("--olap-threads must be positive")
    if args.dram_gib is not None and args.dram_gib <= 0:
        sys.exit("--dram-gib must be positive")


def main() -> None:
    args = parse_args()
    os.environ["BATSTORE_ALLOCATOR"] = args.batstore_allocator
    os.environ["YCSB_PAYLOAD_BYTES"] = "8" if args.ycsb_payload == "u64" else "1000"
    os.environ["YCSB_FIELD_COUNT"] = "1" if args.ycsb_payload == "u64" else "10"
    os.environ["YCSB_FIELD_LENGTH"] = "8" if args.ycsb_payload == "u64" else "100"
    os.environ["YCSB_WRITE_ALL_FIELDS"] = "false"

    base_scale = dataclasses.replace(common.TINY_SCALE) if args.tiny else common.Scale()
    overrides = {
        "s_htap_record_count": args.record_count,
        "s_htap_duration": args.duration,
        "s_htap_hot_window": args.hot_window,
        "s_htap_arrival_ratio": args.arrival_ratio,
        "s_htap_max_lateness": args.max_lateness,
        "s_htap_olap_threads": args.olap_threads,
        "s_htap_olap_lag": args.olap_lag,
        "s_htap_olap_span": args.olap_span,
        "dram_gib": args.dram_gib,
    }
    for field, value in overrides.items():
        if value is not None:
            setattr(base_scale, field, value)

    engines = [value.strip() for value in args.engines.split(",") if value.strip()]
    skews = [value.strip() for value in args.skews.split(",") if value.strip()]
    threads = (
        [int(value.strip()) for value in args.threads.split(",") if value.strip()]
        if args.threads else ([2, 4] if args.tiny else list(DEFAULT_THREADS))
    )
    gc_list = [value.strip() for value in args.gc.split(",") if value.strip()]
    _validate(args, base_scale, engines, skews, threads, gc_list)

    run_dir = Path(args.output_root).resolve() / f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    manifest_path = run_dir / "manifest.csv"
    common.write_manifest_header(manifest_path)
    config = {
        "kind": "s_ycsb_skew",
        "workloads": ["s_htap"],
        "engines": engines,
        "skews": skews,
        "threads": threads,
        "gc": gc_list,
        "s_ycsb": {
            "record_count": base_scale.s_htap_record_count,
            "duration": base_scale.s_htap_duration,
            "hot_window": base_scale.s_htap_hot_window,
            "arrival_ratio": base_scale.s_htap_arrival_ratio,
            "max_lateness": base_scale.s_htap_max_lateness,
            "olap_threads": base_scale.s_htap_olap_threads,
            "olap_lag": base_scale.s_htap_olap_lag,
            "olap_span": base_scale.s_htap_olap_span,
        },
        "ycsb_payload": args.ycsb_payload,
        "ycsb_read_payload": not args.ycsb_key_only,
    }
    (run_dir / "run_config.json").write_text(json.dumps(config, indent=2) + "\n")

    actual_runs_per_point = sum(
        len(gc_list) if getattr(ENGINE_MODULES[name], "SUPPORTS_GC_TOGGLE", False) else 1
        for name in engines
    )
    actual_runs = len(skews) * len(threads) * actual_runs_per_point
    print("\n########## S-YCSB skew sweep ##########")
    print(f"run directory : {run_dir}")
    print(f"engines       : {engines}")
    print(f"hot skews     : {skews} (theta={[skew_to_theta(skew) for skew in skews]})")
    print(f"total threads : {threads} (up to {base_scale.s_htap_olap_threads} OLAP scanners)")
    print(f"gc sweep      : {gc_list}")
    print(f"planned runs  : {actual_runs} (>= {actual_runs * base_scale.s_htap_duration / 60:.1f} min measured)")
    print("########################################\n")

    if not args.skip_build:
        failed_builds = []
        for name in engines:
            ensure_built = getattr(ENGINE_MODULES[name], "ensure_built", None)
            if not ensure_built:
                continue
            print(f"[build] {name}...")
            try:
                ensure_built()
            except (SystemExit, subprocess.CalledProcessError) as exc:
                print(f"    [build] {name} FAILED - excluding it: {exc}")
                failed_builds.append(name)
        engines = [name for name in engines if name not in failed_builds]
        if not engines:
            sys.exit("No engines left after build failures")

    workload = "s_htap"
    for engine_name in engines:
        module = ENGINE_MODULES[engine_name]
        for skew in skews:
            theta = skew_to_theta(skew)
            for thread_count in threads:
                scale_variant = dataclasses.replace(
                    base_scale,
                    ycsb_threads=thread_count,
                    s_htap_theta=theta,
                    label=f"{base_scale.label} skew={skew}",
                )
                if args.dram_gib is None:
                    scale_variant = dataclasses.replace(
                        scale_variant,
                        dram_gib=common.dram_gib_for(workload, scale_variant),
                    )
                out_dir = (
                    run_dir / workload / engine_name / f"skew_{skew}" /
                    f"threads_{thread_count}"
                )
                print(
                    f"=== S-YCSB / {engine_name} / skew={skew} (theta={theta}) / "
                    f"threads={thread_count} ==="
                )
                try:
                    results = common.run_gc_variants(
                        module, workload, scale_variant, out_dir, gc_list,
                        {
                            "reload": engine_name == "postgres",
                            "ycsb_payload": args.ycsb_payload,
                            "read_payload": not args.ycsb_key_only,
                        },
                    )
                except Exception as exc:  # noqa: BLE001 - preserve the rest of a long sweep
                    results = [
                        (gc, common.NormalizedResult(
                            engine_name, workload, scale_variant.label,
                            scale_variant.s_htap_duration, "error", 0.0, 0.0,
                            threads=thread_count, gc_enabled=gc,
                            notes=f"EXCEPTION: {exc}",
                        ))
                        for gc in gc_list
                    ]
                for gc, result in results:
                    common.append_manifest_row(manifest_path, result)
                    print(
                        f"    gc={gc} {result.primary_metric_name}="
                        f"{result.primary_metric_value:.2f} scan_p99={result.scan_p99_us:.1f}us "
                        f"(n={result.scan_count}) [{result.notes or 'OK'}]"
                    )

    print("\nCleaning unneeded engine run artifacts...")
    clean_engine_run(run_dir, delete=True, verbose=False)
    print("\n########## S-YCSB sweep complete ##########")
    print(f"manifest : {manifest_path}")
    print(f"plot with: python3 scripts/plot.py {run_dir}")
    print("############################################\n")


if __name__ == "__main__":
    main()
