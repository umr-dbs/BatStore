#!/usr/bin/env python3
"""Same cross-engine benchmark harness as compare_engines.py, but replaces its tmpfs
hack with each engine's own durability configuration.

compare_engines.py guarantees "no real disk I/O" by forcing every engine's on-disk data
onto a tmpfs (RAM-backed) scratch directory, regardless of that engine's own fsync/WAL
settings (see engines/common.py::fresh_scratch_dir and manual.txt section 4). That
works, but it means every engine still pays real fsync/serialization cost on every
write, and the same bytes sit in RAM twice at once (once as buffer-pool cache, once
again as the tmpfs "disk" file underneath it).

This script instead sets engines/common.py's NO_DURABILITY flag, which asks each engine
wrapper to turn its own durability work off directly, then lets it run against a normal
real-disk scratch directory (engines/common.NO_DURABILITY_SCRATCH_ROOT, default
<WORKSPACE_ROOT>/no_durability_scratch) instead of requiring tmpfs:

  - BatStore (engines/batstore.py): WAL switched off entirely (wal_enabled=false) - no
    partial "log but skip fsync" toggle exists in bat_wal/writer.rs (its sync_data()
    call is unconditional whenever WAL is on at all), so full WAL-off is the only way to
    remove its durability cost. Unlike compare_engines.py, WAL is NOT forced on here.
  - LeanStore (engines/leanstore.py): passes --wal_pwrite=false --wal_fsync=false
    explicitly (backend/leanstore/Config.cpp) - WAL logging itself (--wal) stays on,
    since it's baked into the B-tree core (WALMacros.hpp) and can't be disabled, but
    with pwrite/fsync both off no WAL bytes ever reach disk or get flushed. Both flags
    already default to false upstream, so this makes the "off" state explicit rather
    than an implicit dependency on that default.
  - libmdbx (engines/libmdbx.py): unchanged - src/bat_bench/mdbx_{ycsb,tpcc,s_htap}.rs
    already open every environment with SyncMode::UtterlyNoSync unconditionally, in
    compare_engines.py too.
  - vWeaver_ermia / vweaver_ermia_frugal (engines/vweaver_ermia*.py): unchanged -
    ERMIA's own -null_log_device gflag (benchmarks/dbtest.cc) already defaults to true
    upstream (dbcore/sm-log-alloc.cpp skips the log pwrite whenever it's set), in
    compare_engines.py too.
  - PostgreSQL (engines/postgres_benchbase.py): unchanged durability handling -
    synchronous_commit/fsync/full_page_writes were already forced off by
    _set_unsafe_durability() regardless of this script - only its now-redundant tmpfs
    datadir check (_verify_tmpfs_datadir) is skipped.
  - WiredTiger (engines/wiredtiger.py, via LeanStore's WiredTigerAdapter): UNCHANGED,
    same as compare_engines.py - this is the one engine where "not possible" without
    more invasive surgery actually applies. Its log config is a hardcoded C++ string in
    frontend/shared/WiredTigerAdapter.hpp (see patches/leanstore.patch), already off the
    per-commit fsync path via transaction_sync=(enabled=false) but still logging
    (log=(enabled=true)); fully disabling that needs a source patch and rebuild, not a
    script-level config knob, so it's left exactly as compare_engines.py runs it - just
    without the tmpfs requirement on its own ssd_path, like every other engine here.
  - Umbra (engines/umbra_benchbase.py): NOT POSSIBLE AT ALL, unlike every engine above -
    confirmed directly against a live umbra-server that neither `ALTER SYSTEM SET
    fsync/synchronous_commit/...` nor plain `SET ...` can change any of those GUCs in
    this build ("ALTER SYSTEM not implemented yet" / "cannot change configuration
    parameter"), and there's no equivalent command-line flag either. So this script's
    entire premise - real disk, relying on the engine's own fsync-off - has no safe
    form for Umbra: run() reports every point SKIPPED instead of silently benchmarking
    real-disk fsync cost under a flag whose whole point is "no durability cost". Use
    compare_engines.py (tmpfs-backed) for Umbra numbers.

Sets BATSTORE_BENCH_NO_DURABILITY=1 before importing compare_engines - every behavior
difference above flows from engines/common.py's NO_DURABILITY flag (read once, at
import time) and the handful of engines/*.py call sites that check it. Identical CLI,
workloads, and thread/GC sweep semantics otherwise - see compare_engines.py's own
docstring for that shared usage; this file only changes durability/scratch handling.

Usage: identical to compare_engines.py, e.g.
    python3 scripts/compare_engines_new.py
    python3 scripts/compare_engines_new.py --tiny --threads 2,4
    python3 scripts/compare_engines_new.py --engines batstore,leanstore --workloads tpcc,ycsb_e
"""
from __future__ import annotations

import os

# Must be set before compare_engines (and, transitively, engines/common.py) is imported -
# NO_DURABILITY is read from this env var once, at import time.
os.environ["BATSTORE_BENCH_NO_DURABILITY"] = "1"

from compare_engines import main  # noqa: E402 - env var above must be set first

if __name__ == "__main__":
    main()
