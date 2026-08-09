# OLTP/WAL optimization: short-test profiling and three fixes

Investigation into whether there was still headroom in cMVBT's OLTP (and, by extension,
OLAP-under-load) performance. Short TPC-C/YCSB tests were run through
`scripts/compare_engines.py` (restricted to `--engines cmvbt`) to see where throughput
stood, then `perf record`/`perf annotate` on a debug-symbol build (`cargo build --profile
profiling`) to see where the CPU time actually went. Three concrete issues turned up and
were fixed; this doc has the before/after numbers.

## What was tested, and why

- **`compare_engines.py --engines cmvbt --workloads tpcc,ycsb_a..ycsb_f --threads 16,64`** -
  the harness's own driver (see `manual.txt`), not a one-off script, so the numbers are
  directly comparable to every other benchmark doc in this repo. Two thread counts (16, 64)
  rather than the full default sweep (2..128) to keep each pass short while still showing
  concurrency-scaling behavior; `--tpcc-duration 20 --ycsb-duration 15` for the same
  reason. 8 warehouses, 1M YCSB records - big enough to not be a toy, small enough to load
  in seconds. Every YCSB letter (A-F) was run, not just one, specifically to separate
  read-only/read-mostly access patterns from write-touching ones - this is what makes it
  possible to say *which* code paths a fix actually helps, rather than just "throughput went
  up."
- **`perf record -F 999 -g --call-graph fp` on `target/profiling/cMVBT`** (the `[profile.
  profiling]` in `Cargo.toml`: release optimizations + debug symbols + frame pointers, via
  `RUSTFLAGS="-C force-frame-pointers=yes"`) directly on the same CLI invocations
  `cmvbt.py` builds, for TPC-C and YCSB-A specifically - YCSB-A (50% read / 50% update,
  Zipfian theta=0.99) is the harness's most write-heavy, most contended point, so it's
  where a CPU-cost-per-write problem shows up loudest. `perf annotate` on the resulting
  hotspots (not just `perf report`'s function-level view) is what actually pinned the CRC32
  cost down to specific instructions, and the channel contention down to a specific `pause`
  spin-loop.
- **Setup gotcha worth knowing:** `scripts/engines/common.py`'s `CMVBT_REPO`
  auto-detection prefers `<workspace>/cmvbt` over this checkout if that directory exists
  and has a `Cargo.toml` - documented in `manual.txt`'s "CMVBT_REPO RESOLUTION" section. A
  stale clone was sitting there from an earlier `setup_environment.py` run, so every
  invocation below pins it explicitly: `CMVBT_REPO=$(pwd) python3 scripts/compare_engines.py
  ...`. Skipping this silently benchmarks the wrong checkout.

## What was found, and fixed

**1. `target-cpu=native` was dead.** `Cargo.toml` had a `[build] rustflags =
["-C","target-cpu=native"]` block, but Cargo only reads `rustflags` from `.cargo/
config.toml`, never from the package manifest - confirmed by `cargo build -v` printing
`warning: unused manifest key: build`. The whole binary had been compiling to the generic
x86-64 baseline (no AVX2/BMI2/POPCNT) despite this setting's presence. **Fix:** moved the
block into a new `.cargo/config.toml`; verified `target-cpu=native` now actually appears
in the `rustc` invocation and the warning is gone.

**2. Hand-rolled CRC32 was the single biggest hotspot.** `mv_wal::record::crc32` was a
byte-at-a-time, 8-shifts-per-byte bit-loop, inlined into `encode_entry_framed` - which
`perf annotate` showed as **22.6% of all CPU cycles** (self time) on YCSB-A at 64 threads,
almost entirely that loop. It runs on every single WAL record's body, on the same thread
that's committing the transaction, so it's a direct tax on every write. **Fix:** replaced
it with a 256-entry lookup-table CRC32 in `src/mv_wal/record.rs` - same algorithm/output
(same IEEE polynomial, same on-disk format), no new dependency, pinned with a test against
the standard `"123456789"` -> `0xCBF43926` check value. `encode_entry_framed`'s self-time
dropped to 11.3% on the same workload.

**3. The single shared WAL channel was a real contention point.** Every worker on a tree
enqueues into *one* `WalWriter` (`mv_tree/mvbt.rs`: "One unified `WalWriter` for this tree
- every worker enqueues into"), backed by one unbounded `crossbeam-channel` and one
background flush thread. `perf annotate` on `Sender::send` showed 86% of its own samples
sitting in a `pause` instruction - crossbeam's internal segment-allocation spin-wait -
worth ~8% of *all* CPU cycles on TPC-C at 64 threads. **Fix:** `WalWriter` now splits
committers across 4 independent (channel, background-flush-thread) shards, keyed by
`worker_id % 4` (`src/mv_wal/writer.rs`). All 4 shards still write through one shared
`Arc<Mutex<File>>` and one shared `hardened` watermark (updated via `fetch_max` instead of
`store`, since multiple flush threads can now complete out of order) - so the on-disk WAL
format, `mv_wal::recovery`, the table catalog, and every existing WAL/recovery test are
completely unaffected. This deliberately targets the *enqueue*-side contention perf found,
not raw I/O throughput - matches the concern that this is a 64-core box, not a reason to
spawn one flush thread per worker.

All 114 of the crate's existing unit tests pass unchanged, including the five WAL/recovery
test files (`wal_writer_tests`, `wal_recovery_tests`, `wal_integration_tests`,
`tree_wal_consistency_tests`, `bench_wal_recovery_stress_tests`) and 2 new tests added for
the table-driven CRC32.

## Results

Same harness invocation, same scale, before vs. after all three fixes - `--engines cmvbt
--workloads tpcc,ycsb_a,ycsb_b,ycsb_c,ycsb_d,ycsb_e,ycsb_f --threads 16,64 --gc on
--warehouses 8 --tpcc-duration 20 --ycsb-records 1000000 --ycsb-duration 15`.

| Workload | Threads | Before (ops/s or new_order/s) | After | Change |
|---|---|---|---|---|
| tpcc | 16 | 40,902 | 43,913 | +7.4% |
| tpcc | 64 | 57,667 | 67,812 | **+17.6%** |
| ycsb_a (50% update, Zipfian) | 16 | 1,603,135 | 2,010,940 | +25.4% |
| ycsb_a (50% update, Zipfian) | 64 | 1,339,295 | 2,258,954 | **+68.7%** |
| ycsb_b (95% read / 5% update) | 16 | 4,336,215 | 4,369,575 | +0.8% |
| ycsb_b (95% read / 5% update) | 64 | 7,323,895 | 7,413,759 | +1.2% |
| ycsb_c (100% read) | 16 | 5,141,791 | 5,112,733 | −0.6% (noise) |
| ycsb_c (100% read) | 64 | 9,444,869 | 9,535,624 | +1.0% |
| ycsb_d (read-latest, 5% insert) | 16 | 1,381,326 | 3,067,711 | +122.1%* |
| ycsb_d (read-latest, 5% insert) | 64 | 1,809,628 | 2,532,175 | +39.9%* |
| ycsb_e (scan-heavy) | 16 | 2,137,315 | 2,483,067 | +16.2% |
| ycsb_e (scan-heavy) | 64 | 1,789,143 | 2,679,747 | **+49.8%** |
| ycsb_f (read-modify-write) | 16 | 1,389,661 | 1,677,370 | +20.7% |
| ycsb_f (read-modify-write) | 64 | 1,355,666 | 2,305,318 | **+70.1%** |

*ycsb_d's jump is larger than its 5%-write share would predict from the other workloads'
pattern - flagged, not fully trusted at face value; see Caveats.

The clearest structural result: **every workload with a write component got faster, and
the two that previously got *worse* with more concurrency (ycsb_a, ycsb_e) now scale up
instead** (ycsb_a: −16% from 16->64 threads before, +12% after; ycsb_e: −16% before, +8%
after). The two workloads with no/near-no writes (ycsb_b, ycsb_c) are flat within noise, as
expected - confirms the fixes are surgical to the write path, not incidentally taxing reads.

Perf-recorded runs (profiler overhead included, so not directly comparable to the table
above, but useful for the relative before/after on the same instrumented binary):

| Metric (YCSB-A, 64 threads, under `perf record`) | Before | After |
|---|---|---|
| Throughput | 1,287,648 ops/s | 2,086,939 ops/s (+62%) |
| `encode_entry_framed` self-time | 22.6% | 11.3% |
| `crossbeam_channel::Sender::send` self-time | 7.9% | 0.3% |

| Metric (TPC-C, 64 threads, under `perf record`) | Before | After |
|---|---|---|
| tpmC (new-order/min) | 2,907,028 | 3,632,766 (+25%) |

Raw manifests: `comparison_results/run_20260809_184829` (before),
`comparison_results/run_20260809_192707` (after).

## Caveats / what this doesn't cover

- **Short durations (15-20s measured phase) by design** - fast enough to iterate on, but
  the TPC-C HTAP scan-sweep only completes 7 scans per run, so its scan-latency percentiles
  (p99 swinging from 209µs to 6,053µs between the two thread points) are not trustworthy on
  their own; a longer run would be needed to say anything real about OLAP scan latency
  under this load.
- **ycsb_d's outsized gain is unverified** - only one trial each side, no repeated runs to
  separate signal from run-to-run variance (explicitly not re-run further per instruction
  to stop testing). Worth an independent re-check before relying on that specific number.
- **What these fixes do *not* address:** TPC-C's OCC conflict rate is still very high at 8
  warehouses/64 terminals (Payment: ~71-76% of attempts conflict and are discarded; New-Order:
  ~47%) - pure wasted `traversal_write_olc`/`smo::split` work, unrelated to the WAL. The
  B-tree's own optimistic-lock-coupling backoff (`__sched_yield`, ~8-9% of cycles on
  contended writes) is also unchanged - neither was in scope for this pass.
- Only cMVBT was benchmarked (`--engines cmvbt`), not the other six engines this harness
  can drive - this doc is about cMVBT's own headroom, not a cross-engine comparison.
