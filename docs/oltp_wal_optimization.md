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

## Follow-up: a lock-free WAL backend, an OOM in its own benchmark, and a cross-shard durability race found merging the two

A separate, concurrent line of work pursued the same "WAL append path" contention this
doc's fix #3 targets, but via a different mechanism: instead of splitting the *shared*
channel into shards, `LockFreeWalWriter`/`LockFreeWalBackend`
(`src/mv_wal/lockfree_writer.rs`/`backend.rs`) removes the channel and the dedicated
writer thread entirely - every worker reserves its own byte range in the file via
`tail.fetch_add` and calls `pwrite` itself, directly, optionally batching its own records
locally (`LocalBatch`) before flushing. `WalBackend` wraps both `WalWriter` (this doc's
sharded channel) and `LockFreeWalWriter` behind one enum so callers pick either at attach
time (`enable_wal`/`enable_wal_lockfree`).

**Synthetic microbenchmark** (`tests/wal_writer_throughput_bench.rs`, debug build, this
repo's current state - not the `perf`-profiled numbers above):

| Backend | @8 threads | @16 threads |
|---|---|---|
| `WalWriter` (4-way sharded, this doc's fix #3) | 1,006,755 ops/s | 1,353,816 ops/s |
| `LockFreeWalWriter`, unbatched | 752,179 ops/s | 748,676 ops/s (p99.9 latency: 1.68ms) |
| `LockFreeWalWriter` + `LocalBatch(64)` | 2,549,030 ops/s | **5,089,502 ops/s** |

Unbatched lock-free writes lose to the sharded channel past a few threads - each write pays
a full `pwrite` syscall, and (per that module's own doc) some filesystem-level
inode-extension serialization on a shared growing file. Per-thread batching (`LocalBatch`)
is what actually wins: same lock-free append, but each thread groups its own records into
one `pwrite` per `batch_size`, amortizing the same syscall cost the sharded channel amortizes
a different way (batching *within* a group-commit window instead of *across* threads).

**Bug #1 (memory, not WAL): `RESTART_TRACE` OOM'd the real-workload comparison.**
Running `tests/tpcc_wal_backend_bench.rs`'s full-scale `compare_wal_backends_tpcc` (16
warehouses, 16 terminals, `lockfree-batch64`) got OOM-killed at **12GB+ RSS**, taking the
whole terminal session with it. Cause: `mv_test::RESTART_TRACE` (a write-restart diagnostic,
documented "off by default") had been left `true` in checked-in code. Every OCC restart
records a `String` key into a process-lifetime global map that nothing ever clears; the
comparison loop calls `run_tpcc()` 6 times in one process, so this accumulates across every
iteration, and the *fastest* config (last in the loop, `lockfree-batch64`/16 terminals -
exactly the backend this section is benchmarking) generates restarts fast enough to blow
through available memory before the run even finishes. Reproduced at a *tiny* scale (1
warehouse, 200ms runs) in well under a second: footprint grew from 214 to 877 entries across
4 in-process `run_tpcc()` calls with tracing left on. **Fix:** restored the documented
default (`false`); added `mv_test::reset_restart_trace()`, called at the start of every
`run_tpcc()`, so repeated in-process runs never cross-contaminate even if tracing is
deliberately turned back on for an investigation.

**Bug #2 (WAL, found merging with fix #3 above): cross-shard `hardened_version` race.**
`WalWriter`'s sharded channel (this doc's fix #3) and the lock-free backend above were
developed concurrently and landed via a merge. The sharded channel's `hardened_version`
published every shard's own confirmed watermark into *one* shared atomic via `fetch_max` -
correct only if every shard's watermark is comparable, which it isn't: `ts_start` is one
global sequence spread across shards unpredictably by `worker_id % NUM_SHARDS`, not
partitioned per shard. One busy shard's flush could advance the shared watermark past a
`ts_start` a *different*, slower shard hadn't flushed yet.
`tests/tree_wal_consistency_tests.rs::concurrent_db_transactions_across_tables_match_shared_wal_exactly`
caught it directly: `wal_state.len()` came back `1200` instead of the expected `1800` - a
quarter of the written records missing from the reconstructed WAL because
`wait_wal_hardened` had returned before every shard actually flushed.

A first fix attempt (aggregate = minimum of every shard's own raw `confirmed` value) traded
the false-positive for a hang: a shard whose own submissions simply never reach some
*other* shard's higher watermark would never satisfy a plain `min` comparison, even once
it had flushed everything it was ever going to receive - `wait_wal_hardened` polled forever.
**Actual fix:** give each shard a `submitted` watermark alongside `confirmed` (`submitted`
updated in `enqueue`, strictly before the message is sent, so it can only ever be a stale
*overestimate* of in-flight work, never an underestimate). The aggregate now treats a shard
as imposing **no constraint** once `confirmed >= submitted` ("fully drained, nothing
outstanding regardless of the numeric gap to any other shard"), the same way an always-idle
shard already imposed none - only a shard that's genuinely behind (`confirmed < submitted`)
contributes its own `confirmed` value to the aggregate's minimum. Verified with the exact
failing test run 5x with no flakiness, plus the full 126-test suite run 3x clean.

**New: fast, always-on WAL backend perf smoke tests.** `tests/tpcc_wal_perf_tests.rs`/
`tests/ycsb_wal_perf_tests.rs` sweep the batched vs. lock-free backends end to end (real
TPC-C/YCSB driver, not a synthetic WAL-only microbenchmark) at a deliberately tiny scale -
under 20s wall time combined, RSS growth asserted under a 20GB ceiling - so a regression
like either bug above gets caught on every `cargo test`, not only in the full-scale,
`#[ignore]`d comparison this section started from.

## Second follow-up: re-profiling after the WAL fixes above surfaced two new hotspots

With the WAL append path no longer the dominant cost (channel contention fixed, CRC32
table-driven, lock-free option available), a short re-profiling pass - `perf record -F 999
-g --call-graph fp` on the `profiling` build, 15s TPC-C (8 warehouses/16 threads) and
YCSB-A (1M records/16 threads) runs, WAL on (batched/sharded backend) - found two new,
purely allocator-side hotspots that used to be masked by the channel contention this doc's
fix #3 removed.

**Hotspot #1: a flush ticket built on every write, and discarded on every write.**
`WalWriter::enqueue` constructed a fresh `crossbeam_channel::bounded(1)` on *every* call to
return a "flush ticket" (`Receiver<()>`). Checked every production call site
(`mv_sync::version_handle`): the ticket is always discarded - nothing in production ever
calls `wait_flushed`. This showed up directly: `crossbeam_channel::channel::bounded` at
1.7% self-time on the TPC-C profile, plus its own `RawVecInner::finish_grow`/allocator
churn. **Fix:** split each logging method (`log_with_stamp`/`log_commit`/the `_for_table`
variants/`start_commit_logged`/`start_commit_logged_for_table`) into a ticket-less default
(no channel built at all - matches every real caller) plus an explicit `*_with_ticket`
variant for the handful of tests that genuinely wait on one record.

**Hotspot #2: framing buffers sized for the wrong payload.** Every framing call used a
hardcoded `Vec::with_capacity(24..44)`, tuned for this module's own `u64`-payload tests.
Real payloads are far bigger: `YcsbRow` (10 fields x 100 bytes = 1000 bytes) is roughly
**25x** that capacity; `TpccRow` variants like `Customer`/`Stock` routinely run 300-400+
bytes, 7-10x over. Every WAL write on a real workload was paying for several grow-and-copy
reallocations during encoding - visible as `RawVecInner::finish_grow`/`do_rallocx` costing
7.1% self-time on TPC-C, and a large chain of kernel page-fault handling
(`alloc_pages_mpol`/`get_page_from_freelist`/a real `native_queued_spin_lock_slowpath` at
7.4% self-time) on YCSB, where the undersizing is worst. **Fix:** added
`WalPayload::wal_encode_size_hint()` (default `8`, exact for the base `u64` payload), with
exact overrides for `YcsbRow` (`4 + self.len()`, trivial since the length is already known)
and `TpccRow` (mirrors `wal_encode` field-for-field, so it always matches what's about to
be written). `record::entry_size_hint()` combines this with the fixed header/frame
overhead; every hardcoded constant in `writer.rs`/`lockfree_writer.rs` was swapped for it.

**Results**, same short runs the analysis used, before vs. after both fixes:

| Metric | Before | After | Change |
|---|---|---|---|
| TPC-C, WAL on (tpmC) | 2,675,157 | 3,013,193 | **+12.6%** |
| YCSB-A, WAL on (ops/sec) | 1,374,921 | 1,723,455 | **+25.3%** |
| Synthetic `WalWriter` @ 16 threads, `u64` payload (`wal_writer_throughput_bench.rs`) | 1,353,816 ops/s | 2,427,071 ops/s | **+79%** |

The synthetic case gained the most: against an 8-byte payload, the discarded ticket's
channel allocation *was* essentially the entire per-write cost once the payload itself was
already correctly sized (`u64`'s hint was exact from the start), so removing it alone
nearly doubled throughput. `crossbeam_channel::channel::bounded` no longer appears in the
TPC-C profile at all; `RawVecInner::finish_grow`'s self-time dropped from 7.1% to 3.0%.

**Not fully closed:** YCSB's page-fault/allocator chain is smaller but still present -
right-sizing the buffer removed the grow-and-copy, but a fresh ~1000-byte `Vec` allocated
per write is still a fresh allocation per write. A true buffer pool (reuse across calls,
recycled once the flush thread frees it) would close this further, but needs solving
cross-thread buffer ownership - the bytes have to travel through the channel to the flush
thread, so a simple thread-local scratch buffer doesn't compose with that directly. Flagged
as a candidate for a future pass, not attempted here.

Verified: full 126-test suite passes; `wal_writer_tests.rs`/`wal_recovery_tests.rs` updated
to call the new `_with_ticket` methods where they actually wait on a specific record's
flush.

## Third follow-up: chasing the still-open per-write allocation

Picking back up the "not fully closed" item above - the fresh `Vec` still allocated per
write in `WalWriter::log_with_stamp_impl`, whose bytes then travel through the channel to
be freed on a *different* thread (the flush thread).

**Dead end: a thread-local scratch buffer on `LockFreeWalWriter`.** Reasoned that the
*lock-free* writer's `log_with_stamp`/`log_commit` (and `_for_table` variants) don't have
`WalWriter`'s cross-thread problem at all - they `pwrite` synchronously on the calling
thread, so a plain `thread_local! { RefCell<Vec<u8>> }`, cleared and reused per call, should
be a strict win with none of the cross-thread-ownership complexity. Implemented and
verified correct (55/55 WAL tests pass), but tracing the actual call graph found it
**dead weight**: every real caller of `LockFreeWalWriter` goes through `LockFreeWalBackend`
(`mv_wal/backend.rs`), whose `log_with_stamp`/`log_commit` always call
`LocalBatch::push_write`/`push_commit` first - appending into `LocalBatch`'s own
persistent `bytes: Vec<u8>` (cleared, not dropped, on every `flush_batch`) - before ever
reaching the raw method that was patched. That's true even at `batch_size: 1`. So the only
callers that ever hit the unbatched method directly were this module's own unit tests and
`wal_writer_throughput_bench.rs`'s deliberately-unbatched comparison arm - not production,
not any driver benchmark. **Reverted** rather than keep a change with no real effect.

**Allocator swap experiment: mimalloc, opt-in, kept off by default.** The genuine
cross-thread pattern lives in `WalWriter`: one thread mallocs the framed buffer, the flush
thread frees it once its bytes are copied into that cycle's combined batch (`flush_loop`,
`writer.rs`). jemalloc's per-thread arenas route a cross-thread free back to the *owning*
arena, which needs locking that arena's bin; mimalloc instead gives every page a lock-free
"thread-free list" a foreign thread's `free()` can push onto without any lock - a design
its own benchmarks call out for exactly this producer-consumer shape. Added an opt-in
`mimalloc` Cargo feature (`Cargo.toml`, `src/main.rs`) that swaps `MiMalloc` in as
`#[global_allocator]`; `mv_bench::mem_stats::read_jemalloc_stats` now returns `None` under
that feature instead of reporting stats from an idle jemalloc arena (the allocator-
independent `VmRSS` column, and `scripts/plot_suite.py`'s RSS-over-time plots, are
unaffected either way).

Measured end to end (real driver, not the synthetic writer microbenchmark, which was too
noisy on this machine - run-to-run variance up to ~40% with *no* code change at all,
swamping any allocator effect):

| Backend | Workload | jemalloc | mimalloc | Change |
|---|---|---|---|---|
| `batched` (`WalWriter`, has the cross-thread free) | YCSB-A, 8 threads | 1,744,821 ops/s | 1,797,272 ops/s | **+3.0%** |
| `batched` | YCSB-A, 16 threads | 2,011,806 ops/s | 2,057,250 ops/s | **+2.3%** |
| `lockfree-batch16`/`64` (`LocalBatch`, no cross-thread free) | YCSB-A, 8/16 threads | - | - | -1.3% .. +0.3% (noise) |
| `batched` | TPC-C, 2 terminals | 1,772,013 tpmC | 1,712,769 tpmC | **-3.3%** |
| `batched` | TPC-C, 4 terminals | 2,762,752 tpmC | 2,626,646 tpmC | **-4.9%** |
| `lockfree-batch16`/`64` | TPC-C, 2/4 terminals | - | - | **-0.1% .. -4.1%** (also down) |

The YCSB result is mechanistically clean: a real gain specifically on the one backend with
the cross-thread free, a wash on the two backends that never had it. TPC-C tells the
opposite story - mimalloc is worse across *every* backend, including the `LocalBatch` ones
that have nothing to do with this allocation pattern, all 6 (backend x terminal-count)
combinations moving the same direction (a ~1.6% chance of that being pure noise). Net:
mimalloc trades a small, mechanism-specific YCSB win for a broader, workload-level TPC-C
loss - **not** adopted as the default allocator (jemalloc stays `#[global_allocator]`),
kept as an opt-in feature. `scripts/engines/common.py::cmvbt_cargo_build_args()` (read from
`CMVBT_ALLOCATOR`) plus `compare_engines.py --cmvbt-allocator {jemalloc,mimalloc}` let a
future comparison run pick either without editing `Cargo.toml`.

**Still not fully closed.** Neither of the two things tried here actually eliminates the
per-write allocation in `WalWriter` - the thread-local buffer doesn't compose with a value
that has to cross threads, and the allocator swap only makes the free cheaper (and only
helps one of the two workloads tested). The buffer-pool design flagged in the previous
follow-up - a bounded pool of reusable buffers per shard, with buffers returned to the pool
right after `flush_loop` copies their bytes into the batch buffer (well before the
`pwrite`/`fsync`, so the pool doesn't need to wait on durability) - remains the candidate
that would close this for real, and is still unimplemented.
