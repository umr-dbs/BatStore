# Range-scan iteration: ordered routing and zero-copy streaming

This document describes the range-iteration changes in
`src/bat_query/iter_query.rs` and their TPC-C/CH-benCHmark call sites. It is
separate from [`range_scan_visibility_check.md`](range_scan_visibility_check.md),
which covers the per-record visibility filter after a leaf has been reached.

## Implemented optimization set

Items 6, 7, and 8 from the range-scan optimization review are enabled. Items
1, 2, 4, and 5 were implemented and benchmarked, but were removed after the
GC stress tests proved their shared child-list premise incorrect:

| Item | Optimization | Implementation |
|---:|---|---|
| 1 | Reuse child-selection allocations | **Rejected.** It depends on eagerly constructing a snapshot child list, which admitted overlapping historical routes under copy-on-write GC. |
| 2 | Avoid hashing small pages | **Rejected.** Boundary hashing/linear deduplication is not a sufficient test for whether historical intervals are superseded. |
| 4 | Hybrid narrow-range traversal | **Rejected in its child-list form.** The active cursor-based traversal is already allocation-free and always selects the newest visible route containing the cursor. |
| 5 | Lean frame storage | **Rejected.** Releasing an internal page after expansion requires a correct immutable child snapshot; retaining the page permits correct cursor-based rerouting. |
| 6 | General streaming APIs | Zero-copy range terminals are exposed by `RangeQueryIter`, generic `DbTransaction`, and `TpccTxn`; YCSB and OLAP consumers use them. |
| 7 | Fold and fallible visitation | `fold_ref`/`range_fold` aggregate directly, while `try_for_each_ref`/`try_range_for_each` stop on the first visitor error. |
| 8 | Specialized count path | `count_ref`/`range_count` count visible records without creating `RecordPointResult`s or cloning payload handles. |

The implementation touches these primary files:

- `src/bat_query/iter_query.rs`: cursor-based routing and iterator terminal
  operations;
- `src/bat_db/transaction.rs`: generic database transaction terminals;
- `src/bat_bench/tpcc_txn.rs`: TPC-C transaction fold/count wrappers;
- `src/bat_bench/olap_scan.rs`: full-scan counting through `range_count`;
- `src/bat_bench/ycsb_txn.rs`: count-only YCSB Scan through `count_ref`;
- `src/bat_bench/tpch_queries.rs`: streaming Q1/Q6 aggregation; and
- `tests/iter_query_tests.rs` and `tests/db_integration_tests.rs`: terminal,
  early-exit, snapshot, and routing coverage.

## Why internal pages cannot be scanned in physical order

An internal page is append/version ordered, not key ordered. Structural
modifications append replacement child entries while retaining historical
entries needed by older snapshots. Consequently, neither walking physical
slots from left to right nor sorting every snapshot-matched slot produces a
correct range scan: the latter includes superseded, overlapping children.

`RangeQueryIter` therefore retains the internal pages on its current path. At
each level it searches entries in reverse append order and selects the newest
snapshot-visible fence containing the current scan cursor. After consuming a
leaf it advances the cursor to one key past that leaf's fence and reroutes
from the retained parent. This is the same selection rule as point lookup and
does not allocate child lists.

### Selecting snapshot-visible children

The active traversal performs the selection in this order:

1. Read the page's `(fence, version, pointer)` entries.
2. Traverse them in reverse append order, newest first.
3. Select the first entry whose version matches the snapshot and whose fence
   contains the current cursor.
4. Descend to that child. After its leaf range is consumed, advance the cursor
   and repeat the selection at the parent.

Two chained `unique_by` calls—and the attempted single-set/linear replacements
for them—are neither needed nor generally correct here. Equal-boundary
deduplication cannot establish that arbitrary historical intervals are fully
superseded. Under copy-on-write GC it allowed both a current route and an
overlapping historical route to be scanned, returning duplicate logical keys.
Cursor routing avoids that inference entirely.

After a leaf is consumed, the scan advances its lower bound to one key past
the leaf fence and reroutes at the retained parent. Leaf
records themselves are still filtered by both the requested range and MVCC
visibility; see the companion visibility document for that hot loop.

An exact full-domain scan (`Key::MIN..=Key::MAX`) is detected once before the
leaf loop. Q1, Q6, and the full-scan benchmark then use a visibility-only
record loop, avoiding two key-bound comparisons per physical record. Every
bounded interval continues to use range-first filtering, which remains faster
when edge leaves contain many records outside the requested range.

The final leaf is a special case. Key increment functions saturate at the
maximum key, so incrementing `Key::MAX` does not move the cursor. Both the
materialized and streaming paths explicitly complete when the consumed fence
reaches either the requested upper bound or the tree maximum. Without this
check, a full-range streaming scan routes the unchanged maximum cursor back
to the final child forever.

## Materialized and streaming interfaces

The normal `Iterator` implementation remains available. It converts each
visible record into `RecordPointResult`, clones the payload handle, and queues
one leaf's matches in `buff`. Use this path when results must outlive the scan
callback, be collected, or be returned to a caller.

`RangeQueryIter::for_each_ref` is the analytical path:

```rust
RangeQueryIter::new(tree, snapshot, range, false, worker_id)
    .for_each_ref(|key, payload| {
        // Fold/count directly while `payload` is borrowed from the leaf.
    });
```

The other iterator terminals have the following shapes:

```rust
let count = RangeQueryIter::new(tree, snapshot, range, false, worker_id)
    .count_ref();

let revenue = RangeQueryIter::new(tree, snapshot, range, false, worker_id)
    .fold_ref(0.0, |sum, _key, row| sum + amount(row));

let result = RangeQueryIter::new(tree, snapshot, range, false, worker_id)
    .try_for_each_ref(|key, row| process(key, row));
```

`try_for_each_ref` returns immediately on the first `Err`. If the iterator
registered its own reader snapshot, its `Drop` implementation releases that
snapshot on early termination. A transaction-owned iterator was created with
`register_reader_si = false`, so the surrounding transaction continues to own
and protect the snapshot after an early return.

It uses the same routing and visibility rules but calls the visitor with
`(Key, &Payload)` directly. It avoids:

- constructing one `RecordPointResult` per match;
- cloning one payload handle per match;
- retaining a leaf-sized result buffer; and
- materializing a whole result vector for analytical folds.

The payload reference is callback-scoped and must not be retained. The
visitor runs while the scan has access to the leaf, so it should perform a
small, non-blocking fold/count rather than re-entering the same tree or doing
long-running work.

`TpccTxn::range_for_each` exposes this path across both standard and runtime-
selected big-tree table classes. `range_fold` and `range_count` provide direct
terminal operations on top. The generic `DbTransaction` exposes
`try_range_for_each`, `range_for_each`, `range_fold`, and `range_count` while
keeping its function-local table `Arc` alive until iteration completes.
`RangeQueryIter` itself provides the corresponding `try_for_each_ref`,
`for_each_ref`, `fold_ref`, and `count_ref` operations.

These APIs are used by:

- `fresh_full_scan_once` via `TpccTxn::range_count`;
- CH-benCHmark Q1 to aggregate delivered order lines by line number; and
- CH-benCHmark Q6 to sum qualifying order-line revenue; and
- YCSB Scan via `RangeIterSi::count_ref`, replacing its temporary result
  vector and payload-handle clones.

The transaction still owns and releases the snapshot. `for_each_ref` does not
release a transaction-owned snapshot early; standalone iterators release only
snapshots they registered themselves.

### Generic database transaction API

`DbTransaction` cannot return a lazy iterator borrowing a function-local table
`Arc`, but it can safely consume that iterator before the method returns:

```rust
let count = tx.range_count(table_id, range);

let total = tx.range_fold(table_id, range, 0u64,
    |sum, _key, payload| sum + value(payload));

tx.range_for_each(table_id, range, |key, payload| {
    consume(key, payload);
});

tx.try_range_for_each(table_id, range, |key, payload| {
    fallible_consume(key, payload)
})?;
```

All four methods use the transaction's existing `worker_id` and fixed
`ts_start`; they do not acquire or release an independent snapshot. The local
table `Arc` stays alive until the terminal operation has consumed the scan.

### TPC-C and benchmark API

`TpccTxn::range_for_each`, `range_fold`, and `range_count` dispatch across both
fixed-size standard tables and runtime-selected Warehouse/District big-tree
sizes. `range_fold` and `range_count` are implemented on top of the streaming
visitor so their behavior is identical across the two tree classes.

The call-site migrations are deliberately limited to consumers that do not
need owned result rows:

- full-database OLAP scanning now sums `range_count` across `Table::ALL`;
- Q1 and Q6 aggregate borrowed `OrderLine` rows directly;
- YCSB Scan returns `RangeIterSi(...).count_ref()`; and
- queries such as Q4/Q5 that need owned/indexed intermediate results retain
  their existing materialized paths.

## Allocation behavior

Routing retains only the current path of `(fence, BlockRef)` pairs. It creates
no child vectors, boundary sets, or per-page sorting buffers. The materialized
iterator still buffers one leaf's matching `RecordPointResult`s; the streaming
terminals avoid that result buffer and payload-handle clones.

## Performance measurement (2026-08-11)

A temporary release-mode microbenchmark loaded 200,000 shuffled `u64` rows,
performed five warmups, and collected 40 alternating materialized/streaming
full-scan samples. Alternating call order prevents one path from consistently
receiving the warmer cache state.

With the original two-`unique_by` child selection:

| Path | Median latency | Median throughput |
|---|---:|---:|
| Materialized iterator | 7.95-8.10 ms | 24.7-25.1 M rows/s |
| `for_each_ref` streaming | 6.82-6.90 ms | 28.8-29.3 M rows/s |

Across three repeated runs, streaming was 1.165x-1.173x faster than the
materialized path (about 16.8%).

An experimental replacement of both `unique_by` adaptors with a shared,
single-set child-list helper measured:

| Path | Median latency |
|---|---:|
| Materialized iterator | 6.54-6.72 ms |
| `for_each_ref` streaming | 5.49-5.59 ms |

Within that experimental implementation, streaming was 1.188x-1.209x faster.
The helper also appeared roughly 18-20% faster than the earlier child-selection version.
That second comparison was made in sequential builds/runs rather than a
simultaneous randomized A/B harness, so treat it as indicative; CPU frequency,
thermal state, and system load were not controlled. The temporary benchmark
was removed after measurement and is not part of the normal test suite. The
child-list implementation itself was subsequently removed for correctness, so
these figures must not be treated as performance numbers for the final routing
implementation.

These numbers apply to a full-scan fold over a synthetic `u64` tree. They
demonstrate the cost avoided by streaming, but they do not substitute for the
TPC-C/CH-benCHmark workload results produced by the regular benchmark harness.

### Follow-up: reusable scratch, hybrid routing, and count terminal

The experimental inline/reusable scratch, single-child fast path, lean frame
states, and `count_ref` build was also measured. Forty full scans and 10,000
scans of a 100-key range were timed per run. Three warm repeated runs produced:

| Operation | Streaming/count terminal | Materialized `Iterator::count` | Speedup |
|---|---:|---:|---:|
| Full 200,000-row scan | 3.78-4.09 ms | 4.50-5.09 ms | 1.18x-1.30x |
| Narrow 100-row scan | 1.22-1.31 us | 1.57-1.67 us | 1.24x-1.37x |

Full-scan count throughput was approximately 49-53 million rows/s in those
warm runs. One colder/load-affected sample reversed the full-scan comparison,
which reinforces the earlier caveat: these are local indicative measurements,
not a controlled benchmark campaign. These routing measurements are retained
only as design history: GC stress testing later exposed duplicate keys, and
the routing portions were reverted. `count_ref` remains enabled.

## Correctness coverage

The routing and streaming changes are covered by focused tests for:

- exact range and iterator results after concurrent shuffled inserts;
- snapshot isolation across concurrent inserts;
- finding the true minimum key despite physical insertion order;
- resolving repeatedly updated keys to their latest visible values; and
- count/fold results and fallible early termination;
- full-domain streaming completion when `inc_key(MAX) == MAX`;
- generic `DbTransaction` zero-copy terminals; and
- Q1/Q6 and YCSB scan correctness.

Structural and transactional regressions added with the follow-up fix also verify that:

- a reader opened before an update and subsequent splits continues through the retained
  historical blocks while GC reuse is enabled;
- repeated delete/reinsert cycles in one transaction commit only their final value;
- aborting those cycles restores the pre-transaction value; and
- WAL recovery reconstructs the final committed value after the tuple-reuse path.

The focused iterator, generic-database, YCSB, TPC-H, and range-version suites
pass. The GC regression was diagnosed by comparing total and unique keys:
each surplus `NewOrder` row was a duplicate produced by overlapping historical
routes. After restoring cursor-based routing, three consecutive runs of each
TPC-C stress mode (copy-on-write and update-in-place GC) passed without
duplicate-key reports.

The suite now includes smoke-scaled TPC-C, YCSB, and WAL backend comparisons and
completes with 135 passed and zero ignored tests (8.23 seconds on the development
machine used for the final run). The default TPC-C comparison uses two warehouses and
two terminals for one second per WAL backend; YCSB uses 20,000 records, two threads,
and one second per backend. The WAL writer microbenchmark retains its original workload
because it already completes in roughly two seconds. Set `BATSTORE_FULL_BENCH=1` to
restore the original large TPC-C/YCSB configurations for dedicated release-mode
measurements; those full configurations are performance runs, not required correctness
tests for an ordinary development machine.
