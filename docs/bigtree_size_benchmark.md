# BigTreeSize benchmark: picking Warehouse/District's leaf capacity

Commit `1f6d666` ("Give district/warehouse their own tunable leaf capacity via a
size-class enum") added `BigTreeSize` (`Tiny`/`Small`/`Medium`/`Large`/`Huge`), a
runtime-selectable enum choosing the leaf capacity used specifically by TPC-C's
`Table::Warehouse` and `Table::District` trees (the base tree's own leaf capacity, 123
records, is unrelated and unaffected). District and warehouse are tiny tables (10/1 rows
per warehouse) but extremely hot by write volume, so a bigger leaf reduces how often their
physical version chains overflow and force a write-locking compaction - at the cost of a
larger leaf to scan/copy. The five variants land page-exact on:

| Size | Leaf capacity (records) | Page size |
|---|---|---|
| Tiny | 251 | 8 KiB |
| Small | 507 | 16 KiB |
| Medium | 1019 | 32 KiB |
| Large | 2043 | 64 KiB |
| Huge | 16379 | 512 KiB |

`Medium` is the binary's default (`batstore tpcc`'s positional arg 21, unwired in
`scripts/engines/batstore.py` until this benchmark - see that file's `run()`).

## Setup

Fixed population of 8 warehouses (matching commit `1f6d666`'s own reference config) so
leaf capacity is the only thing varying between points - a separate, earlier experiment in
this same investigation already covers scaling warehouse *count* (1/8/80) and is not
repeated here. Engine: BatStore only. Workload: plain `tpcc` (its always-on HTAP scan-sweep
OLAP thread supplies the scan-latency columns). Every run pinned to NUMA node 0
(`numactl --cpubind=0 --membind=0`, baked into `scripts/engines/common.py`) on the 2x AMD
EPYC 7742 server. 8s per point, 20 points total (5 sizes x 2 thread counts x gc on/off).

## Results

| Size | Threads | GC | tpmC-equiv (new_order/s) | Peak RSS (MB) | Scan p50 (µs) | Scan p99 (µs) |
|---|---|---|---|---|---|---|
| Tiny | 16 | on | 48,453 | 16,529 | 62 | 653 |
| Tiny | 16 | off | 52,364 | 19,293 | 64 | 1,718 |
| Tiny | 64 | on | 67,030 | 23,068 | 82 | 184 |
| Tiny | 64 | off | 73,796 | 34,707 | 91 | 279 |
| Small | 16 | on | 49,080 | 16,558 | 54 | 2,910 |
| Small | 16 | off | 52,591 | 19,193 | 67 | 520 |
| Small | 64 | on | 69,224 | 23,950 | 76 | 134 |
| Small | 64 | off | 72,182 | 29,477 | 76 | 110 |
| **Medium** (default) | 16 | on | 48,728 | 15,560 | 56 | 2,163 |
| **Medium** (default) | 16 | off | 51,655 | 18,951 | 61 | 2,151 |
| **Medium** (default) | 64 | on | 68,474 | 22,752 | 86 | 106 |
| **Medium** (default) | 64 | off | 73,338 | 33,961 | 101 | 111 |
| Large | 16 | on | 48,609 | 17,334 | 66 | 1,772 |
| Large | 16 | off | 52,251 | 21,826 | 58 | 1,234 |
| Large | 64 | on | 69,141 | 22,946 | 76 | 89 |
| Large | 64 | off | 75,913 | 30,005 | 82 | 127 |
| Huge | 16 | on | 48,785 | 15,736 | 59 | 920 |
| Huge | 16 | off | 52,902 | 22,045 | 65 | 2,838 |
| Huge | 64 | on | 69,697 | 22,858 | 97 | 110 |
| Huge | 64 | off | 73,677 | 34,749 | 90 | 97 |

Raw manifest: `comparison_results/run_20260809_135605_bigtree/manifest.csv`.

## Findings

- **tpmC is flat across all five sizes**, confirming commit `1f6d666`'s own claim: at
  threads=64 every size lands within ~69K-76K new_order/s regardless of leaf capacity; at
  threads=16, within ~48K-53K. Leaf size has essentially no effect on raw OLTP throughput
  at this scale.
- **Scan p50 latency is flat too** (54-101µs across the board) - no size makes OLAP scans
  meaningfully slower or faster here.
- **Scan p99 is noisy at threads=16 for every size** (520-2,910µs, no consistent ordering
  by size) but **converges tight at threads=64** (89-279µs) - the 8s/16-terminal points are
  too short/low-contention for a clean p99 read; the threads=64 column is the useful signal.
- **Peak RSS scales mildly with leaf size** but is dominated by thread count, not tree size
  - 8 warehouses is too small a population for even Huge's 512 KiB leaves to show a real
    footprint difference.

## Recommendation

**Medium (32 KiB / 1019 records) remains the good pick.** It's the current default, ties
for best-or-tied-for-best on every metric measured here, and per commit `1f6d666`'s own
(unreproduced by this benchmark) root-restart measurement it already captured most of the
contention win (district root restarts 18.9M -> 4.3M) without Huge's much larger
per-leaf memory cost or Tiny's higher restart rate.

**Caveat:** this benchmark only measures throughput/latency, which commit `1f6d666`
already noted is flat across sizes - it does not distinguish the sizes on the actual
metric that motivated `BigTreeSize` in the first place (per-table root-restart count,
`bat_test::dump_root_restarts_by_table`), which requires rebuilding with
`bat_test::RESTART_TRACE = true` (off by default - it adds per-attempt tracing overhead).
That rebuild+measurement was out of scope here.

## Addendum (2026-08-11): the "flat scan latency" finding above was a duration artifact

Re-investigated as part of a broader OLAP-performance pass. The "Scan p50/p99 flat
across all five sizes" finding above does **not** hold at a longer time horizon, and the
mechanism `tpcc_schema.rs`'s doc comment describes (bigger leaf -> deferred compaction ->
more dead-version garbage per scan, since `bat_query::iter_query::RangeQueryIter` scans a
leaf's whole physical record array, live and dead alike) reproduces cleanly once the OLAP
thread's `scan_after_delay_once` snapshot is actually given time to age:

8 warehouses/16 terminals, `sweep` OLAP mode (isolates the warehouse+district scan from
every other table, unlike this doc's own `fresh_full_scan`-style default), `olap_param=5`
(delays 0,1,2,3,4,5s), 30s run, release build, single run per point (not averaged - treat
as indicative, not final):

| Delay | KiB32 (`32kib`, default) scan latency | KiB512 (`512kib`) scan latency |
|---|---|---|
| 0s  | 155µs | 159µs |
| 1s  |  56µs | 249µs |
| 2s  |  39µs | 294µs |
| 3s  |  58µs | 343µs |
| 4s  |  61µs | 354µs |
| 5s  |  60µs | 269µs |

Both scans return the identical 88 rows (8 warehouses + 80 districts) every time - the
latency gap is pure physical-record-touched overhead, not output size. KiB512 is
consistently ~4-6x slower than the default past the first (near-zero-garbage) point. This
session's original test above used the default `scan_sweep` olap_param (10, so delays
0..=10) at only **8s per point** - the run ends before the OLAP thread's delay list even
reaches the 4-5s range where the gap opens up, which is why it read as flat. The original
`1f6d666`/`tpcc_schema.rs` claim (~100% -> ~21% of baseline OLAP throughput between the
untouched leaf and `KiB512`) should be treated as the accurate one; this doc's "Scan
p50/p99 flat" finding above was an artifact of too-short aging, not a real absence of the
effect, and the "Medium ties for best-or-tied-for-best on every metric measured here"
recommendation is only reliable for tpmC/RSS, not for scan cost under sustained load.

Also added (this session): `bat_test::SCAN_TRACE` (off by default, same dead-code-
eliminated-when-off idiom as `RESTART_TRACE`) - counts records-visited vs.
records-matched per leaf across every `RangeQueryIter` scan in the process
(`bat_test::record_leaf_scan`/`dump_scan_trace`). Enabling it for the same 30s run
(system-wide, not isolated to the OLAP thread - it also counts every OLTP transaction's
own point/range reads) showed **5.74x records visited per record matched**, identical at
both leaf sizes - expected, since the other 12 (default-sized) tables' aggregate read
volume swamps warehouse/district's contribution to the global counter. Confirms the
visited-vs-matched "garbage tax" is a real, measurable, systemic phenomenon in this
engine (worth keeping in mind generally, not just for `BigTreeSize`), but this coarse a
counter can't isolate one table's contribution - a per-tree or per-table breakdown (the
same pattern `dump_root_restarts_by_table` already uses for restarts) would be needed to
attribute it the way the latency table above does directly.
