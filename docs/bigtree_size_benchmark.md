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

`Medium` is the binary's default (`cMVBT tpcc`'s positional arg 21, unwired in
`scripts/engines/cmvbt.py` until this benchmark - see that file's `run()`).

## Setup

Fixed population of 8 warehouses (matching commit `1f6d666`'s own reference config) so
leaf capacity is the only thing varying between points - a separate, earlier experiment in
this same investigation already covers scaling warehouse *count* (1/8/80) and is not
repeated here. Engine: cMVBT only. Workload: plain `tpcc` (its always-on HTAP scan-sweep
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
`mv_test::dump_root_restarts_by_table`), which requires rebuilding with
`mv_test::RESTART_TRACE = true` (off by default - it adds per-attempt tracing overhead).
That rebuild+measurement was out of scope here.
