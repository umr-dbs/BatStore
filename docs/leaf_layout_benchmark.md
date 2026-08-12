# Isolated leaf layout benchmark

Date: 2026-08-12

This microbenchmark compares the production-style array-of-structures (AoS) record
layout with a proposed two-region layout, without tree traversal, latches, WAL, GC,
transactions, allocation, or payload cloning. Its primary test uses 16,384 independently
boxed leaf pages per layout, a 62.5 MiB working set for each representation.

## Layouts

Both pages contain 125 physical records and occupy exactly 4,000 bytes.

- **AoS:** `[Record { key: u64, begin: u64, end: u64, payload: u64 }; 125]`
- **Split:** `[key: u64; 125]` followed by
  `[RecordData { begin: u64, end: u64, payload: u64 }; 125]`

The 32-byte AoS entry matches the size of the production leaf's
`RecordPoint<u64, u64>`. The split layout uses an 8-byte key and 24 bytes of remaining
record information, so capacity and total bytes are held constant.

Pages cycle through 25, 50, 75, 100, and 125 physical records: 20%, 40%, 60%, 80%, and
100% occupancy, respectively. Thus no empty or less-than-20%-full leaf participates. Each
page has five versions per logical key. Records are append-ordered in five update waves,
matching the important production property that a leaf is not key-sorted. Point lookup
walks backward, compares keys, and checks visibility only after a key match. Scan lookup
checks the range before visibility. Results are consumed through `black_box`.

Every leaf is allocated in its own `Box`; the benchmark retains vectors of pointers rather
than one contiguous array of leaf objects. Point tests visit boxes in a fixed randomized
permutation. Both layouts use the same order, occupancies, keys, versions, and queries.

## Results

Median of five runs, 750 ms per layout and operation, pinned to CPU 0. One sweep performs
one point lookup in every leaf, or scans all 1,228,750 occupied physical slots. Lower
milliseconds are better; speedup greater than 1 means the split layout is faster.

| Operation | AoS ms/sweep | Split ms/sweep | AoS throughput | Split throughput | Split speedup |
|---|---:|---:|---:|---:|---:|
| Point: latest visible | 0.536 | 0.494 | 30.57 Mlookups/s | 33.17 Mlookups/s | 1.085x |
| Point: one-version fallback | 0.889 | 0.813 | 18.43 Mlookups/s | 20.15 Mlookups/s | 1.093x |
| Point: three-version fallback | 1.815 | 1.487 | 9.03 Mlookups/s | 11.02 Mlookups/s | 1.221x |
| Scan: full leaves | 3.806 | 3.832 | 322.84 Mslots/s | 320.65 Mslots/s | 0.993x |
| Scan: 50% key ranges | 3.630 | 3.564 | 338.50 Mslots/s | 344.77 Mslots/s | 1.019x |
| Scan: 20% key ranges | 3.463 | 3.158 | 354.82 Mslots/s | 389.09 Mslots/s | 1.097x |

Test system: AMD Ryzen 9 5900X, Rust 1.97.1, Linux x86-64. Compilation used
`-C opt-level=3 -C target-cpu=native`.

## Interpretation

Across the 62.5 MiB boxed-leaf working set, the split key region improves point lookup:
8.5% for the usual latest-visible lookup, 9.3% after one visibility fallback, and 22.1%
after three fallbacks. This supports the key-density/cache-pressure hypothesis that the
single hot key stream can reject unrelated entries without fetching every record's
version and payload cache lines.

Full scans are effectively tied: the split layout is 0.7% slower, which is within the
small run-to-run variation. Selective scans benefit because the key range can be tested
before touching the second region: 1.9% at 50% selectivity and 9.7% at 20% selectivity.

The result is materially different from a single hot 4 KiB leaf, where the complete page
fits in cache and the second address stream can erase the benefit. The multi-leaf test is
more representative of a tree whose active leaves exceed cache, but it still isolates the
physical leaf access rather than claiming an end-to-end tree speedup.

This remains deliberately leaf-only. It does not measure tree traversal, writes,
split/merge cost, concurrency, payload cloning, or the engineering impact of losing a
directly addressable `&[RecordPoint]`. Both complete layout sets coexist in memory during
the process, but only one is touched during a timed sample. Allocation and page creation
are outside the timed region.

## Reproduction

```bash
rustc --edition=2024 -C opt-level=3 -C target-cpu=native \
  tools/leaf_layout_bench.rs -o /tmp/cmvbt_leaf_layout_bench
taskset -c 0 /tmp/cmvbt_leaf_layout_bench
```
