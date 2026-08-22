# Leaf-level zone-map pruning for range scans

Follow-up to the scan-worker-pool/`RangeQueryIter`-parallel-dispatch stream
(`0cb6c17`..`9388b57`) and, further back, `docs/bigtree_size_benchmark.md`'s
finding that OLAP cost is dominated by physical leaf layout (a leaf's whole
record array, live and dead alike, has to be visited). This one asks a
different question about that same layout: is there *any* free space in a
leaf page cheap enough to buy a way to skip visiting some leaves outright,
for range scans with a predicate on a non-key column (CH-benCHmark
Q1/Q6's `ol_delivery_d` filters, the same queries `range_scan_visibility_
check.md` already optimized the per-record hot loop for)?

## What was found: 16–24 free bytes in every leaf, for free

`bat_tree::mvbt`'s own top-of-file doc already documents that `FAN_OUT =
123` (not 125) exists specifically so `OptCell<Block<..>>` lands exactly on
a 4096B page boundary with zero waste. That doc is about the *outer*
allocation; it doesn't say whether the *inner* leaf struct itself has any
slack relative to that budget. Measured directly (`std::mem::size_of`,
`Key = Payload = u64`):

| type | size |
|---|---|
| `InternalPage<123,123,u64,u64>` | 3960B |
| `LeafPage<123,u64,u64>` | 3944B |
| `Node<123,123,u64,u64>` (the union of the two, plus header) | 4032B |
| `OptCell<Block<123,123,u64,u64>>` | 4096B |

`Node`'s payload is a union of `InternalPage`/`LeafPage`, sized to the
larger of the two — so the leaf side sits on **16 idle bytes** already
baked into the layout as pure padding, at zero cost. Confirmed empirically,
not just arithmetically: adding a real `[u8; N]` field to `LeafPage` and
rebuilding showed `Node`/`Block` staying at 4032B and `OptCell` staying at
exactly 4096B for `N` up to 24; at `N = 32`, `Node` jumps to 4096B and
`OptCell` to 4160B — off the page-aligned jemalloc size class, the same
misalignment failure mode `mvbt.rs`'s own doc describes for `FAN_OUT =
125`. So the real, safe budget is ~16–24 bytes, and 24 costs exactly the
same as 16 — there's no reason to leave the extra 8 bytes as padding.

A Bloom filter doesn't fit this budget (needs on the order of 1 byte/key,
i.e. >100B for 123 keys, to get a useful false-positive rate) and wouldn't
help range scans anyway — membership tests don't prune ranges. A min/max
zone map (2×`u64`) fits exactly in 16 bytes, with an extra 8 bytes to
spare.

## Design: `LeafZoneMap`

`src/bat_page_model/leaf_page.rs`: a 24-byte `struct LeafZoneMap { lo: u64,
hi: u64, non_null_count: u64 }` added to `LeafPage`. `lo > hi` is the empty
state (no separate flag needed). `[lo, hi]` is a **safe superset** of every
value ever projected out of a record physically stored in this leaf,
including stale MVCC versions not yet GC'd — it only ever widens, never
shrinks, so it can prune a leaf it's *certain* can't match, never one that
might. `non_null_count` lets an all-null leaf (e.g. every `ol_delivery_d`
still unset) be recognized independent of the `lo`/`hi` sentinel.

Opt-in, one projection per tree (`bat_tree::mvbt::MVBTSt::
set_zone_map_projection(&self, fn(&Payload) -> Option<u64>)`, backed by a
`OnceLock` — not `&mut self`, since `Database::create_table_unpublished`
only ever hands back a shared `Arc<MVBTSt>`, the same reason `enable_gc`
is `&self`-based). Off by default: `None` is a complete no-op, no leaf ever
touches its zone map.

**Write path** (`bat_query::dispatch.rs`): every place a payload is
actually written (`Insert`, `Update`, `update_with`'s in-place fast path)
computes the projected value and calls `LeafPage::widen_zone_map` right
after. A signed column (e.g. `ol_delivery_d: Option<i64>`) is encoded via
the standard sign-bit flip, `(v as u64) ^ (1u64 << 63)`, to preserve total
order as `u64` — shared as `bat_bench::tpcc_schema::encode_signed_zone_value`
so the write-side projection and read-side predicate can never use
different encodings.

**Read path** (`bat_query::iter_query.rs`): `RangeQueryIter::
with_zone_predicate(lo, hi)` stores a bound; `try_for_each_ref` (the one
choke point every scan variant — `for_each_ref`, `count_ref`, all four
`*_parallel` methods — funnels through) does two things from that one
bound and the tree's own registered projection, so they can never disagree:
skips a whole leaf outright once `LeafZoneMap::may_intersect` proves it
can't hold a match, and — for leaves that aren't skipped — automatically
excludes any individual record whose own projected value falls outside the
bound, as part of the existing range/visibility filter. That second part
means a caller's own closure no longer needs to (and shouldn't redundantly)
re-check that column itself: `q1`/`q1_parallel` dropped their manual
`ol_delivery_d <= delivered_before` check entirely; `q6`/`q6_parallel`
dropped their date-range check and kept only `ol_quantity < max_qty` (not
a zone-mapped column, so it can't be automated the same way without a
second projection). Both cross-checked against `tree.cold.
zone_map_projection.is_some()` before consulting anything, so calling
`with_zone_predicate` against a tree with no matching projection configured
degrades to "no predicate at all" instead of silently treating every leaf's
unpopulated zone map as "definitely no match" and dropping real results.

Wired to `ORDER_LINE`/`ol_delivery_d` in `TpccDatabase::create_all_tables`
(the single place all three constructors funnel through), so it's on by
default for every `TpccDatabase`.

## Bug found and fixed: split/merge silently defeated pruning for the exact workload this targets

First implementation seeded a freshly split/merged leaf's zone map from its
source leaf(s)' *existing* zone map (`LeafZoneMap::absorb`, a union) rather
than recomputing it from the records actually landing in the new leaf —
reasoned to be safe (a superset of a superset is still a superset) and
cheap (no need to re-run the projection during split/merge). It is safe.
It is also, for the single most realistic access pattern this feature
exists for, almost completely useless.

**Why:** under a monotonically increasing key (`ORDER_LINE`'s `o_id`,
correlated with `ol_delivery_d` exactly like real TPC-C data), a B-tree
under sustained insertion keeps splitting off its *low* end while
continuing to grow to the *right* — the leaf holding the newest keys splits
repeatedly, but always inherits the zone map of whichever leaf it was
split from, going back to that lineage's very first leaf. Since a zone map
only ever widens, `lo` from that original first leaf propagates forward
through every descendant split, forever — even though each new rightmost
leaf's *actual* content has long since moved on to much higher values.
After enough splits, effectively every leaf in the tree ends up with `lo`
stuck near the table's oldest value, so a predicate anchored at the low
end (exactly what `q1`'s `delivered_before` and `q6`'s `date_lo` are)
never prunes anything, anywhere.

Measured directly (see below): the first working version of `q1` with a
narrow date cutoff visited **2622 of 2622 leaves — zero pruning**, despite
every correctness test passing (a too-wide zone map is safe, just useless;
nothing catches "useless" without measuring the actual leaf count). An
earlier correctness test built with *random*-order key insertion (a
synthetic `u64` tree, not TPC-C) never caught this, because random
insertion doesn't produce the "one lineage keeps growing right" pattern in
the first place — key-split there naturally partitions by value too, so
seeding-from-source happened not to matter for that test's shape of data.

**Fix:** `bat_tree::smo::push_records_onto` now recomputes the zone map
fresh, folding the tree's own projection over exactly the `Vec<RecordPoint>`
about to be written into the new leaf — not seeded from any source leaf at
all. One more pass over `records`, which is already being iterated once to
build the page anyway. This is the only place split/merge needed to
change; the write-path `widen_zone_map` calls and the read-path predicate
logic were unaffected.

## Measured

`tests/bench_tpch_correctness_tests.rs::
zone_map_pruning_speeds_up_a_narrow_q1_predicate` (permanent, runs by
default): 8 warehouses × 20,000 `ORDER_LINE` rows (160,000 total),
`ol_delivery_d` set correlated with insertion order. `q1` with a cutoff
matching all rows vs. one matching only the earliest ~2%:

| | leaves visited | records visited | records matched | latency |
|---|---|---|---|---|
| full range | 2622 | 160,000 | 160,000 | 24.9ms |
| narrow (~2%) | **60** | 3,660 | 3,208 | **6.0ms** |

97.7% of leaves skipped, ~4.1x faster. Leaf-visited counts gathered via a
one-off `bat_test::SCAN_TRACE` flip (reverted after — it costs a global
mutex lock per leaf visit, not something to leave on by default); the
permanent test only asserts/prints timing and row counts, not leaf counts,
so it doesn't depend on that flag.

**A transient, unrelated test failure surfaced while `SCAN_TRACE` was still
flipped on**: `bench_s_htap_stress_tests::
concurrent_default_mix_with_lateness_keeps_every_row_readable_and_scans_
never_overcount` panicked once (`LeafPage::record`, index-out-of-bounds)
during that same investigation session. 17 subsequent runs with
`SCAN_TRACE` back off (2 full suite runs + 15 targeted repeats) all passed.
Same lesson as `range_scan_visibility_check.md`'s dyn-removal investigation:
a global-lock-adding diagnostic flag perturbing timing enough to expose a
rare, pre-existing, apparently unrelated concurrency edge case is not the
same thing as a regression from the actual code change — but it's worth
recording here in case that specific panic (index equals len, in
`LeafRecordIter`/`LeafPage::record`) resurfaces on its own and needs a real
investigation.

## Scope / not done

- Only `ORDER_LINE` has a projection configured; `Warehouse`/`District`
  (`TreeClass::Big`) and every other standard table don't track anything.
- Only `try_for_each_ref` (and everything built on it) checks the
  predicate — the plain `Iterator`/`refill` path (used by `next()`/
  `min_by_key`) doesn't.
- `UpdateRand`/`InsertRand` (benchmark-only paths) and WAL-recovery replay
  don't call `widen_zone_map` — safe (a leaf's zone map just doesn't widen
  from those writes, never wrong), but means recovered/rand-populated data
  gets less precise pruning until touched by a real write again.
- One projection per tree: `q6`'s `ol_quantity < max_qty` condition isn't
  on the zone-mapped column, so it can't be automatically derived the way
  the date-range condition now is.
