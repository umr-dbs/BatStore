# Range-scan visibility check: filter reorder, and an attempted bigger lever

Follow-up to the OLAP performance pass in `docs/bigtree_size_benchmark.md`. That
investigation found the dominant OLAP cost is inherent to the physical layout (a leaf's
whole record array, live and dead alike, has to be scanned — see that doc). This one looks
at what's left to optimize purely in the *scan-iteration* code itself, without touching
leaf/compaction layout.

For the complementary internal-page routing change and the zero-copy
`RangeQueryIter::for_each_ref` analytical path, see
[`range_scan_iteration.md`](range_scan_iteration.md). This document covers
only the per-record visibility hot loop once routing has reached a leaf.

## What was found

`RangeQueryIter::refill` (`src/mv_query/iter_query.rs`) and
`MVBTSt::key_range_read_from_root` (`src/mv_query/query.rs`) both filter each physical
record with:

```rust
r.version().matches(is_visible) && range.contains(r.key())
```

`VersionInfo::matches` (`src/mv_record_model/version_info.rs`) calls `is_visible` up to
twice per record (once for the insertion stamp, once more for the deletion stamp if
present) — i.e. on *every* dead/superseded record a leaf holds, which per the linked doc
is the majority of a garbage-heavy leaf's contents. Before this fix, `is_visible` was a
`&mut dyn FnMut(TxStamp) -> bool` — a type-erased, non-inlinable indirect call — so this
work was paid *before* the cheap, inlinable `range.contains` check ever got a chance to
reject the record.

## Fix 1 (shipped): reorder the filter

Swapped the order in both places: `range.contains(r.key()) && r.version().matches(is_visible)`.
Pure reorder of an existing `&&` — zero behavior change, no API touched, full test suite
clean across repeated runs.

No benefit for a full-table OLAP scan (its range always contains every key in any leaf it
visits at all). Real, mechanistically-explained benefit for a *narrow* range scan sharing
a leaf with keys outside its own range — e.g. CH-benCHmark Q4/Q5's per-order `OrderLine`
sub-scans, which land on leaves shared with many other orders' lines. Measured (8
warehouses/16 terminals, `ch` OLAP mode, 20s runs, noisy/contended, not averaged — treat as
indicative): Q4 latency ~614-617ms before → ~505-507ms after (~18% faster, consistent
across both samples each run); Q5 ~6.81-6.90s before → ~6.09s after (~11-13% faster). Q1/Q6
(full-table scans, no structural benefit expected) showed comparable-or-noisy numbers
either way, matching the theory: the win only shows up exactly where the mechanism
predicts it should.

## Fix 2 (shipped, after a scare): remove the `dyn FnMut` indirection

The bigger lever: reordering only helps *out-of-range* records. Every record actually in
range still pays for a non-inlinable indirect call on every `matches` invocation. Removing
that entirely — making the visibility check a concrete, monomorphized closure the compiler
can inline — would help every touched record, not just the skippable ones.

Scoped deliberately to `RangeQueryIter` only, not `query.rs`'s two call sites:

- `VersionInfo::matches` was generalized from `&mut dyn FnMut(TxStamp) -> bool` to
  `<F: FnMut(TxStamp) -> bool + ?Sized>(&mut F)` — backward compatible, since
  `dyn FnMut(..)` itself satisfies `FnMut(..) + ?Sized`, so every existing caller passing
  an actual trait object keeps compiling and behaving identically with zero changes.
- `TxContext`/`MVBTSt` got a new `with_snapshot_cache_and_logs` method alongside (not
  replacing) the existing `with_visibility_checker`, handing back the raw
  `(&mut SnapshotCache, &[CommitLog])` pair instead of a pre-built type-erased closure —
  so the caller builds its *own* concrete `is_visible` closure directly in its own
  function body, inlinable, instead of receiving one across a generic-callback boundary
  (which can only be named as `dyn` on the far side of that boundary).
- `RangeQueryIter::refill` was restructured to call `with_snapshot_cache_and_logs` once
  per `refill()` invocation (wrapping every leaf that one call visits, not once per leaf),
  build `is_visible` inline, and use it directly — no `dyn` anywhere in this path anymore.
- `query.rs`'s two call sites (`key_point_read_from_root`, `key_range_read_from_root`) were
  left completely untouched, still going through the original `with_visibility_checker`.

This compiled cleanly on the first try and the reasoning holds up: `dyn Trait: Trait`
always holds, so the generalization is provably behavior-preserving for every existing
caller, and the new method is pure infrastructure alongside the old one.

**First verification pass looked like a regression, and almost got reverted for it.**
Running the full test suite repeatedly surfaced `verify_concurrent_shared_keys::
concurrent_read_modify_write_across_a_small_shared_key_set_never_loses_an_update` (a
timed, real-thread concurrent read-modify-write stress test) failing intermittently with
the change applied (2/5, then 4/20 in early samples) against a first baseline sample that
had come back a clean 10/10. That pattern — a concurrency test flaking only with a change
touching concurrency-adjacent code — is exactly what a real bug from this kind of change
would look like, and is exactly what the earlier `unsafe_degree_root` garbage-ratio attempt
this same session turned out to be (see project memory): reverted first, investigated after.

**Re-investigating properly reversed that call.** A wider baseline sample (unmodified
code, no changes at all) taken right after, under the same now-more-loaded machine, came
back 2/19 failures (~10.5%) on the *exact same* test and panic line — the earlier clean
10/10 baseline was drawn before the machine warmed up under this session's own repeated
heavy test/build/benchmark load, not because the flake wasn't there. A clean, side-by-side
20-run batch with the dyn-removal change applied came back 2/20 (10%) on the same test and
line — statistically indistinguishable from the baseline rate, not a multiple of it. This
is a **pre-existing, rare, load-sensitive flake in `verify_concurrent_shared_keys` itself,
unrelated to this change** — worth its own separate investigation (a "never loses an
update" test failing at all, even rarely, is a real correctness signal), but not a reason
to withhold this fix.

**Net effect measured** (same `ch` OLAP mode / 8-warehouse setup as fix 1, still
noisy/contended short runs): Q4 latency ~505-533ms with this fix added, down from the
original ~614-617ms baseline before either fix (fix 1 alone already got to ~505-507ms).
The dyn-removal's own incremental contribution on top of fix 1 alone wasn't cleanly
separable from run-to-run noise at this scale (indirect-call overhead is a few ns/record,
easily swamped by everything else moving between short contended runs) — the correctness
result above is the real point of this fix; treat any further latency number here as
indicative, not precise.

**Lesson for next time this comes up:** don't trust a single small sample (5-10 runs) in
either direction for a timed concurrency test on a machine that's been running heavy
benchmarks for a while — get a same-conditions baseline sample of comparable size *before*
concluding a change caused a failure, not after reverting on the assumption it did.
