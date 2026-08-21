use std::collections::VecDeque;
use std::convert::Infallible;
use std::fmt::Display;
use std::hash::Hash;

use crate::bat_page_model::BlockRef;

use crate::bat_page_model::node::PageType;
use crate::bat_page_model::time_matcher::TimeMatcher;
use crate::bat_query::SnapShot;
use crate::bat_query::interval::{Interval, RangeSplit};
use crate::bat_query::snapshot::ReaderIsolatedSnapShot;
use crate::bat_record_model::record_point::RecordPointResult;
use crate::bat_record_model::tx_stamp::WorkerId;
use crate::bat_record_model::version_info::Version;
use crate::bat_sync::worker::READ_ONLY_SCAN_WORKER_ID;
use crate::bat_tree::mvbt::MVBTSt;
use crate::bat_tree::scan_pool::ScanWorkerPool;

/// Software-prefetch hint for `block`'s backing memory — issued far enough
/// ahead of the actual dereference (see call sites below) to hide some of
/// that cache-miss latency behind other work instead of stalling on it.
/// Skipped entirely on non-`x86_64` targets; either way this can never
/// affect correctness, only (hopefully) timing.
#[inline(always)]
fn prefetch_block<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
>(
    block: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(block.0 as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = block;
}

/// Peeks — without mutating any scan state — the block `refill`'s or
/// `try_for_each_ref`'s *next* loop iteration will descend into right after
/// finishing `curr_fence`'s leaf, and prefetches it. `path`'s second-to-last
/// entry is exactly that leaf's parent internal page: it's still on the
/// stack (only the leaf itself gets popped once processed), so this is the
/// very same reverse `find` the `IndexRef` branch already runs against it —
/// just run one step early, before this leaf's own records are visited,
/// so the sibling's cache lines are in flight while that happens instead of
/// only being requested once the scan actually gets there.
///
/// A pure hint: no parent on the stack (root is itself a leaf), no matching
/// child (this was genuinely the last leaf in range), or a version mismatch
/// (shouldn't happen mid-traversal of one immutable snapshot, but nothing
/// here needs to assert that) all just skip the prefetch.
#[inline(always)]
fn prefetch_next_leaf<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
>(
    path: &[(Interval<Key>, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)],
    next_lower: Key,
    si: SnapShot,
) {
    let Some(parent_index) = path.len().checked_sub(2) else {
        return;
    };
    if let PageType::IndexRef(parent) = path[parent_index].1.as_page_ref() {
        let (keys, versions) = parent.keys_versions();
        if let Some((pos, _)) = versions.iter().zip(keys).enumerate().rev().find(
            |(_, (version, fence))| version.matched(si) && fence.contains(next_lower),
        ) {
            prefetch_block(&parent.get_pointer(pos));
        }
    }
}

pub struct RangeQueryIter<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> {
    pub(crate) isolated_snapshot: ReaderIsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub(crate) range: Interval<Key>,
    path: Vec<(Interval<Key>, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
    buff: VecDeque<RecordPointResult<Key, Payload>>,
    is_completed: bool,
    register_reader_si: bool,
    worker_id: WorkerId,
}

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> Drop for RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn drop(&mut self) {
        // ensure snapshot is released even if user didn't consume all data
        if !self.is_completed && self.register_reader_si {
            self.bat_tree()
                .on_release_reader_snapshot(self.snapshot().into())
        }
    }
}

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn new(
        tree: &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
               version: Version,
               range: Interval<Key>,
               register_reader_si: bool,
        worker_id: WorkerId,
    ) -> Self {
        let root = tree.retrieve_root_for(version);
        Self::new_with_root(tree, version, range, register_reader_si, worker_id, root)
    }

    /// Builds a range iterator from a root already resolved for `version`.
    /// A fixed-snapshot transaction can therefore resolve each table's root
    /// once and reuse it for all later point/range reads.
    #[inline(always)]
    pub(crate) fn new_with_root(
        tree: &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
        version: Version,
        range: Interval<Key>,
        register_reader_si: bool,
        worker_id: WorkerId,
        root: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ) -> Self {
        if register_reader_si {
            tree.on_acquire_reader_snapshot(version);
        }

        Self {
            isolated_snapshot: ReaderIsolatedSnapShot(version, tree),
            range,
            path: vec![(Interval::new(tree.cold.min_key, tree.cold.max_key), root)],
            buff: VecDeque::new(),
            is_completed: false,
            register_reader_si,
            worker_id,
        }
    }

    #[inline(always)]
    pub const fn si(&self) -> &ReaderIsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
        &self.isolated_snapshot
    }

    #[inline(always)]
    pub const fn snapshot(&self) -> SnapShot {
        self.si().snapshot()
    }

    #[inline(always)]
    pub const fn bat_tree(&self) -> &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.si().bat_tree()
    }
}

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> Iterator for RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    type Item = RecordPointResult<Key, Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        self.refill();
        self.buff.pop_front()
    }
}

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Advances the scan until `self.buff` holds at least one more match —
    /// always a whole leaf's worth at once, since a leaf's live/visible
    /// records are filtered into `buff` together in one `extend` call
    /// below — or the range is exhausted. Factored out of `Iterator::next`
    /// (which pops one item off the front once this returns) so
    /// `min_by_key` below can reuse the exact same leaf-fetching logic
    /// without popping, to compare every match a leaf produced rather than
    /// trusting whichever happens to land first in `buff`.
    fn refill(&mut self) {
        if !self.buff.is_empty() {
            return;
        }

        let si = self.snapshot();

        // A direct copy of the stored `&'a MVBTSt` (references are `Copy`),
        // not a call through `self.bat_tree()` — the latter's elided return
        // lifetime ties to `&self`, which would keep `self` borrowed for as
        // long as `tree` (or anything capturing it, like `is_visible` below)
        // is alive, conflicting with the `&mut self.buff`/`self.path` calls
        // later in this same loop.
        let tree: &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> = self.isolated_snapshot.1;

        let inc = tree.cold.inc_key;

        let worker_id = self.worker_id;

        // `with_snapshot_cache_and_logs`, not `with_visibility_checker`:
        // builds `is_visible` as a concrete, `Sized` closure right here
        // (see that method's doc) so `VersionInfo::matches`'s calls into it
        // below are ordinary, inlinable calls instead of an indirect `dyn
        // FnMut` dispatch — worth it specifically here since this loop can
        // call it up to twice per *physical* record in a leaf (live and
        // dead alike, see `refill`'s own module-level doc), not just once
        // per matched result. Called once per `refill()` invocation
        // (wrapping every leaf this call visits), not once per leaf: `si`/
        // `worker_id` never change within one call, so there's nothing to
        // gain from rebuilding `is_visible` per leaf, and one fewer TLS
        // cache lookup on the (common, see `SCAN_TRACE`'s findings)
        // multi-leaf-per-call path.
        tree.with_snapshot_cache_and_logs(|cache, commit_logs| {
            let mut is_visible = |stamp| {
                crate::bat_sync::visibility::is_visible(commit_logs, cache, worker_id, si, stamp)
            };

            loop {
                if self.path.is_empty() || self.range.lower > self.range.upper {
                    // Only release if *this iterator* is the one that
                    // registered the snapshot (`register_reader_si`) —
                    // mirrors `Drop`'s own guard just below. Without it, a
                    // `Transaction`-owned range scan (`Transaction::range`,
                    // `register_reader_si: false`, since the `Transaction`
                    // itself registered `ts_start` at `begin()` and
                    // releases it at `commit()`/drop) would have its
                    // *first* fully-drained range scan release the
                    // transaction's snapshot registration early — leaving
                    // every later read in the same transaction (any further
                    // `tx.point`/`tx.range` call) running with no GC
                    // protection at all, since the registration is already
                    // gone.
                    if self.register_reader_si {
                        tree.on_release_reader_snapshot(si);
                    }

                    self.is_completed = true;
                    return;
                }

                let (curr_fence, curr_block) = self.path.last().unwrap().clone();

                match curr_block.as_page_ref() {
                    PageType::IndexRef(internal_page) => {
                        let (keys, versions) = internal_page.keys_versions();
                        if let Some((pos, (_, fence))) =
                            versions.iter().zip(keys).enumerate().rev().find(
                                |(_, (version, fence))| {
                                    version.matched(si) && fence.contains(self.range.lower)
                                },
                            )
                        {
                            self.path.push((*fence, internal_page.get_pointer(pos)));
                        } else {
                            self.path.pop();
                            self.range.lower = inc(curr_fence.upper);
                        }
                    }
                    PageType::LeafRef(leaf_page) => {
                        let reached_end = curr_fence.upper >= self.range.upper
                            || curr_fence.upper == tree.cold.max_key;
                        if !reached_end {
                            prefetch_next_leaf(&self.path, inc(curr_fence.upper), si);
                        }

                        let records = leaf_page.as_records();

                        let before = self.buff.len();
                        self.buff.extend(
                            records
                            .iter()
                            // Cheap key-range comparison first, so it can
                            // short-circuit `&&` before the costlier
                            // `matches` call below — see `is_visible`'s doc
                            // above for why this call is inlinable now, but
                            // it's still real per-record work (LCB cache
                            // lookup) that a narrower range scan (e.g. one
                            // order's order-lines, sharing a leaf with
                            // neighboring orders/keys outside that range)
                            // can skip outright for every out-of-range
                            // record instead of paying for it first. A
                            // full-table OLAP scan's range always contains
                            // every key in a leaf visited at all, so this is
                            // a no-op there either way.
                                .filter(|r| {
                                    self.range.contains(r.key())
                                        && r.version().matches(&mut is_visible)
                                })
                                .map(RecordPointResult::from_leaf),
                        );
                        crate::bat_test::record_leaf_scan(
                            tree as *const _ as usize,
                            records.len(),
                            self.buff.len() - before,
                        );

                        self.path.pop();
                        if reached_end {
                            self.path.clear();
                        } else {
                            self.range.lower = inc(curr_fence.upper);
                        }
                        if !self.buff.is_empty() || reached_end {
                            return;
                        }
                    }
                    _ => unreachable!(),
                }
            }
        })
    }

    /// Fallible zero-copy streaming scan. Returning `Err` stops immediately;
    /// `Drop` still releases snapshots owned by this iterator.
    pub fn try_for_each_ref<E>(
        mut self,
        mut visit: impl FnMut(Key, &Payload) -> Result<(), E>,
    ) -> Result<(), E> {
        let si = self.snapshot();
        let tree: &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> = self.isolated_snapshot.1;
        let inc = tree.cold.inc_key;
        let worker_id = self.worker_id;
        let mut visit_error = None;
        let full_key_range =
            self.range.lower == tree.cold.min_key && self.range.upper == tree.cold.max_key;

        tree.with_snapshot_cache_and_logs(|cache, commit_logs| {
            let mut is_visible = |stamp| {
                crate::bat_sync::visibility::is_visible(commit_logs, cache, worker_id, si, stamp)
            };

            while !self.path.is_empty() && self.range.lower <= self.range.upper {
                let (curr_fence, curr_block) = self.path.last().unwrap().clone();
                match curr_block.as_page_ref() {
                    PageType::IndexRef(internal_page) => {
                        let (keys, versions) = internal_page.keys_versions();
                        if let Some((pos, (_, fence))) =
                            versions.iter().zip(keys).enumerate().rev().find(
                                |(_, (version, fence))| {
                                    version.matched(si) && fence.contains(self.range.lower)
                                },
                            )
                        {
                            self.path.push((*fence, internal_page.get_pointer(pos)));
                        } else {
                            self.path.pop();
                            self.range.lower = inc(curr_fence.upper);
                        }
                    }
                    PageType::LeafRef(leaf_page) => {
                        let reached_end = curr_fence.upper >= self.range.upper
                            || curr_fence.upper == tree.cold.max_key;
                        if !reached_end {
                            prefetch_next_leaf(&self.path, inc(curr_fence.upper), si);
                        }

                        let records = leaf_page.as_records();
                        let mut matched = 0;
                        if full_key_range {
                            for record in records {
                                if record.version().matches(&mut is_visible) {
                                    matched += 1;
                                    if let Err(error) = visit(record.key(), record.payload()) {
                                        visit_error = Some(error);
                                        return;
                                    }
                                }
                            }
                        } else {
                            for record in records {
                                if self.range.contains(record.key())
                                    && record.version().matches(&mut is_visible)
                                {
                                    matched += 1;
                                    if let Err(error) = visit(record.key(), record.payload()) {
                                        visit_error = Some(error);
                                        return;
                                    }
                                }
                            }
                        }
                        crate::bat_test::record_leaf_scan(tree as *const _ as usize, records.len(), matched);

                        self.path.pop();
                        if reached_end {
                            self.path.clear();
                        } else {
                            self.range.lower = inc(curr_fence.upper);
                        }
                    }
                    _ => unreachable!(),
                }
            }
        });

        if let Some(error) = visit_error {
            return Err(error);
        }

        if self.register_reader_si {
            tree.on_release_reader_snapshot(si);
        }
        self.is_completed = true;
        Ok(())
    }

    /// Structural counterpart to `try_for_each_ref`: visits every leaf's
    /// fence and physical active/dead counts along this scan's range,
    /// without touching an individual record or running any visibility
    /// check at all — `active_dead_count()` is a raw per-page counter
    /// (`LeafPage::active_dead_count`), not filtered by any reader's MVCC
    /// visibility, so there's nothing to check here beyond the same
    /// version/fence-matched descent every other method in this file
    /// already does. Used by idle/proactive compaction
    /// (`bat_tree::idle_compaction`) to find garbage-heavy leaves cheaply:
    /// one packed-length-field read per leaf, nothing more.
    pub(crate) fn for_each_leaf_ratio(mut self, mut visit: impl FnMut(Interval<Key>, u32, u32)) {
        let si = self.snapshot();
        let tree: &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> = self.isolated_snapshot.1;
        let inc = tree.cold.inc_key;

        while !self.path.is_empty() && self.range.lower <= self.range.upper {
            let (curr_fence, curr_block) = self.path.last().unwrap().clone();
            match curr_block.as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let (keys, versions) = internal_page.keys_versions();
                    if let Some((pos, (_, fence))) =
                        versions.iter().zip(keys).enumerate().rev().find(
                            |(_, (version, fence))| {
                                version.matched(si) && fence.contains(self.range.lower)
                            },
                        )
                    {
                        self.path.push((*fence, internal_page.get_pointer(pos)));
                    } else {
                        self.path.pop();
                        self.range.lower = inc(curr_fence.upper);
                    }
                }
                PageType::LeafRef(leaf_page) => {
                    let (active, dead) = leaf_page.active_dead_count();
                    visit(curr_fence, active, dead);

                    self.path.pop();
                    if curr_fence.upper >= self.range.upper || curr_fence.upper == tree.cold.max_key {
                        self.path.clear();
                    } else {
                        self.range.lower = inc(curr_fence.upper);
                    }
                }
                _ => unreachable!(),
            }
        }

        if self.register_reader_si {
            tree.on_release_reader_snapshot(si);
        }
        self.is_completed = true;
    }

    /// Streams visible records without cloning payload handles or
    /// materializing a result vector. Intended for analytical folds/counts.
    pub fn for_each_ref(self, mut visit: impl FnMut(Key, &Payload)) {
        let result: Result<(), Infallible> = self.try_for_each_ref(|key, payload| {
            visit(key, payload);
            Ok(())
        });
        match result {
            Ok(()) => {}
            Err(never) => match never {},
        }
    }

    /// Zero-copy left fold over the visible records.
    pub fn fold_ref<Acc>(
        self,
        initial: Acc,
        mut fold: impl FnMut(Acc, Key, &Payload) -> Acc,
    ) -> Acc {
        let mut acc = Some(initial);
        self.for_each_ref(|key, payload| {
            acc = Some(fold(acc.take().unwrap(), key, payload));
        });
        acc.unwrap()
    }

    /// Counts visible records without constructing result objects.
    pub fn count_ref(self) -> usize {
        let mut count = 0usize;
        self.for_each_ref(|_, _| count += 1);
        count
    }

    /// The record with the smallest key remaining in this scan, or `None`
    /// once exhausted — without materializing (or even visiting) anything
    /// past the first leaf that has a match.
    ///
    /// Sound because sibling leaves partition the keyspace into
    /// non-overlapping, ascending ranges: once the first leaf with any live
    /// match in range is found, no leaf visited later could ever contain a
    /// smaller key, so comparing *within* that one leaf is enough — and it
    /// must be a real comparison, not just `next()`'s first result: leaf
    /// pages are append-ordered, never key-sorted (`LeafPage::
    /// push_uncommitted` always writes at the next free slot), so the
    /// first live match `refill` happens to buffer is not necessarily the
    /// smallest one in that same leaf.
    ///
    /// Takes `self` by value: this is a "get the one thing I need, then
    /// I'm done with this scan" query, not a general-purpose iterator
    /// adapter — callers that also want to keep iterating afterward should
    /// use plain `next()`/`Iterator` methods instead.
    pub fn min_by_key(mut self) -> Option<RecordPointResult<Key, Payload>> {
        self.refill();
        let min_index = self
            .buff
            .iter()
            .enumerate()
            .min_by_key(|(_, r)| r.key)
            .map(|(index, _)| index)?;
        self.buff.remove(min_index)
    }

    /// Releases this iterator's own reader-snapshot registration (if any)
    /// and marks it complete, without running `refill`'s normal drain loop —
    /// used by every `*_parallel` method below once it has gone the
    /// pool-dispatch route instead of `refill`'s usual walk, so `Drop` (which
    /// only releases when `!is_completed`) doesn't release a second time.
    fn finish_after_dispatch(&mut self) {
        if self.register_reader_si {
            self.bat_tree().on_release_reader_snapshot(self.snapshot());
        }
        self.is_completed = true;
    }
}

/// The `*_parallel` counterparts of this module's plain terminal methods
/// above — same job, but able to fan a large-enough `range` out across a
/// caller-supplied [`ScanWorkerPool`] instead of always walking it on the
/// calling thread alone. Kept in their own `impl` block, bounded by the
/// extra `RangeSplit`/`Send` requirements dispatching across threads
/// actually needs (see `ScanWorkerPool::dispatch_evenly`'s doc for why that
/// bound is opt-in rather than on every `RangeQueryIter` method), so a
/// `Key`/`Payload` pair that never needs parallel scanning doesn't have to
/// satisfy it just because this module defines these methods somewhere.
///
/// This is deliberately *the* place that decision lives: a caller (a
/// `DbTransaction`/`TpccTxn` range method, a workload's own scan helper,
/// ...) just hands over whatever pool it has — `None` if it doesn't have
/// one, or doesn't want to use it right now — and every "is this range even
/// worth splitting", "does the pool have a fair share to offer", "which
/// worker id should a pool-thread job use", and "fall back to plain
/// sequential" decision happens right here, once, instead of each caller
/// re-implementing that dance by hand (which is exactly what every one of
/// this module's callers had to do before these methods existed — see
/// `bat_bench::parallel_scan::q1_parallel`/`bat_bench::ycsb_txn::
/// scan_parallel`'s own history for two hand-rolled versions of it).
///
/// `pool: None` always takes the plain sequential path — a real, explicit
/// "no parallel workers" mode for a caller that wants one on purpose (e.g. a
/// test comparing sequential vs. pooled timing/output, or a caller that
/// knows this particular call is too latency-sensitive to risk dispatch
/// overhead), not just an incidental side effect of not having a pool.
///
/// A query that only wants the smallest key in `range`
/// ([`RangeQueryIter::min_by_key`]) has no `_parallel` counterpart here on
/// purpose: it only ever needs to look at the first leaf with a match, so
/// splitting the rest of the range across a pool would do strictly more
/// work for the same answer, never less.
impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + RangeSplit + 'static,
    Payload: Display + Clone + Default + Sync + Send + 'static,
> RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// `Iterator::collect`'s parallel counterpart — eagerly materializes
    /// every visible match in `self.range`, splitting across `pool`'s fair
    /// share when it's large enough to be worth it (see this `impl` block's
    /// doc), falling back to plain `collect()` otherwise.
    pub fn collect_parallel(
        mut self,
        pool: Option<&ScanWorkerPool<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    ) -> Vec<RecordPointResult<Key, Payload>> {
        let ranges = pool.and_then(|pool| pool.evenly_split_ranges(self.range));
        let Some((pool, ranges)) = pool.zip(ranges) else {
            return self.collect();
        };
        let version = self.snapshot();
        let parts = pool.try_dispatch(ranges, move |tree, sub_range| {
            RangeQueryIter::new(tree, version, sub_range, false, READ_ONLY_SCAN_WORKER_ID).collect::<Vec<_>>()
        });
        self.finish_after_dispatch();
        parts.into_iter().flatten().collect()
    }

    /// `count_ref`'s parallel counterpart — see `collect_parallel`'s doc.
    pub fn count_ref_parallel(
        mut self,
        pool: Option<&ScanWorkerPool<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    ) -> usize {
        let ranges = pool.and_then(|pool| pool.evenly_split_ranges(self.range));
        let Some((pool, ranges)) = pool.zip(ranges) else {
            return self.count_ref();
        };
        let version = self.snapshot();
        let parts = pool.try_dispatch(ranges, move |tree, sub_range| {
            RangeQueryIter::new(tree, version, sub_range, false, READ_ONLY_SCAN_WORKER_ID).count_ref()
        });
        self.finish_after_dispatch();
        parts.into_iter().sum()
    }

    /// `for_each_ref`'s parallel counterpart. `visit` runs concurrently on
    /// whichever pool workers pick up a sub-range's job (or, on the
    /// sequential fallback, on the calling thread alone), so it must be a
    /// `Fn`, not a `FnMut` — an accumulating visitor needs its own internal
    /// synchronization (an atomic, a mutex, ...) rather than a bare captured
    /// `&mut`, exactly as any other closure shared across threads would.
    pub fn for_each_ref_parallel(
        mut self,
        pool: Option<&ScanWorkerPool<FAN_OUT, NUM_RECORDS, Key, Payload>>,
        visit: impl Fn(Key, &Payload) + Send + Sync + 'static,
    ) {
        let ranges = pool.and_then(|pool| pool.evenly_split_ranges(self.range));
        let Some((pool, ranges)) = pool.zip(ranges) else {
            return self.for_each_ref(visit);
        };
        let version = self.snapshot();
        pool.try_dispatch(ranges, move |tree, sub_range| {
            RangeQueryIter::new(tree, version, sub_range, false, READ_ONLY_SCAN_WORKER_ID)
                .for_each_ref(&visit);
        });
        self.finish_after_dispatch();
    }

    /// `fold_ref`'s parallel counterpart. Since each sub-range needs its own
    /// independent accumulator to fold into (there's no shared mutable state
    /// to fold through across threads), this takes `init` — an accumulator
    /// *factory*, called once per dispatched sub-range — instead of a single
    /// initial value, plus `merge` to combine the resulting per-sub-range
    /// accumulators back into one, in `pool.try_dispatch`'s (arbitrary,
    /// worker-scheduling-dependent) result order — so `merge` must be
    /// order-independent (e.g. `+`, not `-`) for the overall result to be
    /// deterministic.
    pub fn fold_ref_parallel<Acc: Send + 'static>(
        mut self,
        pool: Option<&ScanWorkerPool<FAN_OUT, NUM_RECORDS, Key, Payload>>,
        init: impl Fn() -> Acc + Send + Sync + 'static,
        fold: impl Fn(Acc, Key, &Payload) -> Acc + Send + Sync + 'static,
        merge: impl Fn(Acc, Acc) -> Acc,
    ) -> Acc {
        let ranges = pool.and_then(|pool| pool.evenly_split_ranges(self.range));
        let Some((pool, ranges)) = pool.zip(ranges) else {
            return self.fold_ref(init(), fold);
        };
        let version = self.snapshot();
        let parts = pool.try_dispatch(ranges, move |tree, sub_range| {
            RangeQueryIter::new(tree, version, sub_range, false, READ_ONLY_SCAN_WORKER_ID)
                .fold_ref(init(), &fold)
        });
        self.finish_after_dispatch();
        parts.into_iter().reduce(merge).expect(
            "try_dispatch returns one result per dispatched range, and evenly_split_ranges \
             never returns an empty Vec (fair_query_fanout requires a fanout of at least 2)",
        )
    }
}
