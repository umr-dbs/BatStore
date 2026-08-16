use std::collections::VecDeque;
use std::convert::Infallible;
use std::fmt::Display;
use std::hash::Hash;

use crate::mv_page_model::BlockRef;

use crate::mv_page_model::node::{ColdLink, PageType};
use crate::mv_page_model::time_matcher::TimeMatcher;
use crate::mv_query::SnapShot;
use crate::mv_query::interval::Interval;
use crate::mv_query::snapshot::ReaderIsolatedSnapShot;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::Version;
use crate::mv_tree::mvbt::MVBTSt;

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
            self.mv_tree()
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
    pub const fn mv_tree(&self) -> &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.si().mv_tree()
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
    /// Continues a leaf's range scan down its cold chain (see `ColdLink`'s
    /// doc in `mv_page_model::node` for the whole design): a reader whose
    /// snapshot needs a version already offloaded to a cold page won't
    /// find it in the hot leaf's own records at all, so a plain scan of
    /// `leaf_page.as_records()` alone would silently drop that key from
    /// the range result. Calls `f` with every additional visible, in-range
    /// match found walking the chain newest-to-oldest, stopping early the
    /// moment `f` returns `false` (mirrors `try_for_each_ref`'s own
    /// stop-on-error shape; `RangeQueryIter`'s own buffered scan below
    /// just always returns `true`, i.e. never stops early). A key never
    /// contributes from both hot and cold to the same reader: MVCC
    /// guarantees exactly one version in a key's whole chain is visible to
    /// a given snapshot, so whichever page (hot or cold) currently holds
    /// that specific version is the only one whose filter passes for it —
    /// no separate dedup needed here.
    ///
    /// Safe to call unconditionally whenever `link` isn't `ColdLink::none()`
    /// with no extra OLC validation beyond the traversal that already got
    /// to this leaf — see `MVBTSt::scan_cold_chain_for_key`'s doc
    /// (`mv_query::query`) for why: a cold chain is fixed for a leaf's
    /// whole lifetime, set once before that leaf is ever linked into the
    /// tree, and a cold page is likewise never mutated after construction.
    /// `hot_keys` — every key this same leaf visit already produced a match
    /// for from the *hot* page — is consulted before `is_visible`/`matches`
    /// for each cold candidate: a key with a hot match already has its
    /// current answer, so a cold entry for that same key (an older, formerly
    /// superseded version whose own `matches` should ordinarily be mutually
    /// exclusive with the hot version's, but isn't guaranteed to be under
    /// every hot/cold classification path — see `mv_tree::smo`'s
    /// `grouped_hot_cold_records`) must never be surfaced alongside it, or a
    /// scan double-counts that key. Skipping by key here is strictly
    /// defense in depth over fixing the classification itself, but it's the
    /// one place that can guarantee "one visible version per key" for every
    /// caller regardless of how the hot/cold split was decided.
    pub(crate) fn walk_cold_chain_for_range<F, V>(
        mut link: ColdLink<FAN_OUT, NUM_RECORDS, Key, Payload>,
        range: Interval<Key>,
        is_visible: &mut F,
        hot_keys: &std::collections::HashSet<Key>,
        mut f: V,
    ) where
        F: FnMut(TxStamp) -> bool,
        V: FnMut(crate::mv_page_model::leaf_page::LeafRecordRef<'_, Key, Payload>) -> bool,
    {
        while !link.is_none() {
            let cold_guard = link.cold().borrow_read();
            let cold_leaf = cold_guard.as_leaf_page_ref();
            for r in cold_leaf.as_records() {
                if range.contains(r.key())
                    && !hot_keys.contains(&r.key())
                    && r.version().matches(is_visible)
                {
                    if !f(r) {
                        return;
                    }
                }
            }
            link = *cold_guard.cold_link();
        }
    }

    /// `walk_cold_chain_for_range`, collecting into `out` the same way the
    /// hot leaf's own matches get collected into `self.buff` below.
    fn extend_from_cold_chain<F: FnMut(TxStamp) -> bool>(
        link: ColdLink<FAN_OUT, NUM_RECORDS, Key, Payload>,
        range: Interval<Key>,
        is_visible: &mut F,
        hot_keys: &std::collections::HashSet<Key>,
        out: &mut VecDeque<RecordPointResult<Key, Payload>>,
    ) {
        Self::walk_cold_chain_for_range(link, range, is_visible, hot_keys, |r| {
            out.push_back(RecordPointResult::from_leaf(r));
            true
        });
    }

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
        // not a call through `self.mv_tree()` — the latter's elided return
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
                crate::mv_sync::visibility::is_visible(commit_logs, cache, worker_id, si, stamp)
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
                        crate::mv_test::record_leaf_scan(records.len(), self.buff.len() - before);

                        let cold_link = *curr_block.cold_link();
                        if !cold_link.is_none() {
                            let hot_keys: std::collections::HashSet<Key> =
                                self.buff.iter().skip(before).map(|r| r.key).collect();
                            Self::extend_from_cold_chain(
                                cold_link,
                                self.range,
                                &mut is_visible,
                                &hot_keys,
                                &mut self.buff,
                            );
                        }

                        self.path.pop();
                        let reached_end = curr_fence.upper >= self.range.upper
                            || curr_fence.upper == tree.cold.max_key;
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
                crate::mv_sync::visibility::is_visible(commit_logs, cache, worker_id, si, stamp)
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
                        let records = leaf_page.as_records();
                        let mut matched = 0;
                        let mut hot_keys: std::collections::HashSet<Key> =
                            std::collections::HashSet::new();
                        if full_key_range {
                            for record in records {
                                if record.version().matches(&mut is_visible) {
                                    matched += 1;
                                    hot_keys.insert(record.key());
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
                                    hot_keys.insert(record.key());
                                    if let Err(error) = visit(record.key(), record.payload()) {
                                        visit_error = Some(error);
                                        return;
                                    }
                                }
                            }
                        }
                        crate::mv_test::record_leaf_scan(records.len(), matched);

                        let cold_link = *curr_block.cold_link();
                        if !cold_link.is_none() {
                            Self::walk_cold_chain_for_range(
                                cold_link,
                                self.range,
                                &mut is_visible,
                                &hot_keys,
                                |r| match visit(r.key(), r.payload()) {
                                    Ok(()) => true,
                                    Err(error) => {
                                        visit_error = Some(error);
                                        false
                                    }
                                },
                            );
                            if visit_error.is_some() {
                                return;
                            }
                        }

                        self.path.pop();
                        if curr_fence.upper >= self.range.upper
                            || curr_fence.upper == tree.cold.max_key
                        {
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
}
