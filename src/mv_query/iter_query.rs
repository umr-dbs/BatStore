use std::collections::VecDeque;
use std::convert::Infallible;
use std::fmt::Display;
use std::hash::Hash;

use crate::mv_page_model::BlockRef;

use crate::mv_page_model::node::PageType;
use crate::mv_page_model::time_matcher::TimeMatcher;
use crate::mv_query::SnapShot;
use crate::mv_query::interval::Interval;
use crate::mv_query::snapshot::ReaderIsolatedSnapShot;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_record_model::tx_stamp::WorkerId;
use crate::mv_record_model::version_info::Version;
use crate::mv_tree::mvbt::MVBTSt;

pub struct RangeQueryIter<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> {
    pub(crate) isolated_snapshot: ReaderIsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub(crate) range: Interval<Key>,
    path: Vec<(Interval<Key>, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
    buff: VecDeque<RecordPointResult<Key, Payload>>,
    is_completed: bool,
    register_reader_si: bool,
    worker_id: WorkerId
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> Drop for RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn drop(&mut self) { // ensure snapshot is released even if user didn't consume all data
        if !self.is_completed && self.register_reader_si {
            self.mv_tree()
                .on_release_reader_snapshot(self.snapshot().into())
        }
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub fn new(tree: &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
               version: Version,
               range: Interval<Key>,
               register_reader_si: bool,
               worker_id: WorkerId) -> Self
    {
        if register_reader_si {
            tree.on_acquire_reader_snapshot(version);
        }

        Self {
            isolated_snapshot: ReaderIsolatedSnapShot(version, tree),
            range,
            path: vec![(Interval::new(tree.min_key, tree.max_key),
                        tree.retrieve_root_for(version))],
            buff: VecDeque::new(),
            is_completed: false,
            register_reader_si,
            worker_id
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

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> Iterator for RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    type Item = RecordPointResult<Key, Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        self.refill();
        self.buff.pop_front()
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
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

        let si
            = self.snapshot();

        // A direct copy of the stored `&'a MVBTSt` (references are `Copy`),
        // not a call through `self.mv_tree()` — the latter's elided return
        // lifetime ties to `&self`, which would keep `self` borrowed for as
        // long as `tree` (or anything capturing it, like `is_visible` below)
        // is alive, conflicting with the `&mut self.buff`/`self.path` calls
        // later in this same loop.
        let tree: &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
            = self.isolated_snapshot.1;

        let inc
            = tree.inc_key;

        let worker_id
            = self.worker_id;

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
            let mut is_visible = |stamp| crate::mv_sync::visibility::is_visible(
                commit_logs, cache, worker_id, si, stamp);

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
                    return
                }

                let (curr_fence, curr_block) = self.path.last().unwrap().clone();

                match curr_block.as_page_ref() {
                    PageType::IndexRef(internal_page) => {
                        let (keys, versions) = internal_page.keys_versions();
                        if let Some((pos, (_, fence))) = versions.iter().zip(keys).enumerate().rev()
                            .find(|(_, (version, fence))|
                                version.matched(si) && fence.contains(self.range.lower))
                        {
                            self.path.push((*fence, internal_page.get_pointer(pos)));
                        } else {
                            self.path.pop();
                            self.range.lower = inc(curr_fence.upper);
                        }
                    }
                    PageType::LeafRef(leaf_page) => {
                        let records = leaf_page
                            .as_records();

                        let before = self.buff.len();
                        self.buff.extend(records
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
                            .filter(|r|
                                self.range.contains(r.key()) && r.version().matches(&mut is_visible))
                            .map(RecordPointResult::from));
                        crate::mv_test::record_leaf_scan(records.len(), self.buff.len() - before);

                        self.path.pop();
                        let reached_end = curr_fence.upper >= self.range.upper
                            || curr_fence.upper == tree.max_key;
                        if reached_end {
                            self.path.clear();
                        } else {
                            self.range.lower = inc(curr_fence.upper);
                        }
                        if !self.buff.is_empty() || reached_end {
                            return
                        }
                    }
                    _ => unreachable!()
                }
            }
        })
    }

    /// Fallible zero-copy streaming scan. Returning `Err` stops immediately;
    /// `Drop` still releases snapshots owned by this iterator.
    pub fn try_for_each_ref<E>(mut self, mut visit: impl FnMut(Key, &Payload) -> Result<(), E>)
        -> Result<(), E>
    {
        let si = self.snapshot();
        let tree: &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> = self.isolated_snapshot.1;
        let inc = tree.inc_key;
        let worker_id = self.worker_id;
        let mut visit_error = None;

        tree.with_snapshot_cache_and_logs(|cache, commit_logs| {
            let mut is_visible = |stamp| crate::mv_sync::visibility::is_visible(
                commit_logs, cache, worker_id, si, stamp);

            while !self.path.is_empty() && self.range.lower <= self.range.upper {
                let (curr_fence, curr_block) = self.path.last().unwrap().clone();
                match curr_block.as_page_ref() {
                    PageType::IndexRef(internal_page) => {
                        let (keys, versions) = internal_page.keys_versions();
                        if let Some((pos, (_, fence))) = versions.iter().zip(keys).enumerate().rev()
                            .find(|(_, (version, fence))|
                                version.matched(si) && fence.contains(self.range.lower))
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
                        for record in records {
                            if self.range.contains(record.key()) && record.version().matches(&mut is_visible) {
                                matched += 1;
                                if let Err(error) = visit(record.key(), record.payload()) {
                                    visit_error = Some(error);
                                    return;
                                }
                            }
                        }
                        crate::mv_test::record_leaf_scan(records.len(), matched);
                        self.path.pop();
                        if curr_fence.upper >= self.range.upper
                            || curr_fence.upper == tree.max_key
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
    pub fn fold_ref<Acc>(self, initial: Acc, mut fold: impl FnMut(Acc, Key, &Payload) -> Acc) -> Acc {
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
        let min_index = self.buff.iter().enumerate()
            .min_by_key(|(_, r)| r.key)
            .map(|(index, _)| index)?;
        self.buff.remove(min_index)
    }
}
