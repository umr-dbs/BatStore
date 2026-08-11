use std::collections::VecDeque;
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

        loop {
            if self.path.is_empty() || self.range.lower > self.range.upper {
                // Only release if *this iterator* is the one that registered
                // the snapshot (`register_reader_si`) — mirrors `Drop`'s own
                // guard just below. Without it, a `Transaction`-owned range
                // scan (`Transaction::range`, `register_reader_si: false`,
                // since the `Transaction` itself registered `ts_start` at
                // `begin()` and releases it at `commit()`/drop) would have
                // its *first* fully-drained range scan release the
                // transaction's snapshot registration early — leaving every
                // later read in the same transaction (any further
                // `tx.point`/`tx.range` call) running with no GC protection
                // at all, since the registration is already gone.
                if self.register_reader_si {
                    tree.on_release_reader_snapshot(si);
                }

                self.is_completed = true;
                return
            }

            let (curr_fence, curr_block)
                = self.path.last().unwrap().clone();

            match curr_block.as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    match versions_page
                        .iter()
                        .zip(keys_page.iter())
                        .enumerate()
                        .rev()
                        .find_map(|(pos, (v, range))|
                            if range.contains(self.range.lower) && v.matched(si){
                                Some((*range, internal_page.get_pointer(pos)))
                            } else {
                                None
                            })
                    {
                        Some((next_keys, next_block)) =>
                            self.path.push((next_keys, next_block)),
                        _ => {
                            self.path.pop();
                            self.range.lower = inc(curr_fence.upper);
                        }
                    }
                }
                PageType::LeafRef(leaf_page) => {
                    let records = leaf_page
                        .as_records();

                    let before = self.buff.len();
                    tree.with_visibility_checker(self.worker_id, si, |is_visible| {
                        self.buff.extend(records
                            .iter()
                            .filter(|r|
                                r.version().matches(is_visible) && self.range.contains(r.key()))
                            .map(RecordPointResult::from));
                    });
                    crate::mv_test::record_leaf_scan(records.len(), self.buff.len() - before);

                    self.path.pop();

                    self.range.lower = inc(curr_fence.upper);
                    if !self.buff.is_empty() || self.range.lower == tree.max_key {
                        return
                    }
                }
                _ => unreachable!()
            }
        }
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