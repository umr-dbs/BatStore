use crate::mv_block::block::BlockGuard;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_page_model::node::PageType;
use crate::mv_page_model::time_matcher::TimeMatcher;
use crate::mv_page_model::{Attempts, BlockRef};
use crate::mv_query::interval::Interval;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_record_model::tx_stamp::WorkerId;
use crate::mv_record_model::version_info::Version;
use crate::mv_root::index_root::RootIndex;
use crate::mv_sync::smart_cell::sched_yield;
use crate::mv_tree::mvbt::MVBTSt;
use itertools::Itertools;
use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn retrieve_root_number_for(&self, lookup_version: Version) -> (usize, usize) {
        match self.root {
            RootIndex::FrugalList(ref fg) => {
                let roots = fg;
                let roots_all
                    = roots.iter().collect_vec();

                let root_count = roots_all.len();

                (roots_all.iter()
                     .enumerate()
                     .rev()
                     .find_map(|(pos, r)|
                         (r.insert_version <= lookup_version).then(|| pos + 1))
                     .unwrap(), root_count)
            }
            RootIndex::LinkedList(ref ll) => {
                let roots = ll.clone();
                let root_count = roots.len();

                (roots.iter()
                    .enumerate()
                    .rev()
                    .find_map(|(pos, r)| (r.version <= lookup_version)
                        .then(|| pos + 1)
                        .or(Some(1)))
                     .unwrap(), root_count)
            }
            _ => (0,0)
        }
    }

    #[inline(always)]
    pub fn retrieve_root_for(&self, lookup_version: Version)
                                    -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        self.root
            .root_for(lookup_version)
            .block
    }

    /// Descends from `root` to the leaf that should hold `key` at
    /// `lookup_version`, mirroring `traversal_write_internal_olc`'s
    /// "no matching child -> restart from the root" recovery
    /// (`mv_query::olc_query`).
    ///
    /// Child *pages* themselves are effectively append-only once linked into
    /// the tree (a split/merge always builds an entirely new page for any
    /// new content — see `MVBTSt::split`/`merge` — never mutates an existing
    /// child's own records in place), so simply holding a `BlockRef` to a
    /// child is not, by itself, the problem. The actual race is one level
    /// up, in the *parent*: `on_overflow_node`/`on_underflow_node` extend an
    /// existing parent page in place (`push_uncommitted` writes the new
    /// entry, then `commit_delta` bumps `len` to publish it), and every step
    /// of that — the length field, and the getters that read it
    /// (`sum_len`/`active_len`/`keys_versions`/...) — uses `Relaxed`
    /// ordering with no acquire/release fence pairing them (several
    /// `fence(Release)`/`fence(Acquire)` call sites are present in the
    /// source but commented out). Without that fence, a reader on another
    /// core can observe the bumped `len` before it's guaranteed to observe
    /// the entry data the writer wrote just before bumping it — i.e. an
    /// unsynchronized, not just stale, view of the parent. This is a
    /// genuine, still-open gap independent of GC: it just happens to be
    /// silent without GC (the worst it can do there is follow an old
    /// pointer that's still a fully valid, if superseded, page — see above),
    /// and loud with GC (that same old child may by then be a block GC has
    /// already reclaimed and reset for something else, so this exact loop
    /// finds it unexpectedly empty).
    ///
    /// Confirmed empirically, not just by argument: with the `register_dead`
    /// version fix (`mv_tree::smo`, correcting *which* version a superseded
    /// entry's old child dies at) applied but this retry removed, the
    /// under-GC crash this whole investigation started from still
    /// reproduced in 3/3 heavily concurrent runs — so that fix alone does
    /// not close the gap. Restarting from the root on a miss does: a fresh
    /// read of the actually-current tree either finds the right entry
    /// immediately, or (if it races again) just retries — the same
    /// recovery already trusted for the identical class of race on the
    /// write path, and re-verified clean across repeated heavy-concurrency
    /// stress runs with GC on.
    /// Takes `root` by reference, not by value: `SmartCell::borrow_read`
    /// hands back a `SmartGuard::Reader(&'a SmartCell<E>, ..)` — a raw
    /// reference to wherever the `SmartCell` it was called on physically
    /// lives, `mem::transmute`d to claim a `'static` lifetime, not an
    /// owned Arc clone. That's sound when called on `internal_page
    /// .get_pointer(pos)` below (a reference into the *parent* page's own
    /// long-lived, tree-owned array), but calling it on `root.clone()` — a
    /// bare temporary — would return a guard referencing this function's
    /// *own* stack frame, which is gone the moment it returns. That's
    /// invisible whenever the tree has real internal pages (this loop
    /// runs at least once, so `curr` ends up referencing the last visited
    /// page's array instead), but a tree that never grows past
    /// height 1 — the *root itself* is the leaf, e.g. any table whose live
    /// key cardinality never exceeds one leaf's capacity, such as TPC-C's
    /// per-table `Warehouse`/`District` trees at standard scale — takes
    /// zero loop iterations, so `curr` is exactly that dangling
    /// caller-frame reference: a real, silent, single-threaded UB bug
    /// (confirmed via gdb: a null-pointer dereference reading stack bytes
    /// the caller's own subsequent calls had since overwritten), not a
    /// concurrency race. Borrowing `root` from the *caller's* frame
    /// instead — which outlives this whole call and everything the
    /// returned guard is used for — fixes it for that case too.
    #[inline]
    fn traverse_read_key<'a>(
        root: &'a BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        key: Key,
        lookup_version: Version)
        -> BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let mut attempts: Attempts = 0;

        'restart: loop {
            let mut curr = root.borrow_read();

            while let PageType::IndexRef(internal_page) = curr.as_page_ref()
            {
                let (keys_page, versions_page) = internal_page
                    .keys_versions();

                curr = match versions_page
                    .iter()
                    .zip(keys_page)
                    .enumerate()
                    .rfind(|(_, (v, range))|
                        v.matched(lookup_version) && range.contains(key))
                    .map(|(pos, _)| internal_page.get_pointer(pos).borrow_read())
                {
                    Some(c) => c,
                    None => {
                        attempts += 1;
                        sched_yield(attempts);
                        continue 'restart;
                    }
                }
            }

            break curr;
        }
    }

    fn traverse_read_key_range(
        mut curr: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        lookup_range: &Interval<Key>,
        lookup_version: Version)
        -> Vec<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>
    {
        let mut blocks
            = VecDeque::new();

        blocks.push_back(curr);

        let mut leafs
            = vec![];

        while !blocks.is_empty() {
            curr = blocks.pop_front().unwrap();

            match curr.as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    let start_pos_si = versions_page.len() -
                        versions_page.binary_search_by(|v| v.into_cmp().cmp(&lookup_version))
                            .unwrap_or_else(|pos| pos);

                    versions_page
                        .iter()
                        .enumerate()
                        .zip(keys_page.iter())
                        .rev()
                        .skip(start_pos_si)
                        .filter(|((.., v), range)| //v.matched(lookup_version) &&
                            v.matched(lookup_version) && lookup_range.overlap(range))
                        .unique_by(|(.., range)| range.lower())
                        .unique_by(|(.., range)| range.upper())
                        .for_each(|((pos, ..), ..)|
                            blocks.push_back(internal_page.get_pointer(pos).clone()));
                }
                _ => leafs.push(curr)
            }
        }

        leafs
    }

    #[inline]
    pub(crate) fn key_point_read_from_root<'a>(
        &self,
        root: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        key: Key,
        reader_worker: WorkerId,
        reader_ts_start: Version)
        -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let records
            = Self::traverse_read_key(&root, key, reader_ts_start);

        self.with_visibility_checker(reader_worker, reader_ts_start, |is_visible| {
            match records
                .as_records()
                .iter()
                .rev()
                .skip_while(|r| r.version.insert_stamp.ts_start() > reader_ts_start)
                .find(|r|
                    r.key() == key && r.version().matches(is_visible))
            {
                None => CRUDOperationResult::MatchedRecords(Vec::with_capacity(0)),
                Some(result) =>
                    CRUDOperationResult::MatchedRecords(vec![RecordPointResult::from(result)])
            }
        })
    }

    pub(crate) fn key_range_read_from_root<'a>(
        &self,
        root: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        lookup_range: Interval<Key>,
        reader_worker: WorkerId,
        reader_ts_start: Version)
        -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let blocks = Self::traverse_read_key_range(
            root,
            &lookup_range,
            reader_ts_start);

        CRUDOperationResult::MatchedRecords(blocks
            .into_iter()
            .map(|leaf| {
                let records = leaf
                    .deref()
                    .as_records();

                let start_pos_si = records.len() -
                    records.binary_search_by(|r|
                        r.version.insert_stamp.ts_start().cmp(&reader_ts_start)
                    ).unwrap_or_else(|pos| pos);

               self.with_visibility_checker(reader_worker, reader_ts_start, |is_visible| {
                   records
                       .iter()
                       .rev()
                       .skip(start_pos_si)
                       .filter(|r|
                           r.version().matches(is_visible) &&
                               lookup_range.contains(r.key()))
                       // .sorted_by_key(|r| r.key())
                       .map(RecordPointResult::from)
                       .collect::<Vec<_>>()
               })
            })
            // .filter(|set| !set.is_empty())
            // .sorted_by_key(|set|
            //     unsafe { set.get_unchecked(0).key })
            .flatten()
            .collect())
    }

    // #[inline]
    // fn retrieve_root_write(&self) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
    //     let height
    //         = self.root.height();
    //
    //     let master_guard
    //         = self.root.borrow_read();
    //
    //     let root_block
    //         = master_guard.block();
    //
    //     let root_guard
    //         = root_block.borrow_read();
    //
    //     match master_guard.unsafe_degree_root() {
    //         BlockUnsafeDegree::Overflow =>
    //             self.split_root(master_guard, root_guard, height),
    //         BlockUnsafeDegree::ActiveUnderflow =>
    //             self.merge_root(master_guard, root_guard, height)
    //             .unwrap(),
    //         _ => root_guard,
    //     }
    // }

    // #[inline]
    // pub(crate) fn traversal_write(&self, key: Key)
    //                               -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>
    // {
    //     let (mut curr_guard) = self.retrieve_root_write();
    //
    //     loop {
    //         match curr_guard.deref().as_page_ref() {
    //             PageType::IndexRef(internal_page) => {
    //                 let keys_page = internal_page
    //                     .keys();
    //
    //                 let index = keys_page
    //                     .iter()
    //                     .enumerate()
    //                     // .rev()
    //                     .rfind(|(.., range)| range.contains(key))
    //                     .map(|(pos, ..)| pos)
    //                     .unwrap();
    //
    //                 let next_curr_block = internal_page
    //                     .get_pointer(index)
    //                     .clone();
    //
    //                 let next_curr_guard
    //                     = next_curr_block.borrow_free();
    //
    //                 match next_curr_guard.deref().unsafe_degree() {
    //                     BlockUnsafeDegree::Overflow =>
    //                         curr_guard = self.on_overflow_node(curr_guard, next_curr_guard, index),
    //                     BlockUnsafeDegree::ActiveUnderflow =>
    //                         curr_guard = self.on_underflow_node(curr_guard, next_curr_guard, index)
    //                             .unwrap(),
    //                     BlockUnsafeDegree::Ok => curr_guard = next_curr_guard
    //                 }
    //             }
    //             _ => return curr_guard
    //         }
    //     }
    // }
}