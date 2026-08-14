use crate::mv_block::block::BlockGuard;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_page_model::leaf_page::{LeafPage, LeafRecordRef};
use crate::mv_page_model::node::PageType;
use crate::mv_page_model::time_matcher::TimeMatcher;
use crate::mv_page_model::{Attempts, BlockRef};
use crate::mv_query::interval::Interval;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
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
    /// child is not, by itself, the problem. The actual race used to be one
    /// level up, in the *parent*: `InternalPage::mark_version_obsolete` used
    /// to flag a superseded child in place — an in-place mutation of a slot
    /// this loop's version check below also reads with no lock at all — and
    /// getting that flag's `Release`/`Acquire` pairing wrong (it used to be
    /// bare `Relaxed` on both sides) let a reader on another core observe a
    /// child as still-active for an unbounded time after it had already been
    /// superseded (and, under GC, after the block behind it had already been
    /// reclaimed and reset for something else) — silent without GC, loud
    /// with it. This was confirmed as the actual mechanism behind the crash
    /// this investigation started from: a reader whose own registered
    /// snapshot was *newer* than the block's death version (so it never
    /// legitimately needed the block at all, by MVCC visibility rules) still
    /// ended up dereferencing it, because the staleness was about memory
    /// visibility, not snapshot age. `mark_version_obsolete` is gone now
    /// (see `InternalPage::live_mask`'s doc): liveness no longer needs a
    /// per-slot flag at all, so there's no longer a write to race against —
    /// this loop's `matched(lookup_version)` check below only ever reads a
    /// slot's birth version, written once before publication and never
    /// touched again.
    ///
    /// The `'restart` loop below is kept as defense in depth, not as the
    /// primary fix: it mirrors the identical "no matching child -> restart
    /// from the root" recovery already trusted on the write path
    /// (`traversal_write_internal_olc`) for other, unrelated causes of a
    /// transient miss (e.g. racing a concurrent split still mid-flight).
    /// `root` is taken by reference purely to avoid an unnecessary refcount
    /// bump at the call boundary — it is no longer load-bearing for
    /// soundness. `SmartCell::borrow_read` used to hand back a
    /// `SmartGuard::Reader(&'a SmartCell<E>, ..)`, a raw reference
    /// `mem::transmute`d to claim a `'static` lifetime; that was unsound for
    /// a tree that never grows past height 1 (root itself is the leaf —
    /// e.g. TPC-C's per-table `Warehouse`/`District` trees at standard
    /// scale), where this loop takes zero iterations and `curr` ended up a
    /// dangling reference into this function's own stack frame (confirmed
    /// via gdb: a null-pointer dereference reading stack bytes the caller's
    /// own subsequent calls had since overwritten). `SmartGuard::Reader` now
    /// owns a cloned `SmartCell` (a real `Arc` clone, see its doc) instead of
    /// borrowing one, so it can never dangle regardless of tree height or
    /// where the `SmartCell` it was produced from lives — this class of bug
    /// is now categorically ruled out, not just avoided by call-site care.
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

    /// Scans one leaf's own records (hot leaf or one cold-chain hop) for
    /// `key`, returning the first (physically last, i.e. newest-to-oldest)
    /// record whose version is visible to `is_visible`. Not SIMD-optimized
    /// like `key_point_read_from_root`'s own hot-leaf scan below on
    /// purpose: this only ever runs for a cold-chain hop today (see that
    /// method's fallback), which by design should be rare — only a reader
    /// whose snapshot needs an already-superseded version, one that a hot
    /// leaf's hot/cold split has since moved out of the hot page, reaches
    /// it at all. Generic over `F`, not `dyn FnMut`, matching
    /// `VersionInfo::matches`'s own signature (see that method's doc for
    /// why: an inlinable concrete closure, not a vtable call, on a path
    /// this hot).
    #[inline]
    pub(crate) fn scan_leaf_for_key<'a, F: FnMut(TxStamp) -> bool>(
        leaf: &'a LeafPage<NUM_RECORDS, Key, Payload>,
        key: Key,
        is_visible: &mut F,
    ) -> Option<LeafRecordRef<'a, Key, Payload>> {
        leaf.keys()
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, k)| **k == key)
            .map(|(i, _)| leaf.record(i))
            .find(|r| r.version().matches(is_visible))
    }

    /// Continues a hot-leaf point-read miss down the cold chain (see
    /// `ColdLink`'s doc for the whole design): a reader whose snapshot
    /// needs a version already offloaded to a cold page won't find it on
    /// the hot leaf at all. Walks newest-to-oldest cold page, stopping at
    /// the first visible match; returns `None` if the whole chain (or no
    /// chain at all) doesn't have one either — a genuine miss, same as
    /// today's behavior for a key that never existed or whose delete is
    /// visible to this reader.
    ///
    /// Safe to follow `link.cold()` without any extra version validation
    /// beyond the traversal that already got us here: a leaf's `cold_link`
    /// is set once, at construction, before that leaf is ever linked into
    /// the tree (mirrors every other child page in this codebase — see
    /// `traverse_read_key`'s doc on child pages being "effectively
    /// append-only once linked"), and a cold page is likewise never
    /// mutated after construction, only ever retired outright (see
    /// `ColdLink`'s doc on "retire, don't reuse"). Whatever `cold_link()`
    /// this reader observes on an already-reachable leaf is therefore
    /// either the final state or has already been fully superseded by a
    /// wholesale leaf replacement this reader simply hasn't navigated to
    /// -- never a half-written one.
    pub(crate) fn scan_cold_chain_for_key<F: FnMut(TxStamp) -> bool>(
        mut link: crate::mv_page_model::node::ColdLink<FAN_OUT, NUM_RECORDS, Key, Payload>,
        key: Key,
        is_visible: &mut F,
    ) -> Option<RecordPointResult<Key, Payload>> {
        while !link.is_none() {
            let cold_guard = link.cold().borrow_read();
            let cold_leaf = cold_guard.as_leaf_page_ref();
            if let Some(r) = Self::scan_leaf_for_key(cold_leaf, key, is_visible) {
                return Some(RecordPointResult::from_leaf(r));
            }
            link = *cold_guard.cold_link();
        }
        None
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

        // `LeafPage`'s SoA layout keeps keys in their own contiguous region,
        // physically separate from the version/payload region (see
        // `LeafPage::keys`/`record`'s docs) — so scanning `keys` alone finds
        // every candidate position without paying, for every *non-matching*
        // slot, for `record(i)`'s `VersionInfo`/`PayloadSlot` reference
        // construction or the `self.len()` reload `keys()`/`data()` each do
        // internally. Only positions that already match `key` ever reach
        // `leaf.record(i)` below.
        let leaf = records.as_leaf_page_ref();
        let keys = leaf.keys();

        // `with_snapshot_cache_and_logs`, not `with_visibility_checker`: see
        // that method's doc and `RangeQueryIter::refill`'s identical call —
        // builds `is_visible` as a concrete, inlinable closure right here
        // instead of receiving a `&mut dyn FnMut` across the callback
        // boundary.
        self.with_snapshot_cache_and_logs(|cache, commit_logs| {
            let mut is_visible = |stamp| crate::mv_sync::visibility::is_visible(
                commit_logs, cache, reader_worker, reader_ts_start, stamp);

            let mut found = None;
            // Shared by both branches below: given a candidate position
            // that already matched `key`, check its visibility and record
            // it as `found` — returns `true` to signal "stop looking" to
            // whichever scan is driving it.
            let mut check_candidate = |i: usize| {
                let r = leaf.record(i);
                if r.version().matches(&mut is_visible) {
                    found = Some(r);
                    true
                } else {
                    false
                }
            };

            // `Key` is `u64` for every real table in this codebase — see
            // `mv_page_model::simd_keys`'s doc — so this AVX2-accelerated
            // scan (falls back to an identical scalar loop when AVX2 isn't
            // available, e.g. off `x86_64`) is the common path in practice;
            // the `filter().any()` below only ever runs for a hypothetical
            // non-`u64` `Key`, which nothing in this codebase instantiates.
            match crate::mv_page_model::simd_keys::try_u64_scalar(key)
                .zip(crate::mv_page_model::simd_keys::try_u64_keys(keys))
            {
                Some((target, u64_keys)) => {
                    crate::mv_page_model::simd_keys::find_eq_desc(u64_keys, target, &mut check_candidate);
                }
                None => {
                    // No `skip_while`/early exit on
                    // `insertion_stamp().ts_start() > reader_ts_start` here
                    // (there used to be one) — it assumed a leaf's physical
                    // (append) order tracks `ts_start` order, so once a
                    // "future" (not-yet-visible) entry was skipped walking
                    // backwards, everything further back was assumed
                    // visible-or-older too. That assumption dates back to a
                    // single-global-version model (`git log -L` on this
                    // line: originally `r.version.insert_version >
                    // lookup_version`) and never held under OSIC's actual
                    // concurrency model: a transaction's `ts_start` is drawn
                    // at `begin()`, *before* it acquires the leaf's write
                    // lock to physically append — two concurrent writers can
                    // draw `ts_start` in one order but append in the other
                    // (whichever wins the lock lands in the leaf first), so
                    // physical order and `ts_start` order can diverge. When
                    // they did, this `skip_while` could walk straight past
                    // the one record actually visible to `reader_ts_start`,
                    // silently returning `None` for a live,
                    // definitely-committed key — confirmed as the mechanism
                    // behind `verify_concurrent_shared_keys`'s intermittent
                    // `v[0]` index-out-of-bounds panic (empty
                    // `MatchedRecords` for a key that's never deleted).
                    // `RangeQueryIter::refill` (`iter_query.rs`) never had
                    // this shortcut and scans every record regardless of
                    // order, which is why only the point-read path was ever
                    // affected. Every position that matches `key` above
                    // still gets its version checked via `check_candidate`
                    // — this reorder only defers *which* records pay for a
                    // version/visibility check, never skips one outright.
                    keys.iter().enumerate().rev()
                        .filter(|(_, k)| **k == key)
                        .any(|(i, _)| check_candidate(i));
                }
            }

            match found {
                None => {
                    // Hot leaf came up empty -- either the key genuinely
                    // isn't visible to this reader, or the version it needs
                    // has since been offloaded to a cold page (see
                    // `ColdLink`'s doc). `is_none()` makes this a single
                    // field read for the overwhelming common case (no cold
                    // chain at all), so a plain fresh-snapshot read pays
                    // nothing extra here.
                    let cold_link = *records.cold_link();
                    if !cold_link.is_none() {
                        if let Some(result) =
                            Self::scan_cold_chain_for_key(cold_link, key, &mut is_visible)
                        {
                            return CRUDOperationResult::MatchedRecords(vec![result]);
                        }
                    }

                    if crate::mv_test::DIAG {
                        let same_key: Vec<String> = keys.iter().enumerate()
                            .filter(|(_, k)| **k == key)
                            .map(|(i, _)| {
                                let r = leaf.record(i);
                                format!(
                                    "insert=(w{},{}) invalid={} deleted={} is_vis_insert={} is_vis_del={:?}",
                                    r.version.insertion_stamp().worker_id(),
                                    r.version.insertion_stamp().ts_start(),
                                    r.version.insertion_stamp().is_invalid(),
                                    r.version.deletion_stamp().map(|d| d.to_string()).unwrap_or_else(|| "*".to_string()),
                                    is_visible(r.version.insertion_stamp()),
                                    r.version.deletion_stamp().map(|d| is_visible(d)),
                                )
                            }).collect();
                        eprintln!(
                            "DIAG key_point_read_from_root: MISS key={key} reader=(w{reader_worker},{reader_ts_start}) leaf_len={} same_key_records={same_key:?}",
                            keys.len(),
                        );
                    }
                    CRUDOperationResult::MatchedRecords(Vec::with_capacity(0))
                }
                Some(result) =>
                    CRUDOperationResult::MatchedRecords(vec![RecordPointResult::from_leaf(result)])
            }
        })
    }

    /// Fresh-snapshot point existence check without allocating a result
    /// vector or cloning the payload. This is the point-read counterpart to
    /// range `count_ref`: callers that only need hit/miss should not pay to
    /// materialize a one-row result.
    #[inline]
    pub fn point_exists_si(&self, key: Key) -> bool {
        let reader_worker = self.worker_id();
        let reader_ts_start = self.begin_snapshot();
        let root = self.retrieve_root_for(reader_ts_start);
        let records = Self::traverse_read_key(&root, key, reader_ts_start);
        let found = self.with_snapshot_cache_and_logs(|cache, commit_logs| {
            let mut is_visible = |stamp| crate::mv_sync::visibility::is_visible(
                commit_logs, cache, reader_worker, reader_ts_start, stamp);
            records.as_records().iter().rev().any(|record|
                record.key() == key && record.version().matches(&mut is_visible))
        });
        self.end_snapshot(reader_ts_start);
        found
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

        // See `key_point_read_from_root`'s identical switch to
        // `with_snapshot_cache_and_logs` — same rationale: this closure runs
        // once per physical record across every leaf `blocks` holds, not
        // once per call, so an inlinable `is_visible` beats a `dyn FnMut`
        // received across the callback boundary.
        self.with_snapshot_cache_and_logs(|cache, commit_logs| {
            let mut is_visible = |stamp| crate::mv_sync::visibility::is_visible(
                commit_logs, cache, reader_worker, reader_ts_start, stamp);
            CRUDOperationResult::MatchedRecords(blocks
                .into_iter()
                .map(|leaf| {
                let records = leaf
                    .deref()
                    .as_records();

                // No `binary_search_by` cutoff on `ts_start` here (there
                // used to be one, computing a `start_pos_si` to `.skip()`
                // below) — `binary_search_by`
                // requires the slice to already be sorted by the key it
                // searches on, and a leaf's physical (append) order does
                // *not* track `ts_start` order under OSIC's actual
                // concurrency model: a transaction's `ts_start` is drawn at
                // `begin()`, before it acquires the leaf's write lock to
                // physically append, so two concurrent writers can draw
                // `ts_start` in one order but append in the other. On an
                // unsorted slice, `binary_search_by` can land anywhere —
                // not just "skip too much/too little" but a genuinely
                // arbitrary cutoff — silently dropping matching records.
                // See `key_point_read_from_root`'s identical (now-fixed)
                // `skip_while` bug for the confirmed real-world symptom this
                // exact assumption caused. Scanning the whole leaf (same
                // cost `RangeQueryIter::refill`/`iter_query.rs` already
                // always pays) is the honest fix.
                records
                    .iter()
                    .rev()
                    // Cheap range check before the indirect (`&mut dyn
                    // FnMut`, not inlinable) `matches` call — see
                    // `iter_query.rs::refill`'s identical reorder for why.
                    .filter(|r|
                        lookup_range.contains(r.key()) &&
                            r.version().matches(&mut is_visible))
                    // .sorted_by_key(|r| r.key())
                    .map(RecordPointResult::from_leaf)
                    .collect::<Vec<_>>()
                })
                // .filter(|set| !set.is_empty())
                // .sorted_by_key(|set|
                //     unsafe { set.get_unchecked(0).key })
                .flatten()
                .collect())
        })
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
