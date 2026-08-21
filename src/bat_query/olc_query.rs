use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use std::ops::Deref;

use crate::bat_block::block::BlockGuard;
use crate::bat_page_model::{Attempts, BlockRef};

use crate::bat_page_model::node::PageType;
use crate::bat_sync::smart_cell::sched_yield;
use crate::bat_test;
use crate::bat_test::{LOG_REORG, VERBOSE};
use crate::bat_tree::mvbt::MVBTSt;
use crate::bat_tree::smo::BlockUnsafeDegree;

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Pins page reclamation for this traversal of the tree's *current*
    /// state for the whole descent, closing a gap `Point`/`Range` reads
    /// don't have: `get_pointer` reads a child pointer with no
    /// synchronization of its own (see `InternalPage::pointer_region`'s
    /// doc), trusting that nothing concurrently reclaims the node out from
    /// under it. GC's reclaim decision (`live_min_snapshot`, `bat_gc`) was
    /// previously blind to write-path traversals entirely — only
    /// `Point`/`Range`/`*Si` reads and `DbTransaction`s registered via
    /// `on_acquire_reader_snapshot`/`begin_snapshot` — so a plain
    /// `dispatch_crud` `Insert`/`Update`/`Delete` (or WAL-recovery replay,
    /// or `DbTransaction::abort_write`, all of which land here) could have
    /// a node reclaimed and reset (`Node::on_reuse`) while still mid-descent
    /// through it. Registering here, rather than once per call site in
    /// `dispatch.rs`, covers all of them for free and scopes the
    /// pin tightly to just the traversal, not the write that follows it.
    /// This is deliberately not a real snapshot: current-tree writers need
    /// allocation lifetime, not MVCC visibility, so no GLC tick is drawn.
    /// An enclosing transaction's live snapshot already supplies the pin.
    #[inline]
    pub(crate) fn traversal_write_olc(
        &self,
        key: Key,
    ) -> BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.with_reclamation_pin(|| self.traversal_write_olc_registered(key))
    }

    /// OLC descent for a caller that already owns a live snapshot
    /// registration covering the entire operation. `DbTransaction` and
    /// `TpccTxn` register once at begin and release only after commit/abort,
    /// so drawing and nesting another snapshot for every write would add a
    /// global-clock increment plus depth bookkeeping without adding any
    /// reclamation protection.
    #[inline]
    pub(crate) fn traversal_write_olc_registered(
        &self,
        key: Key,
    ) -> BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mut attempt = 0;

        loop {
            match self.traversal_write_internal_olc(key, attempt) {
                Err(n_attempt) => {
                    attempt = n_attempt;

                    sched_yield(attempt);
                }
                Ok(guard) => {
                    if bat_test::RESTART_TRACE {
                        bat_test::record_write_attempts(attempt as usize);
                    }
                    break guard;
                }
            }
            }
    }

    #[inline]
    pub(crate) fn retrieve_root_write_olc(
        &self,
        mut attempts: Attempts,
    ) -> (BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>, Attempts) {
        loop {
            match self.retrieve_root_write_internal_olc() {
                Ok(guard) => break (guard, attempts),
                _ => {
                    attempts += 1;
                    if VERBOSE {
                        println!("retrieve_root_write_internal_olc: attempts {:?}", attempts);
                    }
                    sched_yield(attempts);
                }
            }
        }
    }

    #[inline]
    fn retrieve_root_write_internal_olc(
        &self,
    ) -> Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, ()> {
        let root = &self.root;

        let mut master_guard = root.borrow_read();

        let root_block = master_guard.block();

        let mut root_guard = root_block.borrow_read();

        if LOG_REORG {
            let r = root_guard.deref().unsafe_degree_root();

            match r {
                BlockUnsafeDegree::Overflow => unsafe {
                    bat_test::SPLITS_ROOT_COUNTER
                        .lock()
                        .push(self.current_version())
                },
                BlockUnsafeDegree::ActiveUnderflow => unsafe {
                    bat_test::MERGE_ROOT_COUNTER
                        .lock()
                        .push(self.current_version())
                },
                _ => {}
            }
        }
        match root_guard.deref().unsafe_degree_root() {
            // `root_guard` deliberately stays an unexcluded `Reader` here —
            // `split_root` excludes it itself via `try_retire()` (the old
            // root is only ever read, never mutated in place, and once this
            // arm is reached the old root is unconditionally superseded).
            // Same reasoning as `on_overflow_node`'s `simba`; see
            // `SmartGuard::try_retire`'s doc.
            BlockUnsafeDegree::Overflow if master_guard.upgrade_write_lock() => {
                self.split_root(master_guard, root_guard, root.height())
            }
            // Unlike the `Overflow` arm above, `root_guard` here DOES need
            // `upgrade_write_lock()` — `merge_root` never retires
            // `root_guard` itself, it only reads `root_guard.last_child()`
            // to find the child to promote. `unsafe_degree_root()`'s
            // `active == 1` check just above is a snapshot, not a standing
            // guarantee: `on_overflow_node` proves elsewhere in this file
            // that pushing a new sibling into a parent's page requires only
            // that parent's `upgrade_write_lock()`, nothing from the child
            // side — so a concurrent overflow of root's one active child
            // can freely take `root_guard`'s lock (we're not holding it),
            // split that child, and push a second active child into root,
            // all before we get to `last_child()`. Discarding this
            // function's result afterward (as an earlier version of this
            // arm did, unconditionally returning `Err(())`) does NOT make
            // that safe: `merge_root`'s call to `split_root` retires
            // whatever `last_child()` returned and unconditionally
            // publishes it as the new root via `self.root.append_root(..)`
            // *before* returning anything — by the time we could discard a
            // bad result, the wrong child has already silently replaced the
            // whole tree, permanently orphaning the other, genuinely live
            // one. No panic, no error — just silent data loss. Taking the
            // lock here forces that concurrent overflow to finish first (or
            // us to lose the CAS and retry), so `last_child()` is read
            // against a state that's still genuinely at `active == 1`.
            BlockUnsafeDegree::ActiveUnderflow
            if master_guard.upgrade_write_lock() && root_guard.upgrade_write_lock() =>
            {
                self.merge_root(master_guard, root_guard, root.height())
            }
            BlockUnsafeDegree::Ok => Ok(root_guard),
            _ => {
                if bat_test::RESTART_TRACE {
                    bat_test::ROOT_RESTARTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    bat_test::record_root_restart_for_table(self as *const Self as usize);
                }
                Err(())
            }
        }
    }

    #[inline]
    fn traversal_write_internal_olc(
        &'_ self,
        key: Key,
        attempts: Attempts,
    ) -> Result<BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>, Attempts> {
        let (mut curr_guard, attempts) = self.retrieve_root_write_olc(attempts);

        let mut i =  0;
        loop {
            if VERBOSE {
                println!(
                    "traversal_write_internal_olc: Loop: {i}, attempts {attempts}, key: {key}"
                );
                i += 1;
            }

            match curr_guard.as_page_ref() {
                PageType::IndexRef(internal_page) => unsafe {
                    // `curr_guard` is a bare `Reader` here whenever this
                    // level itself doesn't need correcting (only a *child*
                    // might) — nothing excludes a genuinely different
                    // thread that's concurrently write-locked this same
                    // block as `mufasa` for one of its *other* children.
                    // Bracket the read with a lock-free/unchanged check so a
                    // torn read (this project confirmed one in practice: a
                    // `keys_versions()` slice observed with different content
                    // microseconds apart, on the exact same memory, while
                    // this same page was mid-SMO on another thread) forces a
                    // retry instead of silently computing a wrong `index` and
                    // confidently descending into it. Skipped when
                    // `curr_guard` is already *our
                    // own* `Writer` from earlier in this traversal — that
                    // exclusion already makes its content stable.
                    //
                    // `curr_guard` may also be a `Reader` obtained *before*
                    // this exact node was folded into a replacement by a
                    // concurrent `on_overflow_node`/`on_underflow_node`
                    // elsewhere (this level itself was never the one
                    // overflowing/underflowing — only reached here as
                    // *somebody else's* now-obsolete child). Once that
                    // happens its content is frozen forever (nothing ever
                    // mutates a retired node again), so a plain before/after
                    // version comparison alone can never catch it — it sees
                    // "unchanged" for all eternity. See `RETIRED_FLAG_VERSION`'s
                    // doc. `checked_live_version()` folds both this check
                    // and the write-locked one into the same single atomic
                    // load that captures `curr_version_before` below — see
                    // its own doc for why collapsing three back-to-back
                    // reads of the same value into one changes nothing about
                    // what can be observed.
                    let curr_version_before = match curr_guard.checked_live_version() {
                        Some(v) => v,
                        None => {
                            if bat_test::RESTART_TRACE {
                                bat_test::record_restart(
                                    curr_guard.inner().0 as usize,
                                    &key,
                                    "checked_live_version",
                                );
                            }
                            return Err(attempts + 1);
                            }
                    };

                    let keys_page = internal_page.keys();

                    // No liveness check needed here: appends within a page
                    // are strictly index-ordered by recency, and a split/
                    // merge's new entries always exactly re-cover the old
                    // entry's key-interval they supersede — so the
                    // *highest*-index entry whose interval contains `key` is
                    // always the live one, whether or not anything has
                    // superseded a lower-index entry that also happens to
                    // contain `key`. See `InternalPage::live_mask`'s doc for
                    // the general form of this argument.
                    let index = keys_page
                        .iter()
                        .enumerate()
                        .rfind(|(_, range)| range.contains(key))
                        .map(|(pos, ..)| pos);

                    if let None = index {
                        if VERBOSE {
                            println!("traversal_write_internal_olc: None Index");
                        }
                        if bat_test::RESTART_TRACE {
                            bat_test::record_restart(
                                curr_guard.inner().0 as usize,
                                &key,
                                "index_lookup_miss",
                            );
                        }
                        return Err(attempts + 1);
                    }

                    let index = index.unwrap();

                    // `get_pointer` also reads `pointer_region`, written by
                    // the same `push_uncommitted`/`bulk_push*` calls that
                    // mutate `key_interval_region`/`version_region` — keep
                    // it inside the validated window too, not just the
                    // index lookup above.
                    let next_curr_guard = internal_page.get_pointer(index).borrow_read();

                    if curr_guard.live_version() != curr_version_before {
                        if VERBOSE {
                            println!(
                                "traversal_write_internal_olc: curr_guard changed during index lookup"
                            );
                        }
                        if bat_test::RESTART_TRACE {
                            bat_test::record_restart(
                                curr_guard.inner().0 as usize,
                                &key,
                                "index_lookup_race",
                            );
                        }
                        return Err(attempts + 1);
                    }

                    if LOG_REORG {
                        let r = next_curr_guard.deref().unsafe_degree(&self.ctx);

                        match r {
                            BlockUnsafeDegree::Overflow => {
                                bat_test::SPLITS_COUNTER.lock().push(self.current_version())
                            }
                            BlockUnsafeDegree::ActiveUnderflow => {
                                bat_test::MERGES_COUNTER.lock().push(self.current_version())
                            }
                            _ => {}
                        }
                    }
                    let curr_page_addr = if bat_test::RESTART_TRACE {
                        curr_guard.inner().0 as usize
                    } else {
                        0
                    };
                    match next_curr_guard.unsafe_degree(&self.ctx) {
                        // `next_curr_guard` deliberately stays an
                        // unexcluded `Reader` here — `on_overflow_node`
                        // retires it itself via `try_retire()` (only ever
                        // read, and once reached, always used — see that
                        // function's doc). Only `curr_guard`/`mufasa`
                        // (mutated in place) needs the ordinary upgrade.
                        BlockUnsafeDegree::Overflow if curr_guard.upgrade_write_lock() => {
                            match self.on_overflow_node(curr_guard, next_curr_guard, index) {
                                Ok(guard) => curr_guard = guard,
                                Err(..) => {
                                    if VERBOSE {
                                        println!(
                                            "traversal_write_internal_olc: on_overflow_node Err()"
                                        );
                                    }
                                    if bat_test::RESTART_TRACE {
                                        bat_test::record_restart(
                                            curr_page_addr,
                                            &key,
                                            "on_overflow_node",
                                        );
                                    }
                                    return Err(attempts + 1);
                                }
                                    }
                                }
                        // `next_curr_guard` also stays an unexcluded
                        // `Reader` here now — `on_underflow_node` retires
                        // it itself via `try_retire()`, with an explicit
                        // revert (`clear_retired()`) on the one path where
                        // `merge()` fails after that point. See that
                        // function's doc for why the revert is sound.
                        BlockUnsafeDegree::ActiveUnderflow if curr_guard.upgrade_write_lock() => {
                            match self.on_underflow_node(curr_guard, next_curr_guard, index) {
                                Ok(guard) => curr_guard = guard,
                                Err(..) => {
                                    if VERBOSE {
                                        println!(
                                            "traversal_write_internal_olc: on_underflow_node Err()"
                                        );
                                    }
                                    if bat_test::RESTART_TRACE {
                                        bat_test::record_restart(
                                            curr_page_addr,
                                            &key,
                                            "on_underflow_node",
                                        );
                                    }
                                    return Err(attempts + 1);
                                }
                                    }
                                }
                        BlockUnsafeDegree::Ok => curr_guard = next_curr_guard,
                        _ => {
                            if bat_test::RESTART_TRACE {
                                bat_test::record_restart(
                                    curr_page_addr,
                                    &key,
                                    "parent_fix_write_lock",
                                );
                        }
                            return Err(attempts + 1);
                    }
                }
                },
                _ => {
                    let leaf_addr = if bat_test::RESTART_TRACE {
                        curr_guard.inner().0 as usize
                    } else {
                        0
                    };
                    return if curr_guard.upgrade_write_lock() {
                        Ok(curr_guard)
                    } else {
                        if VERBOSE {
                            println!("traversal_write_internal_olc: upgrade_write_lock Err()");
                        }
                        if bat_test::RESTART_TRACE {
                            bat_test::record_restart(leaf_addr, &key, "leaf_write_lock");
                        }
                        Err(attempts + 1)
                    };
                }
            }
        }
    }

    /// Idle/proactive compaction entry point: forces exactly the same
    /// latch/split/commit protocol a real overflow would trigger via
    /// `on_overflow_node`, but for whichever leaf currently covering `key`
    /// has a dead/(active+dead) ratio at or above `dead_ratio_threshold` —
    /// see `traversal_compact_internal_olc`'s doc for why this needs its own
    /// descent rather than reusing `traversal_write_olc`. Returns whether a
    /// compaction actually happened: `false` if, by the time the descent
    /// reached it, the leaf no longer qualified (someone else already
    /// compacted it, a concurrent write pushed it into a real overflow
    /// instead — that path already compacts it as a side effect, see
    /// `split()`'s doc — or the candidate scan that picked `key` is simply
    /// stale).
    pub(crate) fn compact_leaf_olc(&self, key: Key, dead_ratio_threshold: f64) -> bool {
        self.with_reclamation_pin(|| {
            let mut attempts = 0;
            loop {
                match self.traversal_compact_internal_olc(key, dead_ratio_threshold, attempts) {
                    Ok((guard, compacted)) => {
                        drop(guard);
                        break compacted;
                    }
                    Err(n_attempts) => {
                        attempts = n_attempts;
                        sched_yield(attempts);
                    }
                }
            }
        })
    }

    /// Root-aware counterpart to `retrieve_root_write_olc`, for the
    /// compaction traversal only: a tree with few enough live keys has no
    /// internal pages at all, so its root *is* a leaf directly, with no
    /// parent for `traversal_compact_internal_olc`'s own loop to force a
    /// compaction from (that loop only ever checks a *child* it's about to
    /// descend into, via its parent's `on_overflow_node`). Mirrors
    /// `retrieve_root_write_internal_olc`'s real overflow/underflow
    /// handling byte for byte, plus one added arm: an otherwise-`Ok` leaf
    /// root whose dead ratio crosses `dead_ratio_threshold` gets the same
    /// `split_root` call a real overflow would, which — like
    /// `on_overflow_node` — compacts rather than restructures a leaf that
    /// isn't also key-overflowing (see `split()`'s doc). Returns whether
    /// that happened, threaded into `traversal_compact_internal_olc`'s own
    /// `compacted` flag.
    #[inline]
    fn retrieve_root_compact_olc(
        &self,
        dead_ratio_threshold: f64,
        mut attempts: Attempts,
    ) -> (BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>, Attempts, bool) {
        loop {
            match self.retrieve_root_compact_internal_olc(dead_ratio_threshold) {
                Ok((guard, compacted)) => break (guard, attempts, compacted),
                Err(()) => {
                    attempts += 1;
                    sched_yield(attempts);
                }
            }
        }
    }

    #[inline]
    fn retrieve_root_compact_internal_olc(
        &self,
        dead_ratio_threshold: f64,
    ) -> Result<(BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>, bool), ()> {
        let root = &self.root;

        let mut master_guard = root.borrow_read();

        let root_block = master_guard.block();

        let mut root_guard = root_block.borrow_read();

        let needs_compaction = root_guard.is_leaf() && {
            let (active, dead) = root_guard.active_dead_count();
            let total = active as u64 + dead as u64;
            total > 0 && dead as f64 / total as f64 >= dead_ratio_threshold
        };

        match root_guard.deref().unsafe_degree_root() {
            // Same reasoning as `retrieve_root_write_internal_olc`'s
            // identical arms — see that function's doc.
            BlockUnsafeDegree::Overflow if master_guard.upgrade_write_lock() => self
                .split_root(master_guard, root_guard, root.height())
                .map(|guard| (guard, false)),
            BlockUnsafeDegree::ActiveUnderflow
                if master_guard.upgrade_write_lock() && root_guard.upgrade_write_lock() =>
            {
                self.merge_root(master_guard, root_guard, root.height())
                    .map(|guard| (guard, false))
            }
            BlockUnsafeDegree::Ok if needs_compaction && master_guard.upgrade_write_lock() => self
                .split_root(master_guard, root_guard, root.height())
                .map(|guard| (guard, true)),
            BlockUnsafeDegree::Ok => Ok((root_guard, false)),
            _ => Err(()),
        }
    }

    /// A hand-duplicated copy of `traversal_write_internal_olc` — not a
    /// parameterized version of it — with exactly one addition: when the
    /// child about to become `curr_guard` is a leaf whose
    /// `active_dead_count()` ratio has crossed `dead_ratio_threshold` *and*
    /// its physical `unsafe_degree()` is otherwise `Ok` (no overflow/
    /// underflow correction already pending, which already handles this
    /// leaf one way or another), this forces the same `on_overflow_node`
    /// call a real overflow would get. `split()` decides `ByKey` vs
    /// `ByVersion` from the live count regardless of why it was called (see
    /// that function's doc), so an under-capacity, garbage-heavy leaf comes
    /// back compacted (`ByVersion`) rather than restructured.
    ///
    /// Duplicated rather than folded into `traversal_write_internal_olc`
    /// itself: that function sits on every single OLTP write's hot path and
    /// has a documented history of subtle, timing-dependent corruption bugs
    /// (see its own comments) — keeping this feature's one added branch
    /// (and the extra `active_dead_count()` read it needs on every level)
    /// off that path entirely is worth the duplication, at the cost of the
    /// two functions drifting if one changes without the other someday.
    fn traversal_compact_internal_olc(
        &'_ self,
        key: Key,
        dead_ratio_threshold: f64,
        attempts: Attempts,
    ) -> Result<(BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>, bool), Attempts> {
        let (mut curr_guard, attempts, mut compacted) =
            self.retrieve_root_compact_olc(dead_ratio_threshold, attempts);

        loop {
            match curr_guard.as_page_ref() {
                PageType::IndexRef(internal_page) => unsafe {
                    // Same OLC race guards as `traversal_write_internal_olc`
                    // — see that function's doc for why each one is needed.
                    let curr_version_before = match curr_guard.checked_live_version() {
                        Some(v) => v,
                        None => return Err(attempts + 1),
                    };

                    let keys_page = internal_page.keys();
                    let index = keys_page
                        .iter()
                        .enumerate()
                        .rfind(|(_, range)| range.contains(key))
                        .map(|(pos, ..)| pos);

                    let Some(index) = index else {
                        return Err(attempts + 1);
                    };

                    let next_curr_guard = internal_page.get_pointer(index).borrow_read();

                    if curr_guard.live_version() != curr_version_before {
                        return Err(attempts + 1);
                    }

                    let needs_compaction = next_curr_guard.is_leaf() && {
                        let (active, dead) = next_curr_guard.active_dead_count();
                        let total = active as u64 + dead as u64;
                        total > 0 && dead as f64 / total as f64 >= dead_ratio_threshold
                    };

                    match next_curr_guard.unsafe_degree(&self.ctx) {
                        BlockUnsafeDegree::Overflow if curr_guard.upgrade_write_lock() => {
                            match self.on_overflow_node(curr_guard, next_curr_guard, index) {
                                Ok(guard) => curr_guard = guard,
                                Err(..) => return Err(attempts + 1),
                            }
                        }
                        BlockUnsafeDegree::ActiveUnderflow if curr_guard.upgrade_write_lock() => {
                            match self.on_underflow_node(curr_guard, next_curr_guard, index) {
                                Ok(guard) => curr_guard = guard,
                                Err(..) => return Err(attempts + 1),
                            }
                        }
                        BlockUnsafeDegree::Ok if needs_compaction && curr_guard.upgrade_write_lock() => {
                            match self.on_overflow_node(curr_guard, next_curr_guard, index) {
                                Ok(guard) => {
                                    curr_guard = guard;
                                    compacted = true;
                                }
                                Err(..) => return Err(attempts + 1),
                            }
                        }
                        BlockUnsafeDegree::Ok => curr_guard = next_curr_guard,
                        _ => return Err(attempts + 1),
                    }
                },
                // Unlike `traversal_write_internal_olc`'s identical-looking
                // base case, this traversal never writes to the leaf it
                // finally lands on — any compaction it needed already
                // happened one level up, via `on_overflow_node` on this
                // leaf as somebody else's child (or `split_root` if it's
                // the tree's root — see `retrieve_root_compact_olc`'s doc).
                // `compact_leaf_olc` only ever drops the returned guard, so
                // there's nothing here that needs exclusive access — a
                // plain, already-valid `Reader` is enough.
                _ => return Ok((curr_guard, compacted)),
            }
        }
    }
}