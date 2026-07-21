use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use std::ops::Deref;

use crate::mv_block::block::BlockGuard;
use crate::mv_page_model::{Attempts, BlockRef};

use crate::mv_page_model::node::PageType;
use crate::mv_page_model::time_matcher::TimeMatcher;
use crate::mv_test;
use crate::mv_test::{LOG_REORG, VERBOSE};
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_sync::smart_cell::sched_yield;
use crate::mv_tree::smo::BlockUnsafeDegree;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Registers this traversal as a live reader of the tree's *current*
    /// state for the whole descent, closing a gap `Point`/`Range` reads
    /// don't have: `get_pointer` reads a child pointer with no
    /// synchronization of its own (see `InternalPage::pointer_region`'s
    /// doc), trusting that nothing concurrently reclaims the node out from
    /// under it. GC's reclaim decision (`live_min_snapshot`, `mv_gc`) was
    /// previously blind to write-path traversals entirely — only
    /// `Point`/`Range`/`*Si` reads and `DbTransaction`s registered via
    /// `on_acquire_reader_snapshot`/`begin_snapshot` — so a plain
    /// `dispatch_crud` `Insert`/`Update`/`Delete` (or WAL-recovery replay,
    /// or `DbTransaction::abort_write`, all of which land here) could have
    /// a node reclaimed and reset (`Node::on_reuse`) while still mid-descent
    /// through it. Registering here, rather than once per call site in
    /// `dispatch.rs`, covers all of them for free and scopes the
    /// registration tightly to just the traversal, not the write that
    /// follows it. Nesting inside an already-registered `DbTransaction` is
    /// fine — concurrent registrations for the same value are explicitly
    /// designed to stack (`TransactionTrace`'s doc). A no-op with zero
    /// overhead when block reclaim (GC) is disabled — the default here —
    /// since `TxContext::on_tx_start`/`on_tx_completed` short-circuit on
    /// that flag.
    #[inline]
    pub(crate) fn traversal_write_olc(&self, key: Key) -> BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let ts_start = self.begin_snapshot();

        let mut attempt = 0;

        let guard = loop {
            match self.traversal_write_internal_olc(key, attempt) {
                Err(n_attempt) => {
                    attempt = n_attempt;

                    sched_yield(attempt);
                }
                Ok(guard) => {
                    // RESTARTS_COUNTER
                    //     .get(attempt as usize)
                    //     .inspect(|a| { a.fetch_add(1, Relaxed); });
                    break guard
                },
            }
        };

        self.end_snapshot(ts_start);

        guard
    }

    #[inline]
    pub(crate) fn retrieve_root_write_olc(
        &self,
        mut attempts: Attempts,
    ) -> (BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
        Attempts)
    {
        loop {
            match self.retrieve_root_write_internal_olc() {
                Ok(guard) =>
                    break (guard, attempts),
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
    fn retrieve_root_write_internal_olc(&self) -> Result<
        BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
        let root
            = &self.root;

        let mut master_guard
            = root.borrow_read();

        let root_block
            = master_guard.block();

        let mut root_guard
            = root_block.borrow_read();

        if LOG_REORG {
            let r
                = root_guard.deref().unsafe_degree_root();

            match r {
                BlockUnsafeDegree::Overflow => unsafe {
                    mv_test::SPLITS_ROOT_COUNTER.lock().push(self.current_version())
                }
                BlockUnsafeDegree::ActiveUnderflow => unsafe {
                    mv_test::MERGE_ROOT_COUNTER.lock().push(self.current_version())
                }
                _ => {}
            }
        }
        match root_guard.deref().unsafe_degree_root() {
            // `root_guard` deliberately stays an unexcluded `Reader` here —
            // `split_root` retires it itself via `try_retire()`, since it's
            // only ever read and, once reached, always used (see that
            // function's doc). Only `master_guard` (mutated in place, via
            // `self.root.append_root`) needs the ordinary upgrade.
            BlockUnsafeDegree::Overflow
            if master_guard.upgrade_write_lock()
            => self.split_root(master_guard, root_guard, root.height()),
            BlockUnsafeDegree::ActiveUnderflow
            if master_guard.upgrade_write_lock() && root_guard.upgrade_write_lock() => {
                let _ = self.merge_root(master_guard, root_guard, root.height());
                Err(())
            },
            BlockUnsafeDegree::Ok
            => Ok(root_guard),
            _ => Err(()),
        }
    }

    #[inline]
    fn traversal_write_internal_olc(&'_ self, key: Key, attempts: Attempts)
    -> Result<BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>, Attempts>
    {
        let (mut curr_guard,
            attempts) = self.retrieve_root_write_olc(attempts);

        let mut i =  0;
        loop {
            if VERBOSE {
                println!("traversal_write_internal_olc: Loop: {i}, attempts {attempts}, key: {key}");
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
                    // `keys_versions()` slice observed with a different
                    // `is_active()` value microseconds apart, on the exact
                    // same memory) forces a retry instead of silently
                    // computing a wrong `index` and confidently descending
                    // into it. Skipped when `curr_guard` is already *our
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
                        None => return Err(attempts + 1),
                    };

                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    let index = keys_page
                        .iter()
                        .enumerate()
                        .rfind(|(pos, range)|
                            versions_page.get_unchecked(*pos).is_active() &&
                                range.contains(key))
                        .map(|(pos, ..)| pos);

                    if let None = index {
                        if VERBOSE {
                            println!("traversal_write_internal_olc: None Index");
                        }
                        return Err(attempts + 1);
                    }

                    let index
                        = index.unwrap();

                    // `get_pointer` also reads `pointer_region`, written by
                    // the same `push_uncommitted`/`bulk_push*` calls that
                    // mutate `key_interval_region`/`version_region` — keep
                    // it inside the validated window too, not just the
                    // index lookup above.
                    let next_curr_guard = internal_page
                        .get_pointer(index)
                        .borrow_read();

                    if curr_guard.live_version() != curr_version_before {
                        if VERBOSE {
                            println!("traversal_write_internal_olc: curr_guard changed during index lookup");
                        }
                        return Err(attempts + 1);
                    }

                    if LOG_REORG {
                        let r
                            = next_curr_guard.deref().unsafe_degree();

                        match r {
                            BlockUnsafeDegree::Overflow =>
                                mv_test::SPLITS_COUNTER.lock().push(self.current_version()),
                            BlockUnsafeDegree::ActiveUnderflow =>
                                mv_test::MERGES_COUNTER.lock().push(self.current_version()),
                            _ => {}
                        }
                    }
                    match next_curr_guard.unsafe_degree() {
                        // `next_curr_guard` deliberately stays an
                        // unexcluded `Reader` here — `on_overflow_node`
                        // retires it itself via `try_retire()` (only ever
                        // read, and once reached, always used — see that
                        // function's doc). Only `curr_guard`/`mufasa`
                        // (mutated in place) needs the ordinary upgrade.
                        BlockUnsafeDegree::Overflow
                        if curr_guard.upgrade_write_lock()
                        => match self.on_overflow_node(curr_guard, next_curr_guard, index) {
                                Ok(guard) => curr_guard = guard,
                                Err(..) => {
                                    if VERBOSE {
                                        println!("traversal_write_internal_olc: on_overflow_node Err()");
                                    }
                                    return Err(attempts + 1)
                                }
                            },
                        // `next_curr_guard` also stays an unexcluded
                        // `Reader` here now — `on_underflow_node` retires
                        // it itself via `try_retire()`, with an explicit
                        // revert (`clear_retired()`) on the one path where
                        // `merge()` fails after that point. See that
                        // function's doc for why the revert is sound.
                        BlockUnsafeDegree::ActiveUnderflow
                        if curr_guard.upgrade_write_lock()
                        => match self.on_underflow_node(curr_guard, next_curr_guard, index) {
                                Ok(guard) => curr_guard = guard,
                                Err(..) => {
                                    if VERBOSE {
                                        println!("traversal_write_internal_olc: on_underflow_node Err()");
                                    }
                                    return Err(attempts + 1)
                                }
                            },
                        BlockUnsafeDegree::Ok => curr_guard = next_curr_guard,
                        _ => return Err(attempts + 1)
                    }
                }
                _ => return if curr_guard.upgrade_write_lock() {
                    Ok(curr_guard)
                } else {
                    if VERBOSE {
                        println!("traversal_write_internal_olc: upgrade_write_lock Err()");
                    }
                    Err(attempts + 1)
                }
            }
        }
    }
}