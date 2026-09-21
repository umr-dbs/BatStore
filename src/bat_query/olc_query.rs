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
    #[inline]
    pub(crate) fn traversal_write_olc(
        &self,
        key: Key,
    ) -> BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.with_reclamation_pin(|| self.traversal_write_olc_registered(key))
    }

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
            BlockUnsafeDegree::Overflow if master_guard.upgrade_write_lock() => {
                self.split_root(master_guard, root_guard, root.height())
            }
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

        let mut i = 0;
        loop {
            if VERBOSE {
                println!(
                    "traversal_write_internal_olc: Loop: {i}, attempts {attempts}, key: {key}"
                );
                i += 1;
            }

            match curr_guard.as_page_ref() {
                PageType::IndexRef(internal_page) => unsafe {
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

    #[inline]
    fn retrieve_root_compact_olc(
        &self,
        dead_ratio_threshold: f64,
        mut attempts: Attempts,
    ) -> (
        BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
        Attempts,
        bool,
    ) {
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
                        BlockUnsafeDegree::Ok
                            if needs_compaction && curr_guard.upgrade_write_lock() =>
                        {
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
                _ => return Ok((curr_guard, compacted)),
            }
        }
    }
}
