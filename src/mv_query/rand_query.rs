use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;

use crate::mv_block::block::BlockGuard;
use crate::mv_page_model::Attempts;
use crate::mv_page_model::internal_page::Fence;
use crate::mv_page_model::node::PageType;
use crate::mv_test;
use crate::mv_test::{LOG_REORG, VERBOSE};
use crate::mv_tree::mvbt::MVBTSt;

use crate::mv_sync::smart_cell::sched_yield;
use crate::mv_tree::smo::BlockUnsafeDegree;

pub const RAND_ATTEMPTS_MAX: Attempts = 10; // for insertion generation upper bound

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// See `olc_query::traversal_write_olc`'s matching doc — same
    /// GC-reclaim-registration gap, same fix. A lightweight reclamation pin
    /// is sufficient here; this current-tree traversal needs no MVCC
    /// timestamp. Nothing
    /// about a random-key write traversal makes it exempt from the same
    /// "GC's reclaim decision was previously blind to write-path
    /// traversals" gap `traversal_write_olc`'s doc describes.
    #[inline]
    pub(crate) fn traversal_write_rand_query(
        &self,
    ) -> (
        Fence<Key>,
        BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
    ) {
        self.with_reclamation_pin(|| {
        let mut attempt = 0;
            loop {
            match self.traversal_write_internal_rand(attempt) {
                Err(n_attempt) => {
                    attempt = n_attempt;
                    sched_yield(attempt);
                }
                Ok(guard) => break guard,
            }
            }
        })
    }

    #[inline]
    fn traversal_write_internal_rand(
        &self,
        attempts: Attempts,
    ) -> Result<(Fence<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>), Attempts> {
        let (mut curr_guard, attempts) = self.retrieve_root_write_olc(attempts);

        let mut curr_fence = Fence::new(self.cold.min_key, self.cold.max_key);

        let mut traversal_loops =  0;
        loop {
            let curr_guard_result = curr_guard.deref();

            match curr_guard_result.as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let sum_len = internal_page.sum_len();

                    let raw_index = if sum_len == 0 {
                        0
                    } else {
                        rand::random_range(0..sum_len)
                    };

                    // Picks a uniformly random *slot* (live or not), then
                    // resolves it to whichever slot currently owns that
                    // slot's own key — mimics the old per-slot `is_active()`
                    // filter without needing a flag at all: the highest-
                    // index slot whose interval contains a given key is
                    // always the live one (see `InternalPage::live_mask`'s
                    // doc for why). A slot that's since been split into
                    // several still-live descendants is proportionally more
                    // likely to be reached this way than a slot that hasn't
                    // — unlike the old "uniform over currently-live slots"
                    // sampling — which is fine for this random stress walk.
                    let probe_key = internal_page.get_key(raw_index).lower;

                    let index = internal_page
                        .keys()
                        .iter()
                        .enumerate()
                        .rfind(|(_, range)| range.contains(probe_key))
                        .map(|(pos, ..)| pos)
                        .unwrap();

                    if VERBOSE {
                        println!(
                            "traversal_write_internal_olc: Loop: {traversal_loops}, attempts {attempts}, live_index: {index}"
                        );
                        traversal_loops += 1;
                    }

                    curr_fence = internal_page.get_key(index).clone();
                    let next_curr_block = internal_page.get_pointer(index);

                    let next_curr_guard = next_curr_block.borrow_read();

                    if LOG_REORG {
                        let r = next_curr_guard.deref().unsafe_degree();

                        match r {
                            BlockUnsafeDegree::Overflow => unsafe {
                                mv_test::SPLITS_COUNTER.lock().push(self.current_version())
                            },
                            BlockUnsafeDegree::ActiveUnderflow => unsafe {
                                mv_test::MERGES_COUNTER.lock().push(self.current_version())
                            },
                            _ => {}
                        }
                    }
                    match next_curr_guard.deref().unsafe_degree() {
                        BlockUnsafeDegree::Overflow if curr_guard.upgrade_write_lock() => {
                            match self.on_overflow_node(curr_guard, next_curr_guard, index) {
                            Ok(guard) => curr_guard = guard,
                            Err(..) => {
                                if VERBOSE {
                                        println!(
                                            "traversal_write_internal_rand: on_overflow_node Err()"
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
                                    return Err(attempts + 1);
                                }
                                }
                            }
                        BlockUnsafeDegree::Ok => curr_guard = next_curr_guard,
                        _ => return Err(attempts + 1),
                    }
                }
                _ => {
                    return if curr_guard.upgrade_write_lock() {
                    Ok((curr_fence, curr_guard))
                } else {
                    if VERBOSE {
                        println!("traversal_write_internal_olc: upgrade_write_lock Err()");
                    }
                    Err(attempts + 1)
                    };
                }
            }
        }
    }
}