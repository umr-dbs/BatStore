use crate::bat_block::block::{Block, BlockGuard};
use crate::bat_block::block_handle::BlockAllocManager;
use crate::bat_page_model::leaf_page::LeafRecordRef;
use crate::bat_page_model::node::PageType;
use crate::bat_page_model::{BlockRef, Height};
use crate::bat_query::interval::Interval;
use crate::bat_root::index_root::RootIndexGuard;
use crate::bat_root::root::Root;
use crate::bat_sync::tx_context::TxContext;
use crate::bat_test::{record_version_split, DIAG, VERBOSE};
use crate::bat_tree::mvbt::MVBTSt;
use itertools::Itertools;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;

#[cfg(not(feature = "lightweight-gc"))]
type LiveSnapshots = std::collections::HashSet<u64>;
#[cfg(feature = "lightweight-gc")]
type LiveSnapshots = Option<u64>;

pub(crate) const TRACE_KEY_DEBUG: bool = false;

pub(crate) static TRACE_LOG: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

pub(crate) fn push_trace(s: String) {
    TRACE_LOG.lock().unwrap().push(s);
}

pub fn drain_trace_log() -> Vec<String> {
    std::mem::take(&mut *TRACE_LOG.lock().unwrap())
}

// TEMPORARY diagnostic helper.
fn diag_thread_hash() -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    hasher.finish()
}

fn nearest_key_boundary<T, K: PartialEq>(
    items: &[T],
    target: usize,
    capacity: usize,
    key_of: impl Fn(&T) -> K,
) -> usize {
    let len = items.len();
    debug_assert!(len >= 2);
    let target = target.clamp(1, len - 1);
    let is_boundary = |i: usize| key_of(&items[i - 1]) != key_of(&items[i]);
    let fits = |i: usize| i <= capacity && len - i <= capacity;

    let search = |accept: &dyn Fn(usize) -> bool| -> Option<usize> {
        if accept(target) {
            return Some(target);
        }

        let mut lo = target;
        while lo > 1 && !accept(lo) {
            lo -= 1;
        }
        let found_lo = accept(lo).then_some(lo);

        let mut hi = target;
        while hi < len - 1 && !accept(hi) {
            hi += 1;
        }
        let found_hi = accept(hi).then_some(hi);

        match (found_lo, found_hi) {
            (Some(l), Some(h)) => Some(if target - l <= h - target { l } else { h }),
            (Some(l), None) => Some(l),
            (None, Some(h)) => Some(h),
            (None, None) => None,
        }
    };

    search(&|i| is_boundary(i) && fits(i))
        .or_else(|| search(&fits))
        .or_else(|| search(&is_boundary))
        .unwrap_or(target)
}

#[repr(u8)]
pub enum BlockUnsafeDegree {
    Ok,
    Overflow,
    ActiveUnderflow,
}

impl BlockUnsafeDegree {
    #[inline(always)]
    pub const fn is_overflow(&self) -> bool {
        matches!(self, BlockUnsafeDegree::Overflow)
    }
}

impl<
        const FAN_OUT: usize,
        const NUM_RECORDS: usize,
        Key: Default + Ord + Copy + Hash + Display + 'static,
        Payload: Clone + Default + 'static,
    > Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{

    #[inline(always)]
    fn lacks_room_for_split_entries(&self) -> bool {
        let (active, dead) = self.active_dead_count();
        (active as usize) + (dead as usize) >= self.overflow_units_count()
    }

    #[inline]
    fn has_protected_garbage(&self, ctx: &TxContext) -> bool {
        self.as_records().iter().any(|r| {
            let version = r.version();
            !version.is_live()
                && !version.insertion_stamp().is_invalid()
                && version
                    .deletion_stamp()
                    .is_some_and(|del| ctx.is_snapshot_live(del.ts_start()))
        })
    }

    #[inline(always)]
    pub fn unsafe_degree(&self, ctx: &TxContext) -> BlockUnsafeDegree {
        let (active, dead) = self.active_dead_count();

        let (active, dead) = (active as usize, dead as usize);

        if self.is_leaf() {
            return if active + dead >= self.overflow_units_count() {
                BlockUnsafeDegree::Overflow
            } else if active + dead <= self.filling_20_percent() {
                BlockUnsafeDegree::ActiveUnderflow
            } else if active <= self.filling_20_percent() && !self.has_protected_garbage(ctx) {
                BlockUnsafeDegree::ActiveUnderflow
            } else {
                BlockUnsafeDegree::Ok
            };
        }

        let one_d = self.filling_20_percent();

        if active <= one_d {
            BlockUnsafeDegree::ActiveUnderflow
        } else {
            let overflow_units_count = self.overflow_units_count();

            let is_overflow = active + dead >= overflow_units_count;

            if is_overflow && active <= one_d * 2 {
                BlockUnsafeDegree::ActiveUnderflow
            } else if is_overflow {
                BlockUnsafeDegree::Overflow
            } else {
                BlockUnsafeDegree::Ok
            }
        }
    }

    #[inline(always)]
    pub fn unsafe_degree_root(&self) -> BlockUnsafeDegree {
        let (active, dead) = self.active_dead_count();

        let (active, dead) = (active as usize, dead as usize);

        let is_leaf = self.is_leaf();

        if active == 1 && !is_leaf {
            // single child
            BlockUnsafeDegree::ActiveUnderflow
        } else if active + dead >= self.overflow_units_count() {
            BlockUnsafeDegree::Overflow
        } else {
            BlockUnsafeDegree::Ok
        }
    }

    #[inline(always)]
    pub fn max_units(&self) -> usize {
        // absolute units
        match self.is_leaf() {
            true => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_records(),
            false => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_keys(),
        }
    }

    #[inline(always)]
    pub fn filling_40_percent(&self) -> usize {
        // 40%
        (2 * self.max_units() + 4) / 5
    }

    #[inline(always)]
    pub fn filling_80_percent(&self) -> usize {
        // 80%
        (4 * self.max_units() + 4) / 5
    }

    #[inline(always)]
    pub fn filling_20_percent(&self) -> usize {
        // 20%
        (self.max_units() + 4) / 5
    }

    #[inline(always)]
    pub fn overflow_units_count(&self) -> usize {
        // trigger for overflow
        match self.is_leaf() {
            true => {
                BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_records_count()
            }
            false => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_keys_count(),
        }
    }

    #[inline(always)]
    pub(crate) fn active_dead_count(&self) -> (u32, u32) {
        match self.as_page_ref() {
            PageType::IndexRef(internal_page) => internal_page.active_dead_count(),
            PageType::LeafRef(leaf_page) => leaf_page.active_dead_count(),
            _ => unreachable!(),
        }
    }

}

pub(crate) enum BlockSplit<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> {
    ByKey(
        Interval<Key>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        Interval<Key>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ),
    ByVersion(BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>),
}

impl<
        const FAN_OUT: usize,
        const NUM_RECORDS: usize,
        Key: Default + Ord + Copy + Hash + Display,
        Payload: Clone + Default,
    > BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload>
{
}

pub(crate) enum MergeResult<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> {
    Merged(
        usize,
        Interval<Key>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ),
    KeySplit(
        usize,
        BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ),
    Error,
}

impl<
        const FAN_OUT: usize,
        const NUM_RECORDS: usize,
        Key: Default + Ord + Copy + Hash + Sync + 'static + Display,
        Payload: Display + Clone + Default + Sync + 'static,
    > MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn on_overflow_node<'a>(
        &self,
        mufasa: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        child_index: usize,
    ) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()> {
        #[cfg(feature = "tpcc-tree-stats")]
        self.smo_stats
            .record(crate::bat_tree::stats::SmoKind::OverflowAttempt);
        if mufasa.lacks_room_for_split_entries() {
            #[cfg(feature = "tpcc-tree-stats")]
            self.smo_stats
                .record(crate::bat_tree::stats::SmoKind::OverflowFailed);
            return Err(());
        }

        let mufasa_deref_mut = mufasa.deref_mut();

        let internal_page = mufasa_deref_mut.as_internal_page();

        let fence = internal_page.get_key(child_index).clone();

        let current_len = internal_page.sum_len();

        if DIAG && format!("{}", fence.upper) == "18446744073709551615" {
            eprintln!(
                "DIAG on_overflow_node ENTER thread={:#x} page={:p} child_index={child_index} current_len={current_len} fence=[{},{}] sum_len_before={}",
                diag_thread_hash(),
                internal_page as *const _,
                fence.lower,
                fence.upper,
                internal_page.sum_len()
            );
        }

        let simba_cell = match simba.try_retire() {
            Ok(cell) => cell,
            Err(..) => {
                #[cfg(feature = "tpcc-tree-stats")]
                self.smo_stats
                    .record(crate::bat_tree::stats::SmoKind::OverflowFailed);
                return Err(());
            }
        };

        #[cfg(feature = "tpcc-tree-stats")]
        let child_is_leaf = simba_cell.deref().is_leaf();

        #[cfg(feature = "tpcc-tree-stats")]
        let completed_kind;
        let version = match self.split(simba_cell.deref(), &fence) {
            BlockSplit::ByKey(left_fence, left, right_fence, right) => {
                #[cfg(feature = "tpcc-tree-stats")]
                {
                    completed_kind = if child_is_leaf {
                        crate::bat_tree::stats::SmoKind::LeafKeySplit
                    } else {
                        crate::bat_tree::stats::SmoKind::InternalKeySplit
                    };
                }
                let version = self.start_tx_commit();

                internal_page.push_uncommitted(left_fence, version, left, current_len);

                internal_page.push_uncommitted(right_fence, version, right, current_len + 1);

                internal_page.commit_delta(1, 1);
                if DIAG && format!("{}", right_fence.upper) == "18446744073709551615" {
                    eprintln!(
                        "DIAG on_overflow_node ByKey thread={:#x} page={:p} child_index={child_index} superseded, pushed left=[{},{}]@{current_len} right=[{},{}]@{}",
                        diag_thread_hash(),
                        internal_page as *const _,
                        left_fence.lower,
                        left_fence.upper,
                        right_fence.lower,
                        right_fence.upper,
                        current_len + 1
                    );
                }
                version
            }
            BlockSplit::ByVersion(fresh) => {
                #[cfg(feature = "tpcc-tree-stats")]
                {
                    completed_kind = if child_is_leaf {
                        crate::bat_tree::stats::SmoKind::LeafVersionSplit
                    } else {
                        crate::bat_tree::stats::SmoKind::InternalVersionSplit
                    };
                }
                let version = self.start_tx_commit();

                internal_page.push_uncommitted(fence, version, fresh, current_len);

                internal_page.commit_delta(0, 1);
                if DIAG && format!("{}", fence.upper) == "18446744073709551615" {
                    eprintln!(
                        "DIAG on_overflow_node ByVersion thread={:#x} page={:p} child_index={child_index} superseded, pushed fence=[{},{}]@{current_len}",
                        diag_thread_hash(),
                        internal_page as *const _,
                        fence.lower,
                        fence.upper
                    );
                }
                version
            }
        };

        self.block_manager
            .register_dead(self.worker_id(), version, simba_cell);
        #[cfg(feature = "tpcc-tree-stats")]
        self.smo_stats.record(completed_kind);
        Ok(mufasa)
    }

    pub(crate) fn on_underflow_node<'a>(
        &self,
        mufasa: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
        index_simba: usize,
    ) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()> {
        #[cfg(feature = "tpcc-tree-stats")]
        self.smo_stats
            .record(crate::bat_tree::stats::SmoKind::UnderflowAttempt);
        if VERBOSE {
            println!("on_underflow_node");
        }

        if mufasa.lacks_room_for_split_entries() {
            #[cfg(feature = "tpcc-tree-stats")]
            self.smo_stats
                .record(crate::bat_tree::stats::SmoKind::UnderflowFailed);
            return Err(());
        }

        let mufasa_deref_mut = mufasa.deref_mut();

        let simba_cell = match simba.try_retire() {
            Ok(cell) => cell,
            Err(..) => {
                #[cfg(feature = "tpcc-tree-stats")]
                self.smo_stats
                    .record(crate::bat_tree::stats::SmoKind::UnderflowFailed);
                return Err(());
            }
        };

        #[cfg(feature = "tpcc-tree-stats")]
        let child_is_leaf = simba_cell.deref().is_leaf();
        #[cfg(feature = "tpcc-tree-stats")]
        let completed_kind;

        match self.merge(mufasa_deref_mut, simba_cell.deref(), index_simba) {
            MergeResult::Merged(index_sibling, fence_sibling, merged_block, candidate_cell) => {
                #[cfg(feature = "tpcc-tree-stats")]
                {
                    completed_kind = if child_is_leaf {
                        crate::bat_tree::stats::SmoKind::LeafMerge
                    } else {
                        crate::bat_tree::stats::SmoKind::InternalMerge
                    };
                }
                if VERBOSE {
                    println!(
                        "MergeResult::Merged: Simba-fence: {} - Sibling-fence: {}",
                        mufasa_deref_mut.as_internal_page_ref().get_key(index_simba),
                        fence_sibling
                    );
                }
                let mufasa_internal_page = mufasa_deref_mut.as_internal_page();

                let mufasa_len = mufasa_internal_page.sum_len();

                let mut merged_fence = mufasa_internal_page.get_key(index_simba).clone();

                merged_fence.merged(&fence_sibling);

                let version = self.start_tx_commit();

                mufasa_internal_page.push_uncommitted(
                    merged_fence,
                    version,
                    merged_block,
                    mufasa_len,
                );

                mufasa_internal_page.commit_delta(-1, 2);

                if DIAG && format!("{}", merged_fence.upper) == "18446744073709551615" {
                    eprintln!(
                        "DIAG on_underflow_node Merged thread={:#x} page={:p} index_simba={index_simba} index_sibling={index_sibling} superseded, pushed merged=[{},{}]@{mufasa_len}",
                        diag_thread_hash(),
                        mufasa_internal_page as *const _,
                        merged_fence.lower,
                        merged_fence.upper
                    );
                }

                self.block_manager.register_dead_col(
                    self.worker_id(),
                    [(version, simba_cell), (version, candidate_cell)],
                )
            }
            MergeResult::KeySplit(
                index_sibling,
                BlockSplit::ByKey(left_interval, left, right_interval, right),
                candidate_cell,
            ) => {
                #[cfg(feature = "tpcc-tree-stats")]
                {
                    completed_kind = if child_is_leaf {
                        crate::bat_tree::stats::SmoKind::LeafMergeKeySplit
                    } else {
                        crate::bat_tree::stats::SmoKind::InternalMergeKeySplit
                    };
                }
                if VERBOSE {
                    unsafe {
                        println!(
                            "MergeResult::KeySplit: \
                       \tleft-fence: {}, \
                       \tright-fence: {}.\
                        \n\tSimba-fence: {} - Sibling-fence: {}\n\
                        \tsimba:\n{}",
                            left_interval,
                            right_interval,
                            mufasa_deref_mut.keys().get_unchecked(index_simba),
                            mufasa_deref_mut.keys().get_unchecked(index_sibling),
                            simba_cell.deref().node_data.as_ref()
                        );
                    }
                }
                let mufasa_internal_page = mufasa_deref_mut.as_internal_page();

                let mufasa_len = mufasa_internal_page.sum_len();

                let version = self.start_tx_commit();

                mufasa_internal_page.push_uncommitted(left_interval, version, left, mufasa_len);

                mufasa_internal_page.push_uncommitted(
                    right_interval,
                    version,
                    right,
                    mufasa_len + 1,
                );

                mufasa_internal_page.commit_delta(0, 2);

                if DIAG && format!("{}", right_interval.upper) == "18446744073709551615" {
                    eprintln!(
                        "DIAG on_underflow_node KeySplit thread={:#x} page={:p} index_simba={index_simba} index_sibling={index_sibling} superseded, pushed left=[{},{}]@{mufasa_len} right=[{},{}]@{}",
                        diag_thread_hash(),
                        mufasa_internal_page as *const _,
                        left_interval.lower,
                        left_interval.upper,
                        right_interval.lower,
                        right_interval.upper,
                        mufasa_len + 1
                    );
                }

                // See the `Merged` arm's matching comment.
                self.block_manager.register_dead_col(
                    self.worker_id(),
                    [(version, simba_cell), (version, candidate_cell)],
                )
            }
            _ => {
                simba_cell.clear_retired();
                #[cfg(feature = "tpcc-tree-stats")]
                self.smo_stats
                    .record(crate::bat_tree::stats::SmoKind::UnderflowFailed);
                return Err(());
            }
        }

        #[cfg(feature = "tpcc-tree-stats")]
        self.smo_stats.record(completed_kind);
        Ok(mufasa)
    }

    /// Whether a record must be carried forward by a version-split/merge's
    /// GC compaction — `is_live()` plus one more case: a record that's
    /// *deleted* (not invalidated) by a transaction whose `ts_start` is
    /// still registered as in-flight (`TxContext::is_snapshot_live`) is not
    /// safe to discard yet, even though `is_live()` already reports it
    /// dead. `is_deleted()`/`is_live()` are pure local bookkeeping with no
    /// notion of commit status — OSIC's "instant commit" model means
    /// `delete()` marks a record dead the moment `DbTransaction::update`/
    /// `delete` calls it, well before (and regardless of whether) that
    /// transaction ever actually commits. If it aborts instead (dropped
    /// without `commit()` — e.g. New-Order's own ~1%-of-transactions
    /// business-logic rollback, `bat_bench::tpcc_txn::new_order`, which can
    /// abort *after* having already updated `District`), reversing that
    /// delete (`LeafPage::apply_invalidate`/`apply_undelete`) requires the
    /// predecessor record to still be physically present to undelete. A
    /// version-split/merge racing in between — reading `is_deleted()` as
    /// `true` and discarding the record, exactly as it should for a
    /// *committed* delete — makes that reversal silently impossible
    /// instead, permanently losing the key: confirmed empirically as the
    /// root cause of `bat_bench::tpcc_txn`'s District
    /// `ZeroAffected(KeyDoesNotExist)` panic under `gc=on` at high thread
    /// counts.
    ///
    /// Deliberately does *not* extend the same protection to an invalidated
    /// `insert_stamp` (an aborted `Insert`/the new half of an `Update`):
    /// invalidation only ever happens as part of that same abort-reversal
    /// (never speculatively ahead of it, unlike a plain `delete()`), so by
    /// the time a record is invalid its owning transaction has already
    /// fully resolved — there's no future "undo" that still needs it kept
    /// around.
    ///
    /// Other readers do not require their visible predecessor to be copied
    /// into a replacement page. A reader older than the split/merge routes
    /// through the retained source block, whose replacement birth version
    /// is also its death version; `live_min_snapshot` prevents that block's
    /// reuse until the reader finishes. If the deleting transaction is
    /// still unresolved when the SMO runs, the check below copies the
    /// predecessor for abort safety and for readers born against that new
    /// structural version. Once deletion commits, future readers see it.
    #[inline]
    #[cfg(not(feature = "lightweight-gc"))]
    /// Precise GC: keep record if its deletion's snapshot ID is one of the
    /// currently-active snapshots. Uses pre-computed HashSet for O(1) lookups.
    /// Avoids keeping unnecessarily old deletions.
    pub(crate) fn record_survives_gc(
        &self,
        version: &crate::bat_record_model::version_info::VersionInfo,
        live_snapshots: &LiveSnapshots,
    ) -> bool {
        if version.is_live() {
            return true;
        }

        !version.insertion_stamp().is_invalid()
            && version
                .deletion_stamp()
                .is_some_and(|del| live_snapshots.contains(&del.ts_start()))
    }

    #[cfg(feature = "lightweight-gc")]
    /// Lightweight GC: keep record if its deletion happened at or after the
    /// oldest live snapshot (conservative but O(1): just compares against min).
    /// May keep some dead records but avoids HashSet collection overhead.
    pub(crate) fn record_survives_gc(
        &self,
        version: &crate::bat_record_model::version_info::VersionInfo,
        live_min_snapshot: &LiveSnapshots,
    ) -> bool {
        if version.is_live() {
            return true;
        }

        !version.insertion_stamp().is_invalid()
            && version
                .deletion_stamp()
                .is_some_and(|del| live_min_snapshot.is_some_and(|min| del.ts_start() >= min))
    }

    /// Collect all currently-live transaction snapshot IDs for efficient GC filtering.
    #[cfg(not(feature = "lightweight-gc"))]
    fn live_snapshots(&self) -> LiveSnapshots {
        self.ctx.live_snapshots_set()
    }

    /// Lightweight GC only needs the oldest snapshot, computed once per SMO
    /// batch rather than once for every record in that batch.
    #[cfg(feature = "lightweight-gc")]
    fn live_snapshots(&self) -> LiveSnapshots {
        self.ctx.live_min_snapshot()
    }

    /// Returns borrowed views of all records retained from this leaf
    /// generation, in physical order. The replacement page clones each view
    /// directly into its final slot; no owned record staging buffer is built.
    fn retained_leaf_history<'a>(
        &self,
        block: &'a Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ) -> Vec<LeafRecordRef<'a, Key, Payload>> {
        let live_snapshots = self.live_snapshots();
        self.retained_leaf_history_with(block, &live_snapshots)
    }

    /// As `retained_leaf_history`, using a snapshot view already
    /// collected by a multi-page operation such as merge.
    fn retained_leaf_history_with<'a>(
        &self,
        block: &'a Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        live_snapshots: &LiveSnapshots,
    ) -> Vec<LeafRecordRef<'a, Key, Payload>> {
        let mut history = Vec::with_capacity(block.as_records().len());
        for record in block.as_records().iter() {
            if self.record_survives_gc(record.version(), live_snapshots) {
                history.push(record);
            }
        }
        history
    }

    fn populate_leaf_history(
        &self,
        page: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        mut records: Vec<LeafRecordRef<'_, Key, Payload>>,
    ) -> usize {
        records.sort_by_key(|record| record.key());
        self.push_records_onto(page, &records)
    }

    fn push_records_onto(
        &self,
        page: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        records: &[LeafRecordRef<'_, Key, Payload>],
    ) -> usize {
        let count = records.len();
        let leaf = page.unsafe_borrow_mut().as_leaf_page();
        let zone_map = leaf.bulk_push_from_slice_ref_projected(
            records,
            self.cold.zone_map_projection.get().copied(),
        );
        leaf.seed_zone_map(zone_map);
        count
    }

    pub(crate) fn merge<'a>(
        &self,
        mufasa: &'a Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: &Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba_index: usize,
    ) -> MergeResult<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mufasa_internal_page = mufasa.as_internal_page_ref();

        let is_simba_leaf = simba.is_leaf();

        let simba_fence = mufasa_internal_page.get_key(simba_index);

        let simba_max_units = simba.max_units();

        let (simba_active_count, _simba_dead_count) = simba.active_dead_count();

        let (simba_active_count, _simba_dead_count) =
            (simba_active_count as usize, _simba_dead_count as usize);

        let mufasa_children = mufasa_internal_page.children();

        let mut all_candidates = mufasa_children
            .iter()
            .enumerate()
            .zip(mufasa_internal_page.versions())
            .zip(mufasa_internal_page.keys())
            .filter(|(((index, ..), ..), ..)| *index != simba_index)
            .filter(|(((index, ..), ..), ..)| mufasa_internal_page.is_slot_live(*index))
            .sorted_by_key(|(.., fence)| fence.lower())
            .map(|(((index, bro), ..), fence)| (index, bro, fence))
            .collect_vec();

        let mut compute_candidate = || {
            let insertion_point =
                match all_candidates.binary_search_by_key(&simba_fence.lower, |(.., f)| f.lower) {
                    Ok(index) => return Ok(all_candidates.remove(index)),
                    Err(index) => index,
                };

            let right = (insertion_point < all_candidates.len()).then_some(insertion_point);
            let left = insertion_point.checked_sub(1);

            let chosen = match (left, right) {
                (Some(l), Some(r)) => {
                    let (l_active, l_dead) = all_candidates[l].1.active_dead_count();
                    let (r_active, r_dead) = all_candidates[r].1.active_dead_count();
                    if l_active as usize + l_dead as usize <= r_active as usize + r_dead as usize {
                        l
                    } else {
                        r
                    }
                }
                (Some(l), None) => l,
                (None, Some(r)) => r,
                (None, None) => return Err(()),
            };

            Ok(all_candidates.remove(chosen))
        };

        let (candidate_index, candidate_block, candidate_fence) = match compute_candidate() {
            Ok(triple) => triple,
            Err(()) => return MergeResult::Error,
        };

        let candidate_cell = match candidate_block.borrow_read().try_retire() {
            Ok(cell) => cell,
            Err(..) => return MergeResult::Error,
        };

        all_candidates.clear();

        let (candidate_active_count, _candidate_dead_count) =
            candidate_cell.deref().active_dead_count();

        let candidate_active_count = candidate_active_count as usize;

        let retained_leaf_histories = is_simba_leaf.then(|| {
            let live_snapshots = self.live_snapshots();
            let mut records = self.retained_leaf_history_with(simba, &live_snapshots);
            records
                .extend(self.retained_leaf_history_with(candidate_cell.deref(), &live_snapshots));
            records
        });
        let leaf_merge_would_overflow = retained_leaf_histories
            .as_ref()
            .is_some_and(|records| records.len() > simba_max_units);

        if !leaf_merge_would_overflow
            && candidate_active_count + simba_active_count <= ((4 * simba_max_units) / 5)
        {
            // <= 80% ok merge
            let combined_block = match is_simba_leaf {
                false => {
                    let combined_block = self.block_manager.new_empty_index_block(&self.ctx);

                    let (keys, versions, pointers) =
                        simba.as_internal_page_ref().keys_versions_pointers();

                    let simba_internal = simba.as_internal_page_ref();

                    let (c_keys, c_versions, c_pointers) = candidate_cell
                        .deref()
                        .as_internal_page_ref()
                        .keys_versions_pointers();

                    let candidate_internal = candidate_cell.deref().as_internal_page_ref();

                    let shadow_copy = keys
                        .iter()
                        .zip(versions.iter().copied())
                        .zip(pointers.iter())
                        .enumerate()
                        .filter(|(index, ..)| simba_internal.is_slot_live(*index))
                        .map(|(_, rest)| rest)
                        .merge_by(
                            c_keys
                                .iter()
                                .zip(c_versions.iter().copied())
                                .zip(c_pointers.iter())
                                .enumerate()
                                .filter(|(index, ..)| candidate_internal.is_slot_live(*index))
                                .map(|(_, rest)| rest),
                            |((.., v0), ..), ((.., v1), ..)| v0 <= v1,
                        )
                        .collect_vec();

                    combined_block
                        .unsafe_borrow_mut()
                        .as_internal_page()
                        .bulk_push(shadow_copy);

                    combined_block
                }
                true => {
                    let combined_block = self.block_manager.new_empty_leaf(&self.ctx);

                    self.populate_leaf_history(
                        combined_block,
                        retained_leaf_histories.expect("leaf histories"),
                    );

                    if TRACE_KEY_DEBUG {
                        push_trace(format!(
                            "TRACE merge::Merged(leaf) thread={:#x} simba={:p} simba_fence={} simba_live={} candidate={:p} candidate_fence={} candidate_live={} -> combined={:p} combined_live={}",
                            diag_thread_hash(),
                            simba,
                            simba_fence,
                            simba
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            candidate_cell.deref(),
                            candidate_fence,
                            candidate_cell
                                .deref()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            combined_block.unsafe_borrow(),
                            combined_block
                                .unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(",")
                        ));
                    }

                    combined_block
                }
            };

            MergeResult::Merged(
                candidate_index,
                candidate_fence.clone(),
                combined_block,
                candidate_cell,
            )
        } else {
            // Keysplit when merged: > 80% active entries ---> redistribute the keys
            match is_simba_leaf {
                true => {
                    let candidate_records = candidate_cell.deref().as_records();
                    let simba_records = simba.as_records();
                    let mut records = retained_leaf_histories.expect("leaf histories");
                    records.sort_by_key(|r| r.key());
                    let middle =
                        nearest_key_boundary(&records, records.len() / 2, simba_max_units, |r| {
                            r.key()
                        });
                    let split_key = records[middle].key();

                    let left_interval = Interval::new(
                        candidate_fence.lower.min(simba_fence.lower),
                        (self.cold.dec_key)(split_key),
                    );

                    let right_interval =
                        Interval::new(split_key, candidate_fence.upper.max(simba_fence.upper));

                    let combined_block_0 = self.block_manager.new_empty_leaf(&self.ctx);

                    let combined_block_1 = self.block_manager.new_empty_leaf(&self.ctx);

                    self.push_records_onto(combined_block_0, &records[..middle]);
                    self.push_records_onto(combined_block_1, &records[middle..]);

                    if TRACE_KEY_DEBUG {
                        push_trace(format!(
                            "TRACE merge::KeySplit(leaf) thread={:#x} simba={:p} simba_fence={} simba_live={} candidate={:p} candidate_fence={} candidate_live={} -> left={:p} left_fence={} left_live={} right={:p} right_fence={} right_live={}",
                            diag_thread_hash(),
                            simba,
                            simba_fence,
                            simba_records
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            candidate_cell.deref(),
                            candidate_fence,
                            candidate_records
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            combined_block_0.unsafe_borrow(),
                            left_interval,
                            combined_block_0
                                .unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            combined_block_1.unsafe_borrow(),
                            right_interval,
                            combined_block_1
                                .unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(",")
                        ));
                    }

                    drop(records);
                    MergeResult::KeySplit(
                        candidate_index,
                        BlockSplit::ByKey(
                            left_interval,
                            combined_block_0,
                            right_interval,
                            combined_block_1,
                        ),
                        candidate_cell,
                    )
                }
                false => unsafe {
                    let candidate_internal_page = candidate_cell.deref().as_internal_page_ref();

                    let (c_keys, c_versions, c_children) =
                        candidate_internal_page.keys_versions_pointers();

                    let (s_keys, s_version, s_children) = simba.keys_versions_pointers();

                    let simba_internal = simba.as_internal_page_ref();

                    let mut joined = c_keys
                        .iter()
                        .zip(c_versions.iter().copied())
                        .zip(c_children.iter())
                        .enumerate()
                        .filter(|(index, ..)| candidate_internal_page.is_slot_live(*index))
                        .map(|(_, rest)| rest)
                        .sorted_by_key(|((k, ..), ..)| k.lower)
                        .merge_by(
                            s_keys
                                .iter()
                                .zip(s_version.iter().copied())
                                .zip(s_children.iter())
                                .enumerate()
                                .filter(|(index, ..)| simba_internal.is_slot_live(*index))
                                .map(|(_, rest)| rest)
                                .sorted_by_key(|((k, ..), ..)| k.lower),
                            |((f, ..), ..), ((s, ..), ..)| f.lower < s.lower,
                        )
                        .collect_vec();

                    let joined_len = joined.len();
                    let (first, second) = joined.split_at_mut(joined_len / 2);

                    let left_fence = Interval::new(
                        candidate_fence.lower.min(simba_fence.lower),
                        (self.cold.dec_key)(second.get_unchecked(0).0 .0.lower),
                    );

                    let right_fence = Interval::new(
                        second.get_unchecked(0).0 .0.lower,
                        candidate_fence.upper.max(simba_fence.upper),
                    );

                    first.sort_by_key(|((.., v), ..)| *v);
                    second.sort_by_key(|((.., v), ..)| *v);

                    let combined_block_0 = self.block_manager.new_empty_index_block(&self.ctx);

                    let combined_block_1 = self.block_manager.new_empty_index_block(&self.ctx);

                    combined_block_0
                        .unsafe_borrow_mut()
                        .as_internal_page()
                        .bulk_push_from_slice(first);

                    combined_block_1
                        .unsafe_borrow_mut()
                        .as_internal_page()
                        .bulk_push_from_slice(second);

                    MergeResult::KeySplit(
                        candidate_index,
                        BlockSplit::ByKey(
                            left_fence,
                            combined_block_0,
                            right_fence,
                            combined_block_1,
                        ),
                        candidate_cell,
                    )
                },
            }
        }
    }

    pub(crate) fn split(
        &self,
        block: &Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        fence: &Interval<Key>,
    ) -> BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let is_leaf = block.is_leaf();

        let (active_block, _dead_block) = block.active_dead_count();

        let retained_history = is_leaf.then(|| self.retained_leaf_history(block));
        let survivor_count = match &retained_history {
            Some(records) => records.len(),
            None => block.as_internal_page_ref().live_count(),
        };

        let capacity = if is_leaf { NUM_RECORDS } else { FAN_OUT };

        if active_block as usize >= block.filling_80_percent() || survivor_count >= capacity {
            // KEY_SPLIT
            match is_leaf {
                true => unsafe {
                    // LeafPage
                    if VERBOSE {
                        println!(
                            "Key Split: Leaf\n{}",
                            block.as_records().iter().join("\n\t")
                        );
                    }
                    let block_records = block.as_records();
                    let mut records = retained_history.expect("leaf history");
                    records.sort_by_key(|r| r.key());
                    let middle =
                        nearest_key_boundary(&records, records.len() / 2, capacity, |r| r.key());
                    let split_key = records[middle].key();

                    let (left, right) = (
                        self.block_manager.new_empty_leaf(&self.ctx),
                        self.block_manager.new_empty_leaf(&self.ctx),
                    );

                    let fence_left = Interval::new(fence.lower, (self.cold.dec_key)(split_key));

                    let fence_right = Interval::new(split_key, fence.upper);
                    self.push_records_onto(left, &records[..middle]);
                    self.push_records_onto(right, &records[middle..]);

                    if TRACE_KEY_DEBUG {
                        push_trace(format!(
                            "TRACE split::ByKey(leaf) thread={:#x} old={:p} old_fence={} old_live={} -> left={:p} left_fence={} left_live={} right={:p} right_fence={} right_live={}",
                            diag_thread_hash(),
                            block,
                            fence,
                            block_records
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            left.unsafe_borrow(),
                            fence_left,
                            left.unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            right.unsafe_borrow(),
                            fence_right,
                            right
                                .unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(",")
                        ));
                    }

                    BlockSplit::ByKey(fence_left, left, fence_right, right)
                },
                false => unsafe {
                    // KEY_SPLIT InternalPage
                    if VERBOSE {
                        println!("Key Split: Internal");
                    }
                    let (left, right) = (
                        self.block_manager.new_empty_index_block(&self.ctx),
                        self.block_manager.new_empty_index_block(&self.ctx),
                    );

                    let (key_intervals, versions, pointers) = block.keys_versions_pointers();

                    let internal = block.as_internal_page_ref();

                    let mut filtered = key_intervals
                        .iter()
                        .zip(versions.iter().copied())
                        .zip(pointers.iter())
                        .enumerate()
                        .filter(|(index, ..)| internal.is_slot_live(*index))
                        .map(|(_, rest)| rest)
                        .sorted_by_key(|((i, ..), ..)| i.lower)
                        .collect_vec();

                    let middle = filtered.len() / 2;
                    let (first, second) = filtered.split_at_mut(middle);

                    debug_assert!(!first.is_empty() && !second.is_empty());

                    let fence_left = Interval::new(
                        fence.lower,
                        (self.cold.dec_key)(second.get_unchecked(0).0 .0.lower),
                    );

                    if let PageType::IndexMut(internal_page) =
                        left.unsafe_borrow_mut().as_page_mut()
                    {
                        first.sort_by_key(|((.., v), ..)| *v);
                        internal_page.bulk_push_from_slice(first)
                    }

                    let fence_right =
                        Interval::new(second.get_unchecked(0).0 .0.lower, fence.upper);

                    if let PageType::IndexMut(internal_page) =
                        right.unsafe_borrow_mut().as_page_mut()
                    {
                        second.sort_by_key(|((.., v), ..)| *v);
                        internal_page.bulk_push_from_slice(second)
                    }

                    BlockSplit::ByKey(fence_left, left, fence_right, right)
                },
            }
        } else {
            // < max_units_safe. meaning: active >= 40% and active < 80%
            // VERSION SPLIT
            match is_leaf {
                true => {
                    // LeafPage
                    if VERBOSE {
                        println!("Version Split: Leaf");
                    }
                    let new_leaf = self.block_manager.new_empty_leaf(&self.ctx);

                    let block_records = block.as_records();
                    let records = retained_history.expect("leaf history");
                    // debug_assert!(active_records.len() >= block.filling_40_percent(),
                    //               "Active records = {}, required >= {}", active_records.len(), block.filling_40_percent());

                    // debug_assert!(active_records.len() <=
                    //     BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_records());

                    let record_count = self.populate_leaf_history(new_leaf, records);
                    record_version_split(fence.to_string(), record_count);

                    if TRACE_KEY_DEBUG {
                        push_trace(format!(
                            "TRACE split::ByVersion(leaf) thread={:#x} old={:p} fence={} old_live={} -> new={:p} new_live={}",
                            diag_thread_hash(),
                            block,
                            fence,
                            block_records
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            new_leaf.unsafe_borrow(),
                            new_leaf
                                .unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(",")
                        ));
                    }

                    BlockSplit::ByVersion(new_leaf)
                }
                false => {
                    // VERSION SPLIT InternalPage
                    if VERBOSE {
                        println!("Version Split: Internal");
                    }
                    let new_internal_page = self.block_manager.new_empty_index_block(&self.ctx);

                    let (key_intervals, versions, pointers) = block.keys_versions_pointers();

                    let internal = block.as_internal_page_ref();

                    let active_entries = key_intervals
                        .iter()
                        .zip(versions.iter().copied())
                        .zip(pointers.iter())
                        .enumerate()
                        .filter(|(index, ..)| internal.is_slot_live(*index))
                        .map(|(_, rest)| rest)
                        .collect_vec();

                    if VERBOSE {
                        let key_intervals = active_entries
                            .iter()
                            .map(|((k, ..), ..)| (k.lower, k.upper))
                            .sorted_by_key(|i| i.0)
                            .collect_vec();

                        if !key_intervals
                            .iter()
                            .zip(key_intervals.iter().skip(1))
                            .all(|((k0, k1), (k2, k3))| (self.cold.dec_key)(*k2) == *k1)
                        {
                            let s = "sdasdasdasdasln".to_string();
                        }
                    }

                    // RootSplit calls this too! Root may run under conditioned 2d
                    // debug_assert!(active_entries.len() >= block.two_d_filling(),
                    //               "Active entries = {}, required >= {}", active_entries.len(), block.two_d_filling());
                    if let PageType::IndexMut(internal_page) =
                        new_internal_page.unsafe_borrow_mut().as_page_mut()
                    {
                        internal_page.bulk_push(active_entries)
                    }

                    BlockSplit::ByVersion(new_internal_page)
                }
            }
        }
    }

    #[inline]
    pub(crate) fn merge_root<'a>(
        &self,
        master_guard: RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        root_guard: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        height: Height,
    ) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()> {
        #[cfg(feature = "tpcc-tree-stats")]
        self.smo_stats
            .record(crate::bat_tree::stats::SmoKind::RootMergeAttempt);
        if VERBOSE {
            println!("merge root");
        }

        let child_ref = root_guard.as_internal_page_ref().last_child();

        let child_guard = child_ref.borrow_read();

        if VERBOSE {
            println!("Old root height = {}, new height = {}", height, height - 1);
        }

        let guard = match self.replace_root(master_guard, child_guard, height - 1, true) {
            Ok(guard) => guard,
            Err(()) => {
                #[cfg(feature = "tpcc-tree-stats")]
                self.smo_stats
                    .record(crate::bat_tree::stats::SmoKind::RootMergeFailed);
                return Err(());
            }
        };

        #[cfg(feature = "tpcc-tree-stats")]
        self.smo_stats
            .record(crate::bat_tree::stats::SmoKind::RootMerge);

        if VERBOSE {
            let guard_deref = guard.deref_mut();

            let (active, dead) = guard_deref.active_dead_count();

            println!("active dead count: ({} / {})", active, dead);
        }

        Ok(guard)
    }

    #[inline]
    pub(crate) fn split_root<'a>(
        &self,
        master_guard: RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        root_guard: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        height: Height,
    ) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()> {
        #[cfg(feature = "tpcc-tree-stats")]
        self.smo_stats
            .record(crate::bat_tree::stats::SmoKind::RootSplitAttempt);

        let result = self.replace_root(master_guard, root_guard, height, false);
        #[cfg(feature = "tpcc-tree-stats")]
        if result.is_err() {
            self.smo_stats
                .record(crate::bat_tree::stats::SmoKind::RootSplitFailed);
        }
        result
    }

    #[inline]
    fn replace_root<'a>(
        &self,
        _master_guard: RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        root_guard: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        height: Height,
        _is_root_merge: bool,
    ) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()> {
        let root_cell = match root_guard.try_retire() {
            Ok(cell) => cell,
            Err(_) => {
                if VERBOSE {
                    println!("split_root: root_guard.try_retire() failed");
                }
                return Err(());
            }
        };

        match self.split(
            root_cell.deref(),
            &Interval::new(self.cold.min_key, self.cold.max_key),
        ) {
            BlockSplit::ByKey(left_fence, left, right_fence, right) => {
                let new_root_block = self.block_manager.new_empty_index_block(&self.ctx);

                let root_internal_page = new_root_block
                    .unsafe_borrow_mut()
                    .as_mut()
                    .as_internal_page();

                let version = self.start_tx_commit();

                root_internal_page.push_uncommitted(left_fence, version, left, 0);

                root_internal_page.push_uncommitted(right_fence, version, right, 1);

                root_internal_page.commit_delta(2, 0);

                let new_root_latch = new_root_block.borrow_read();

                self.root
                    .append_root(Root::new(new_root_block, version, height + 1));

                self.block_manager
                    .register_dead(self.worker_id(), version, root_cell);

                #[cfg(feature = "tpcc-tree-stats")]
                if !_is_root_merge {
                    self.smo_stats
                        .record(crate::bat_tree::stats::SmoKind::RootKeySplit);
                }

                Ok(new_root_latch)
            }
            BlockSplit::ByVersion(new_root_block) => {
                let version = self.start_tx_commit();

                let new_root_latch = new_root_block.borrow_read();

                self.root
                    .append_root(Root::new(new_root_block, version, height));

                self.block_manager
                    .register_dead(self.worker_id(), version, root_cell);

                #[cfg(feature = "tpcc-tree-stats")]
                if !_is_root_merge {
                    self.smo_stats
                        .record(crate::bat_tree::stats::SmoKind::RootVersionSplit);
                }

                Ok(new_root_latch)
            }
        }
    }
}
