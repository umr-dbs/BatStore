use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use triomphe::Arc;
use crate::mv_gc::block_tracer::{DeadPageValue, BlockTrace};
use crate::mv_page_model::BlockRef;
use crate::mv_page_model::time_matcher::TimeMatcher;
use crate::mv_record_model::tx_stamp::WorkerId;
use crate::mv_record_model::version_info::Version;
use crate::mv_sync::tx_context::TxContext;

pub type TrackerHandle<
    const P_F: usize,
    const P_N: usize,
    Key,
    Payload> = Arc<TrackerHandleSt<P_F, P_N, Key, Payload>>;

/// Always present on every tree, one per `BlockAllocManager` — page/block
/// reclaim (`dead_blocks`/`register_died_page*`/`free_block`), gated by its
/// own `block_reclaim_enabled` flag (`MVBTSt::enable_gc`/`disable_gc`).
/// Genuinely per-table: dead pages are physically owned by this table's own
/// `BlockAllocManager`, so reclaiming them is never a cross-table concern.
///
/// Active-snapshot tracking used to live here too, but it isn't a per-table
/// concept once several tables can share one transactional core (see
/// `crate::mv_sync::tx_context::TxContext`'s doc): it moved there, since a
/// snapshot registered once by a cross-table transaction may go on to read
/// *any* table sharing that context, not just whichever one happened to
/// register it. `free_block` below reads that shared state (via a `&TxContext`
/// parameter) rather than a per-table copy — see its doc for why that's
/// required for soundness, not just a rename.
pub struct TrackerHandleSt<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static>
{
    dead_blocks: BlockTrace<P_F, P_N, Key, Payload>,
    /// Explicit opt-in for block reclaim (`MVBTSt::enable_gc`/`disable_gc`).
    /// `false` by default: a fresh tree never reuses blocks until this is
    /// turned on, matching the pre-existing behavior from when the whole
    /// tracker was optional.
    block_reclaim_enabled: AtomicBool,
}

impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default> TrackerHandleSt<P_F, P_N, Key, Payload>
{
    pub fn new() -> Self {
        Self {
            dead_blocks: BlockTrace::new(),
            block_reclaim_enabled: AtomicBool::new(false),
        }
    }

    /// See `block_reclaim_enabled`'s field doc.
    #[inline]
    pub fn set_block_reclaim_enabled(&self, enabled: bool) {
        self.block_reclaim_enabled.store(enabled, Relaxed);
    }

    /// See `block_reclaim_enabled`'s field doc.
    #[inline]
    pub fn block_reclaim_enabled(&self) -> bool {
        self.block_reclaim_enabled.load(Relaxed)
    }

    /// Always marks `page` retired (see `OptCell::retired`'s doc) — a
    /// correctness fix independent of GC, so it applies whether or not block
    /// reclaim is on. The actual `dead_blocks` bookkeeping stays gated
    /// behind `block_reclaim_enabled` as before — otherwise it would just
    /// grow forever recording pages nothing will ever come collect.
    #[inline]
    pub fn register_died_page(&self, worker_id: WorkerId, page_version: Version, page: DeadPageValue<P_F, P_N, Key, Payload>) {
        page.mark_retired();

        if self.block_reclaim_enabled.load(Relaxed) {
            self.dead_blocks.register_died_page(worker_id, page_version, page)
        }
    }

    #[inline]
    pub fn register_died_page_col(&self, worker_id: WorkerId, dead_pages: [(Version, BlockRef<P_F, P_N, Key, Payload>); 2]) {
        dead_pages.iter().for_each(|(_, page)| page.mark_retired());

        if self.block_reclaim_enabled.load(Relaxed) {
            self.dead_blocks.register_died_page_col(worker_id, dead_pages)
        }
    }

    /// Reclaims one dead page safe to reuse, or `None` if reclaim is off or
    /// nothing qualifies yet. Takes `ctx` — the (possibly shared)
    /// transactional core this table's tree belongs to — because the "safe
    /// to reclaim" bound is a property of *every* active snapshot across
    /// every table sharing `ctx`, not just this table's own readers: a
    /// cross-table transaction registers its snapshot once, before
    /// necessarily having touched this specific table yet, so this table's
    /// reclaim must still respect it.
    #[inline]
    pub fn free_block(&self, ctx: &TxContext) -> Option<BlockRef<P_F, P_N, Key, Payload>> {
        if !self.block_reclaim_enabled.load(Relaxed) {
            return None;
        }

        // `live_min_snapshot` already folds in any worker mid-registration
        // (drawn a ts_start, not yet recorded in `live_tx` — might need
        // exactly the block we're about to hand out) alongside fully-active
        // transactions — see `TxContext::in_flight_bound`'s doc. No separate
        // "wait until nothing anywhere is mid-registration" check needed.
        let live_min_snapshot = ctx.live_min_snapshot();

        self.dead_blocks.try_reclaim(|dead_v| match live_min_snapshot {
            None => true,
            Some(live_min_snapshot) => dead_v.lt_self_any(live_min_snapshot),
        })
    }
}
