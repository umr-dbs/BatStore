use crate::bat_gc::block_tracer::{BlockTrace, DeadPageValue};
use crate::bat_page_model::BlockRef;
use crate::bat_page_model::time_matcher::TimeMatcher;
use crate::bat_record_model::tx_stamp::WorkerId;
use crate::bat_record_model::version_info::Version;
use crate::bat_sync::tx_context::TxContext;
#[cfg(feature = "gc-stats")]
use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use std::fmt::Display;
use std::hash::Hash;
#[cfg(feature = "gc-stats")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use triomphe::Arc;

// Preserve the source of prefetched pages until they actually satisfy a request.
// Normal builds retain the original cache layout and incur no tagging overhead.
#[cfg(feature = "gc-stats")]
type ReusableBlock<const F: usize, const N: usize, K, V> = (BlockRef<F, N, K, V>, bool);
#[cfg(not(feature = "gc-stats"))]
type ReusableBlock<const F: usize, const N: usize, K, V> = BlockRef<F, N, K, V>;

/// Per-shard (see `BlockTrace`'s doc — same `num_cpus`-sized, `worker_id %
/// shard_count`-indexed sharding) breakdown of where a reclaimed/allocated
/// block came from, read back by `TrackerHandleSt::gc_stats_per_shard` for
/// H6-style GC-cost benchmarking. `CachePadded` for the same false-sharing
/// reason as `TxContext::live_tx` — see that field's doc.
///
/// Only ever populated behind the `gc-stats` Cargo feature (see its doc in
/// `Cargo.toml`) — with the feature off, `gc_stats_per_shard` returns an
/// empty `Vec` and none of the counter fields/increments exist in the
/// compiled binary at all, so a normal release build pays nothing for this.
pub struct GcStats {
    /// Allocation requests satisfied by a page reclaimed from the owning shard,
    /// including prefetched pages later handed out from its reuse cache.
    pub local_reuse: u64,
    /// Requests satisfied by a page reclaimed from another shard, including
    /// prefetched pages. Each page is counted only when handed to an allocation.
    pub steal: u64,
    /// No reusable/dead block found anywhere; a brand-new block was
    /// obtained from the global allocator (see `Block::into_cell`).
    pub fresh_alloc: u64,
}

pub type TrackerHandle<const P_F: usize, const P_N: usize, Key, Payload> =
    Arc<TrackerHandleSt<P_F, P_N, Key, Payload>>;

/// Always present on every tree, one per `BlockAllocManager` — page/block
/// reclaim (`dead_blocks`/`register_died_page*`/`free_block`), gated by its
/// own `block_reclaim_enabled` flag (`MVBTSt::enable_gc`/`disable_gc`).
/// Genuinely per-table: dead pages are physically owned by this table's own
/// `BlockAllocManager`, so reclaiming them is never a cross-table concern.
///
/// Active-snapshot tracking used to live here too, but it isn't a per-table
/// concept once several tables can share one transactional core (see
/// `crate::bat_sync::tx_context::TxContext`'s doc): it moved there, since a
/// snapshot registered once by a cross-table transaction may go on to read
/// *any* table sharing that context, not just whichever one happened to
/// register it. `free_block` below reads that shared state (via a `&TxContext`
/// parameter) rather than a per-table copy — see its doc for why that's
/// required for soundness, not just a rename.
pub struct TrackerHandleSt<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static,
> {
    dead_blocks: BlockTrace<P_F, P_N, Key, Payload>,
    /// Pages already proven reclaimable, filled in batches so the expensive
    /// all-worker liveness scan is amortized across several allocations.
    reusable: Vec<Mutex<Vec<ReusableBlock<P_F, P_N, Key, Payload>>>>,
    /// Explicit opt-in for block reclaim (`MVBTSt::enable_gc`/`disable_gc`).
    /// `false` by default: a fresh tree never reuses blocks until this is
    /// turned on, matching the pre-existing behavior from when the whole
    /// tracker was optional.
    block_reclaim_enabled: AtomicBool,
    /// See `GcStats`'s doc. One triple per shard, `worker_id % len()`-indexed
    /// like `reusable`/`dead_blocks`, so recording these never introduces
    /// the cross-thread contention this benchmarking is meant to measure the
    /// absence of. Feature-gated (see `GcStats`'s doc) — doesn't exist in a
    /// plain build.
    #[cfg(feature = "gc-stats")]
    local_reuse: Vec<CachePadded<AtomicU64>>,
    #[cfg(feature = "gc-stats")]
    steal: Vec<CachePadded<AtomicU64>>,
    #[cfg(feature = "gc-stats")]
    fresh_alloc: Vec<CachePadded<AtomicU64>>,
}

impl<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default,
> TrackerHandleSt<P_F, P_N, Key, Payload>
{
    pub fn new() -> Self {
        let shard_count = num_cpus::get().max(1);
        Self {
            dead_blocks: BlockTrace::new(),
            reusable: (0..shard_count).map(|_| Mutex::new(Vec::new())).collect(),
            block_reclaim_enabled: AtomicBool::new(false),
            #[cfg(feature = "gc-stats")]
            local_reuse: (0..shard_count)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
            #[cfg(feature = "gc-stats")]
            steal: (0..shard_count)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
            #[cfg(feature = "gc-stats")]
            fresh_alloc: (0..shard_count)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
        }
    }

    /// See `GcStats`'s doc. Called from `BlockAllocManager::alloc_block`'s
    /// fallback branch, once `free_block` has already returned `None` (i.e.
    /// nothing reusable was found locally, nor by stealing from any other
    /// shard). A no-op without the `gc-stats` feature.
    #[cfg(feature = "gc-stats")]
    #[inline]
    pub(crate) fn record_fresh_alloc(&self, worker_id: WorkerId) {
        let idx = worker_id as usize % self.fresh_alloc.len();
        self.fresh_alloc[idx].fetch_add(1, Relaxed);
    }

    #[cfg(not(feature = "gc-stats"))]
    #[inline(always)]
    pub(crate) fn record_fresh_alloc(&self, _worker_id: WorkerId) {}

    /// Per-shard GC breakdown accumulated since this tracker was created —
    /// see `GcStats`'s doc. Always empty without the `gc-stats` feature.
    #[cfg(feature = "gc-stats")]
    pub fn gc_stats_per_shard(&self) -> Vec<GcStats> {
        (0..self.local_reuse.len())
            .map(|i| GcStats {
                local_reuse: self.local_reuse[i].load(Relaxed),
                steal: self.steal[i].load(Relaxed),
                fresh_alloc: self.fresh_alloc[i].load(Relaxed),
            })
            .collect()
    }

    #[cfg(not(feature = "gc-stats"))]
    pub fn gc_stats_per_shard(&self) -> Vec<GcStats> {
        Vec::new()
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

    /// Always marks `page` retired (see `RETIRED_FLAG_VERSION`'s doc) — a
    /// correctness fix independent of GC, so it applies whether or not block
    /// reclaim is on. The actual `dead_blocks` bookkeeping stays gated
    /// behind `block_reclaim_enabled` as before — otherwise it would just
    /// grow forever recording pages nothing will ever come collect.
    #[inline]
    pub fn register_died_page(
        &self,
        worker_id: WorkerId,
        page_version: Version,
        page: DeadPageValue<P_F, P_N, Key, Payload>,
    ) {
        page.mark_retired();

        if self.block_reclaim_enabled.load(Relaxed) {
            self.dead_blocks
                .register_died_page(worker_id, page_version, page)
        }
    }

    #[inline]
    pub fn register_died_page_col(
        &self,
        worker_id: WorkerId,
        dead_pages: [(Version, BlockRef<P_F, P_N, Key, Payload>); 2],
    ) {
        dead_pages.iter().for_each(|(_, page)| page.mark_retired());

        if self.block_reclaim_enabled.load(Relaxed) {
            self.dead_blocks
                .register_died_page_col(worker_id, dead_pages)
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

        let worker_id = ctx.worker_id();
        let cache_index = worker_id as usize % self.reusable.len();
        if let Some(page) = self.reusable[cache_index].lock().pop() {
            #[cfg(feature = "gc-stats")]
            let page = {
                let (page, stolen) = page;
                let counter = if stolen { &self.steal } else { &self.local_reuse };
                counter[cache_index].fetch_add(1, Relaxed);
                page
            };
            return Some(page);
        }

        // `live_min_snapshot` already folds in any worker mid-registration
        // (drawn a ts_start, not yet recorded in `live_tx` — might need
        // exactly the block we're about to hand out) alongside fully-active
        // transactions — see `TxContext::in_flight_bound`'s doc. No separate
        // "wait until nothing anywhere is mid-registration" check needed.
        let live_min_snapshot = ctx.live_min_snapshot();

        const RECLAIM_BATCH: usize = 16;
        let (reclaimed_roots, _local_count, _steal_count) =
            self.dead_blocks
                .reclaim_batch(
                    worker_id,
                    RECLAIM_BATCH,
                    |(dead_v, _)| match live_min_snapshot {
            None => true,
            Some(live_min_snapshot) => dead_v.lt_self_any(live_min_snapshot),
                    },
                );
        // reclaim_batch orders own-shard pages first, then stolen pages.
        #[cfg(feature = "gc-stats")]
        let mut reclaimed: Vec<_> = reclaimed_roots.into_iter().enumerate()
            .map(|(index, page)| (page, index >= _local_count)).collect();
        #[cfg(not(feature = "gc-stats"))]
        let mut reclaimed = reclaimed_roots;
        let result = reclaimed.pop();
        #[cfg(feature = "gc-stats")]
        let result = result.map(|(page, stolen)| {
            let counter = if stolen { &self.steal } else { &self.local_reuse };
            counter[cache_index].fetch_add(1, Relaxed);
            page
        });
        if !reclaimed.is_empty() {
            self.reusable[cache_index].lock().extend(reclaimed);
        }
        result
    }
}
