use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use crate::mv_gc::block_tracer::{DeadPageValue, BlockTrace};
use crate::mv_gc::query_tracer::TransactionTrace;
use crate::mv_page_model::BlockRef;
use crate::mv_page_model::time_matcher::TimeMatcher;
use crate::mv_query::SnapShot;
use crate::mv_record_model::tx_stamp::WorkerId;
use crate::mv_record_model::version_info::Version;

pub type TrackerHandle<
    const P_F: usize,
    const P_N: usize,
    Key,
    Payload> = Arc<TrackerHandleSt<P_F, P_N, Key, Payload>>;

/// Always present on every tree, one per `BlockAllocManager` (see that
/// type's `tracker` field) — the struct itself isn't a GC-only add-on, but
/// both concerns it bundles *are* gated by the same `block_reclaim_enabled`
/// flag:
///
/// - Active-snapshot tracking (`live_tx`, `registrations_in_flight`): feeds
///   `CommitLog` pruning (`MVBTSt::commit_tx`), which drops a worker's old
///   commit timestamps once no *currently* active snapshot needs them for
///   its `LCB` query. That's only sound if anything whose `LCB` data gets
///   dropped is itself unreachable by then — true when block reclaim is
///   actually removing dead pages in lockstep, false otherwise: with reclaim
///   off, dead records — and, notably, an explicit historical read
///   (`CRUDOperation::Point`/`Range` at an old `version` nobody's held a
///   live snapshot on since — see `mv_query::dispatch`'s doc on those
///   variants) — stay reachable forever, so pruning the `LCB` data they need
///   out from under them would silently corrupt their visibility check.
///   So this tracking — and `MVBTSt::commit_tx`'s choice of
///   `CommitLog::commit_pruned` vs. plain `commit` — is gated by
///   `block_reclaim_enabled` exactly like block reclaim itself: a tree run
///   with GC off keeps unbounded per-worker `CommitLog`s (matching pre-OSIC
///   behavior) rather than risk that corruption.
/// - Block reclaim (`dead_blocks`, `free_block`): the actual page-recycling
///   GC feature — gated by the same flag (toggled by `MVBTSt::enable_gc`/
///   `disable_gc`).
pub struct TrackerHandleSt<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static>
{
    live_tx: TransactionTrace,
    dead_blocks: BlockTrace<P_F, P_N, Key, Payload>,
    /// Count of `begin_snapshot` calls that have drawn a `ts_start` (via the
    /// global clock, immediately visible to everyone) but not yet finished
    /// registering it in `live_tx` — see `mv_sync::version_handle::
    /// begin_snapshot`. While this is nonzero, `free_block` must not
    /// reclaim anything: some in-flight reader's about-to-be-registered
    /// snapshot could need exactly the page a naive check would otherwise
    /// call safe, since it isn't in `live_tx` yet to be counted. Cheaper
    /// than closing the gap by pre-registering a provisional entry in
    /// `live_tx` itself (measured: that approach cost 6-9x throughput under
    /// sustained concurrent load, from doubling/tripling skip-list
    /// insertions at its monotonically-growing, therefore always-contended,
    /// tail) — this is one shared counter, checked and touched with plain
    /// `Relaxed` increments/decrements, no allocation, no per-value entry.
    registrations_in_flight: AtomicUsize,
    /// Explicit opt-in for block reclaim (`MVBTSt::enable_gc`/`disable_gc`)
    /// — see the type doc. `false` by default: a fresh tree never reuses
    /// blocks until this is turned on, matching the pre-existing behavior
    /// from when the whole tracker was optional. Does *not* gate active-
    /// snapshot tracking (`on_tx_start`/`on_tx_completed`/`active_snapshots`
    /// /`begin_snapshot_registration`/`end_snapshot_registration`), which
    /// stays live unconditionally — that's the whole point.
    block_reclaim_enabled: AtomicBool,
}

impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default> TrackerHandleSt<P_F, P_N, Key, Payload>
{
    pub fn new() -> Self {
        Self {
            live_tx: TransactionTrace::new(),
            dead_blocks: BlockTrace::new(),
            registrations_in_flight: AtomicUsize::new(0),
            block_reclaim_enabled: AtomicBool::new(false),
        }
    }

    /// See `block_reclaim_enabled`'s field doc.
    #[inline]
    pub fn set_block_reclaim_enabled(&self, enabled: bool) {
        self.block_reclaim_enabled.store(enabled, Relaxed);
    }

    /// See `block_reclaim_enabled`'s field doc — also what gates whether
    /// `on_tx_start`/`on_tx_completed` actually track anything, and whether
    /// `MVBTSt::commit_tx` prunes its `CommitLog`s (see this type's doc).
    #[inline]
    pub fn block_reclaim_enabled(&self) -> bool {
        self.block_reclaim_enabled.load(Relaxed)
    }

    /// Call *before* drawing a new `ts_start`, pairing with
    /// `end_snapshot_registration` once it's been recorded in `live_tx` —
    /// see the field doc on `registrations_in_flight`.
    #[inline]
    pub fn begin_snapshot_registration(&self) {
        self.registrations_in_flight.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn end_snapshot_registration(&self) {
        self.registrations_in_flight.fetch_sub(1, Relaxed);
    }

    /// No-op while block reclaim is disabled — see this type's doc for why
    /// tracking a snapshot as "active" is pointless (and, worse, would let
    /// `MVBTSt::commit_tx` believe it's safe to prune around) when nothing
    /// downstream (`CommitLog` pruning, `free_block`) is gated on the result.
    #[inline]
    pub fn on_tx_start(&self, snap_shot: SnapShot) {
        if self.block_reclaim_enabled.load(Relaxed) {
            self.live_tx.on_tx_start(snap_shot)
        }
    }

    /// See `on_tx_start`.
    #[inline]
    pub fn on_tx_completed(&self, snap_shot: SnapShot) {
        if self.block_reclaim_enabled.load(Relaxed) {
            self.live_tx.on_tx_completed(snap_shot);
        }
    }

    /// No-op while block reclaim is disabled — otherwise `dead_blocks` would
    /// just grow forever recording pages nothing will ever come collect,
    /// the exact "always present but only pruned when GC is on" problem
    /// this type's always-active `live_tx`/`registrations_in_flight` exist
    /// to *not* have.
    #[inline]
    pub fn register_died_page(&self, worker_id: WorkerId, page_version: Version, page: DeadPageValue<P_F, P_N, Key, Payload>) {
        if self.block_reclaim_enabled.load(Relaxed) {
            self.dead_blocks.register_died_page(worker_id, page_version, page)
        }
    }

    #[inline]
    pub fn register_died_page_col(&self, worker_id: WorkerId, dead_pages: [(Version, BlockRef<P_F, P_N, Key, Payload>); 2]) {
        if self.block_reclaim_enabled.load(Relaxed) {
            self.dead_blocks.register_died_page_col(worker_id, dead_pages)
        }
    }

    // #[inline]
    // pub fn oldest_live_si(&self) -> Option<SnapShot> {
    //     let min_si = self.live_tx.peek_min();
    //     if min_si == Version::MAX {
    //         None
    //     }
    //     else {
    //         Some(min_si)
    //     }
    // }

    #[inline]
    pub fn newest_live_si(&self) -> Option<SnapShot> {
        self.live_tx.peek_max()
    }

    /// All currently active `ts_start`s — used to safely prune per-worker
    /// `CommitLog`s (see `MVBTSt::commit_tx`).
    #[inline]
    pub fn active_snapshots(&self) -> impl Iterator<Item = SnapShot> + '_ {
        self.live_tx.active_snapshots()
    }

    #[inline]
    pub fn free_block(&self) -> Option<BlockRef<P_F, P_N, Key, Payload>> {
        if !self.block_reclaim_enabled.load(Relaxed) {
            return None;
        }

        // A reader mid-registration (drawn a ts_start, not yet recorded in
        // live_tx — see `registrations_in_flight`'s doc) might need exactly
        // the block we're about to hand out; defer entirely rather than
        // risk it. Self-limiting: registration is a couple of instructions,
        // so this is never held for long.
        if self.registrations_in_flight.load(Relaxed) > 0 {
            return None;
        }

        let live_min_snapshot = self.live_tx.peek_min();

        self.dead_blocks.try_reclaim(|dead_v| match live_min_snapshot {
            None => true,
            Some(live_min_snapshot) => dead_v.lt_self_any(live_min_snapshot),
        })
    }
}
