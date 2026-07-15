use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::Relaxed;

use crate::mv_gc::query_tracer::TransactionTrace;
use crate::mv_query::SnapShot;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::Version;
use crate::mv_sync::clock::GlobalClock;
use crate::mv_sync::commit_log::CommitLog;
use crate::mv_sync::visibility;
use crate::mv_sync::worker::WorkerRegistry;

/// The transactional core one or more `MVBTSt` trees share: the OSIC Global
/// Logical Clock, every worker's `CommitLog`, the `WorkerRegistry`, and
/// active-snapshot tracking (`live_tx`/`registrations_in_flight`, moved here
/// from `mv_gc::tracker_handle::TrackerHandleSt`). None of this is generic
/// over `Key`/`Payload` — it only ever operates on `Version`/`WorkerId` — so
/// a single, non-generic `TxContext` can be shared (via `Arc`) by any number
/// of differently-typed per-table trees, letting one `Transaction`-like
/// caller draw one `ts_start`, write across several tables, and commit once,
/// atomically and snapshot-isolated, across all of them — exactly like a
/// single-tree `MVBTSt` already does for itself.
///
/// A tree that doesn't need to share (every existing single-tree caller —
/// `mv_test`, `ycsb_driver`, `main.rs`) just gets its own private
/// `Arc<TxContext>`, built once at construction (see `MVBTSt::make`) — this
/// type changes nothing observable for those callers, only where the fields
/// physically live.
///
/// Page/block reclaim (`mv_gc::tracker_handle::TrackerHandleSt::dead_blocks`)
/// deliberately stays OUT of this type and per-table instead: dead pages are
/// physically owned by one table's `BlockAllocManager`, so reclaiming them is
/// never a cross-table concern the way visibility/commit/snapshot-liveness
/// is. See `TrackerHandleSt::free_block`'s use of `live_min_snapshot`/
/// `registrations_in_flight` below for the one place block reclaim still
/// needs to *read* (not own) this shared state.
pub(crate) struct TxContext {
    global_clock: GlobalClock,
    commit_logs: Vec<CommitLog>,
    worker_registry: WorkerRegistry,
    /// Active-snapshot tracking, moved verbatim from `TrackerHandleSt`: see
    /// that type's former doc (now this one's) for why pruning `commit_logs`
    /// is only sound while this precisely reflects every transaction with a
    /// live, unreleased `ts_start` across every table sharing this context.
    live_tx: TransactionTrace,
    /// See `TrackerHandleSt::registrations_in_flight`'s original doc
    /// (moved here unchanged): counts `begin_snapshot` calls that have drawn
    /// a `ts_start` but not yet finished recording it in `live_tx` — nothing
    /// may treat a page as reclaimable while this is nonzero.
    registrations_in_flight: AtomicUsize,
    /// Own copy, independent of any single table's
    /// `TrackerHandleSt::block_reclaim_enabled` (which still gates that
    /// table's own dead-page bookkeeping/reuse) — this one gates whether
    /// `commit_tx` is allowed to prune the *shared* `commit_logs` and
    /// whether `on_tx_start`/`on_tx_completed` track anything at all. Sound
    /// only if every table sharing this context reclaims in lockstep, which
    /// is why callers must toggle GC uniformly across a whole multi-table
    /// database (see `mv_bench::tpcc_schema::TpccDatabase::enable_gc`)
    /// rather than per table.
    block_reclaim_enabled: AtomicBool,
    freshest_si_truncate_commit_log: AtomicBool,
}

impl TxContext {
    pub(crate) fn new(max_workers: usize) -> Self {
        Self {
            global_clock: GlobalClock::new(),
            commit_logs: (0..max_workers).map(|_| CommitLog::new()).collect(),
            worker_registry: WorkerRegistry::new(max_workers),
            live_tx: TransactionTrace::new(),
            registrations_in_flight: AtomicUsize::new(0),
            block_reclaim_enabled: AtomicBool::new(false),
            freshest_si_truncate_commit_log: AtomicBool::new(true),
        }
    }

    #[inline(always)]
    pub(crate) fn max_workers(&self) -> usize {
        self.worker_registry.max_workers()
    }

    /// Exposes the raw clock for `WalWriter::start_commit_logged`, which
    /// mints its own stamp directly off it (see `MVBTSt::wal_start_commit`).
    #[inline(always)]
    pub(crate) fn global_clock(&self) -> &GlobalClock {
        &self.global_clock
    }

    #[inline]
    pub(crate) fn set_block_reclaim_enabled(&self, enabled: bool) {
        self.block_reclaim_enabled.store(enabled, Relaxed);
    }

    #[inline]
    pub(crate) fn set_truncate_commit_log(&self, enabled: bool) {
        self.freshest_si_truncate_commit_log.store(enabled, Relaxed);
    }

    #[inline]
    pub(crate) fn block_reclaim_enabled(&self) -> bool {
        self.block_reclaim_enabled.load(Relaxed)
    }

    #[inline(always)]
    pub(crate) fn worker_id(&self) -> WorkerId {
        crate::mv_sync::worker::worker_id_for(&self.worker_registry)
    }

    /// See `mv_gc::tracker_handle::TrackerHandleSt::begin_snapshot_registration`
    /// (moved here unchanged, same pairing contract with `end_snapshot_registration`).
    #[inline]
    fn begin_snapshot_registration(&self) {
        self.registrations_in_flight.fetch_add(1, Relaxed);
    }

    #[inline]
    fn end_snapshot_registration(&self) {
        self.registrations_in_flight.fetch_sub(1, Relaxed);
    }

    /// Draws a fresh `ts_start` and hands it to `register` before releasing
    /// the in-flight-registration guard — see `begin_snapshot`'s doc (this
    /// project's `mv_sync::version_handle` used to carry this same doc
    /// before the gap-closing logic moved here).
    #[inline(always)]
    pub(crate) fn draw_snapshot_version_with<R>(&self, register: impl FnOnce(Version) -> R) -> R {
        self.begin_snapshot_registration();

        let ts_start = self.global_clock.next_timestamp();
        let result = register(ts_start);

        self.end_snapshot_registration();

        result
    }

    /// No-op while block reclaim is disabled — see this type's doc for why
    /// tracking a snapshot as "active" is pointless (and, worse, would let
    /// `commit_tx` believe it's safe to prune around) when nothing
    /// downstream (`CommitLog` pruning, `free_block`) is gated on the
    /// result. `pub(crate)`: also called directly by
    /// `MVBTSt::on_acquire_reader_snapshot`/`on_release_reader_snapshot`
    /// (registering an already-known version as a reader, as opposed to
    /// `begin_snapshot`/`end_snapshot`, which draw a fresh one).
    #[inline]
    pub(crate) fn on_tx_start(&self, snapshot: SnapShot) {
        if self.block_reclaim_enabled.load(Relaxed) {
            self.live_tx.on_tx_start(snapshot);
        }
    }

    #[inline]
    pub(crate) fn on_tx_completed(&self, snapshot: SnapShot) {
        if self.block_reclaim_enabled.load(Relaxed) {
            self.live_tx.on_tx_completed(snapshot);
        }
    }

    #[inline(always)]
    pub(crate) fn begin_snapshot(&self) -> Version {
        self.draw_snapshot_version_with(|ts_start| {
            self.on_tx_start(ts_start);
            ts_start
        })
    }

    #[inline(always)]
    pub(crate) fn end_snapshot(&self, ts_start: Version) {
        self.on_tx_completed(ts_start);
    }

    #[inline(always)]
    pub(crate) fn current_version(&self) -> Version {
        self.global_clock.current_version()
    }

    #[inline(always)]
    pub(crate) fn start_tx_commit(&self) -> Version {
        self.global_clock.next_timestamp()
    }

    #[inline(always)]
    pub(crate) fn is_visible_stamp(
        &self,
        reader_worker: WorkerId,
        reader_ts_start: Version,
        stamp: TxStamp,
    ) -> bool {
        crate::mv_sync::worker::with_snapshot_cache(&self.worker_registry, |cache| {
            visibility::is_visible(&self.commit_logs, cache, reader_worker, reader_ts_start, stamp)
        })
    }

    #[inline(always)]
    pub(crate) fn with_visibility_checker<R>(
        &self,
        reader_worker: WorkerId,
        reader_ts_start: Version,
        f: impl FnOnce(&mut dyn FnMut(TxStamp) -> bool) -> R,
    ) -> R {
        crate::mv_sync::worker::with_snapshot_cache(&self.worker_registry, |cache| {
            f(&mut |stamp| visibility::is_visible(&self.commit_logs, cache, reader_worker, reader_ts_start, stamp))
        })
    }

    #[inline(always)]
    pub(crate) fn commit_tx(&self, worker_id: WorkerId) -> Version {
        if self.block_reclaim_enabled.load(Relaxed) ||
           self.freshest_si_truncate_commit_log.load(Relaxed)
        {
            self.commit_logs[worker_id as usize].commit_pruned(
                &self.global_clock,
                self.commit_logs.len(),
                self.live_tx.active_snapshots(),
            )
        } else {
            self.commit_logs[worker_id as usize].commit(&self.global_clock)
        }
    }

    #[inline]
    pub(crate) fn newest_live_si(&self) -> Option<SnapShot> {
        self.live_tx.peek_max()
    }

    /// The oldest currently-active snapshot across every table sharing this
    /// context, or `None` if there are none — the "safe to reclaim anything
    /// dead strictly before this" bound `TrackerHandleSt::free_block` needs
    /// (see that method's doc: it must consult this *shared* bound, not a
    /// per-table one, since a snapshot registered once here may later read
    /// any table).
    #[inline]
    pub(crate) fn live_min_snapshot(&self) -> Option<SnapShot> {
        self.live_tx.peek_min()
    }

    /// See `registrations_in_flight`'s field doc — `TrackerHandleSt::free_block`
    /// must not reclaim anything while this is nonzero.
    #[inline]
    pub(crate) fn registrations_in_flight(&self) -> usize {
        self.registrations_in_flight.load(Relaxed)
    }

    /// Test-only: current entry count of one worker's `CommitLog`, for tests
    /// confirming pruning keeps it bounded (or, with GC off, that it keeps
    /// growing) rather than exposing `commit_logs` itself.
    #[cfg(test)]
    pub(crate) fn commit_log_len(&self, worker_id: WorkerId) -> usize {
        self.commit_logs[worker_id as usize].len()
    }
}
