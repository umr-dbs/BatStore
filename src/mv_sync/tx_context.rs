use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicU32};

use crossbeam_utils::CachePadded;

use crate::mv_query::SnapShot;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::{AtomicVersion, Version};
use crate::mv_sync::clock::GlobalClock;
use crate::mv_sync::commit_log::CommitLog;
use crate::mv_sync::visibility;
use crate::mv_sync::worker::WorkerRegistry;

/// A short-lived page-reclamation guard for a traversal which needs current
/// tree pointers to remain allocated, but does not need an MVCC timestamp.
/// The slot is cleared on unwind as well as on the ordinary return path.
struct ReclamationPin<'a> {
    ctx: &'a TxContext,
    worker_id: WorkerId,
    published: bool,
}

impl Drop for ReclamationPin<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.published {
            self.ctx.in_flight_bound[self.worker_id as usize].store(NOT_IN_FLIGHT, Release);
        }
    }
}

/// Sentinel for "this worker isn't mid-registration right now" in
/// `TxContext::in_flight_bound` — real `ts_start`s are drawn from a counter
/// starting at `version_handle::START_VERSION`, so `Version::MAX` is
/// unreachable as a genuine one.
const NOT_IN_FLIGHT: Version = Version::MAX;

/// The transactional core one or more `MVBTSt` trees share: the OSIC Global
/// Logical Clock, every worker's `CommitLog`, the `WorkerRegistry`, and
/// active-snapshot tracking (`live_tx`/`in_flight_bound`, moved here from
/// `mv_gc::tracker_handle::TrackerHandleSt`). None of this is generic
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
/// is. See `TrackerHandleSt::free_block`'s use of `live_min_snapshot` below
/// for the one place block reclaim still needs to *read* (not own) this
/// shared state.
pub(crate) struct TxContext {
    global_clock: GlobalClock,
    commit_logs: Vec<CommitLog>,
    worker_registry: WorkerRegistry,
    /// Active-snapshot tracking: one slot per worker, holding that worker's
    /// currently-published `ts_start` (or `NOT_IN_FLIGHT` while idle) — see
    /// that former doc (now this one's) for why pruning `commit_logs` is
    /// only sound while this precisely reflects every transaction with a
    /// live, unreleased `ts_start` across every table sharing this context.
    ///
    /// Was a shared `SkipMap<SnapShot, AtomicUsize>` (`mv_gc::query_tracer`,
    /// now removed): profiling a saturated TPC-C run showed it eating
    /// roughly a quarter to half of *all* CPU cycles — every worker's
    /// `on_tx_start`/`on_tx_completed` call inserts/removes into the same
    /// shared skip list, clustering near its current max key exactly like
    /// `in_flight_bound`'s doc describes for the counter it replaced, and
    /// `commit_tx` walks the *entire* thing on every commit (not just under
    /// GC — see `freshest_si_truncate_commit_log`). A worker only ever runs
    /// one transaction at a time, so — like `in_flight_bound` — one slot per
    /// worker suffices: `O(max_workers)` uncontended `Acquire` scans instead
    /// of skip-list traversals contending with concurrent inserts/removes,
    /// and no refcounting (two workers can independently publish the exact
    /// same value with zero coordination, since neither ever touches the
    /// other's slot).
    ///
    /// One wrinkle a naive single-value-per-worker slot would get wrong:
    /// `mv_query::olc_query::traversal_write_olc` calls `begin_snapshot`/
    /// `end_snapshot` on *every* insert/update/delete traversal, nested
    /// inside an already-registered `DbTransaction`'s own live snapshot —
    /// intentionally (its own doc: "concurrent registrations... explicitly
    /// designed to stack"). `live_tx_depth` below makes only the *outermost*
    /// `on_tx_start`/`on_tx_completed` for a worker actually touch this
    /// slot: since one worker's `ts_start`s are drawn from a single
    /// strictly-increasing clock, an outer (earlier, lower) registration's
    /// protection already covers everything any later, nested (higher) one
    /// could need, so nested calls have nothing to publish.
    /// `CachePadded`: without it, up to 8 adjacent workers' `AtomicU64`
    /// slots share one 64-byte cache line, so one worker's `Release` store
    /// here (every `on_tx_start`/`on_tx_completed`) invalidates the line for
    /// every *other* worker whose slot happens to land in it too — silent
    /// false sharing that directly contradicts this field's own "no shared
    /// cache line with any other worker's slot" design intent below. Doesn't
    /// change behavior at all (`CachePadded<T>` derefs straight to `T`), only
    /// each slot's physical placement — invisible at low thread counts (the
    /// coherence traffic is real but small next to everything else going on)
    /// and, going by this exact false-sharing shape, plausibly significant
    /// at the 64-core/128-thread scale this project's benchmarks target,
    /// where cross-CCD cache-coherence traffic costs considerably more than
    /// on a small single-CCX box.
    live_tx: Vec<CachePadded<AtomicVersion>>,
    /// Reentrancy depth per worker for `on_tx_start`/`on_tx_completed`, same
    /// indexing as `live_tx` — see that field's doc for why nesting exists.
    /// `Relaxed` throughout: each slot is written only by the one worker it
    /// belongs to (nothing else ever touches index `worker_id`), so there is
    /// no cross-thread ordering to establish here, just a plain counter that
    /// happens to sit behind an atomic for `Sync`. A debug-only sanity net,
    /// not load-bearing for correctness: `on_tx_start`/`on_tx_completed`
    /// assert this stays balanced (never negative, never re-publishes over
    /// an already-live outer value) — genuine caller bugs (unpaired calls),
    /// not something real callers are expected to trigger.
    ///
    /// `CachePadded` for the same reason as `live_tx` — see that field's doc.
    live_tx_depth: Vec<CachePadded<AtomicU32>>,
    /// One slot per worker (`WorkerId`-indexed, sized to `max_workers` like
    /// `commit_logs`): each worker publishes its own conservative lower bound
    /// here — `global_clock.current_version()` read just *before* drawing
    /// its real `ts_start` — while it's mid-registration (drawn a `ts_start`
    /// but not yet recorded it in `live_tx`), and clears it back to
    /// `NOT_IN_FLIGHT` once that's done. `free_block` needs *some* protection
    /// for this narrow window (a reader mid-registration might need exactly
    /// the block about to be handed out), but the window is per-worker and
    /// short, so protecting it shouldn't require every *other* worker
    /// system-wide to be simultaneously quiescent.
    ///
    /// Superseded a single global `registrations_in_flight: AtomicUsize`
    /// (a shared counter every worker had to contend on, incremented/
    /// decremented on literally every snapshot draw) that measurably
    /// collapsed under concurrency: instrumented under a 16-thread
    /// update-heavy YCSB workload, it rejected 73.8% of `free_block` attempts
    /// (vs. 5.2% at 2 threads) — with more workers each drawing snapshots
    /// constantly, the odds that *some* worker is inside that tiny window at
    /// any sampled instant approach certainty, even though the window itself
    /// never got any longer. A per-worker slot has no such effect: each
    /// worker only ever contends with itself (one `Release` store, no CAS,
    /// no shared cache line with any other worker's slot), so `free_block`
    /// scanning every slot costs `O(max_workers)` with zero contention
    /// instead of waiting on a single hot counter every worker is
    /// incrementing/decrementing at full throughput.
    ///
    /// Each slot is `Release`-written by its own worker and `Acquire`-read
    /// by `free_block`/`live_min_snapshot` — a single atomic word, so a
    /// pairwise Release/Acquire on that one location is sufficient on its
    /// own (unlike the old design's separate counter+`live_tx` pairing,
    /// which needed the counter to reach *zero* specifically to paper over
    /// the fact that a `ts_start`'s draw and its `live_tx` insert are two
    /// different pieces of state). Soundness relies on: a fresh `ts_start`
    /// is always >= any bound already published by an in-flight-or-complete
    /// registration (the clock only ever increases via `next_timestamp`'s
    /// `fetch_add`), so a slot transitioning from `NOT_IN_FLIGHT` to some
    /// value *during* a scan can only ever raise the true minimum, never
    /// lower it — the scan doesn't need every slot to be simultaneously
    /// consistent with every other, each is independently safe to read on
    /// its own.
    ///
    /// `CachePadded` for the same false-sharing reason as `live_tx` — see
    /// that field's doc (this field's own "no shared cache line" claim above
    /// is exactly what the padding actually delivers on).
    in_flight_bound: Vec<CachePadded<AtomicVersion>>,
    /// Own copy, independent of any single table's
    /// `TrackerHandleSt::block_reclaim_enabled` (which still gates that
    /// table's own dead-page bookkeeping/reuse) — this one gates whether
    /// `commit_tx` is allowed to prune the *shared* `commit_logs` and
    /// whether `on_tx_start`/`on_tx_completed` track anything at all. Sound
    /// only if every table sharing this context reclaims in lockstep, which
    /// is why callers must toggle GC uniformly across a whole multi-table
    /// database (see `mv_bench::tpcc_schema::TpccDatabase::enable_gc`)
    /// rather than per table.
    pub(crate) block_reclaim_enabled: AtomicBool,
    freshest_si_truncate_commit_log: AtomicBool,
}

impl TxContext {
    pub(crate) fn new(max_workers: usize) -> Self {
        Self {
            global_clock: GlobalClock::new(),
            commit_logs: (0..max_workers).map(|_| CommitLog::new()).collect(),
            worker_registry: WorkerRegistry::new(max_workers),
            live_tx: (0..max_workers)
                .map(|_| CachePadded::new(AtomicVersion::new(NOT_IN_FLIGHT)))
                .collect(),
            live_tx_depth: (0..max_workers)
                .map(|_| CachePadded::new(AtomicU32::new(0)))
                .collect(),
            in_flight_bound: (0..max_workers)
                .map(|_| CachePadded::new(AtomicVersion::new(NOT_IN_FLIGHT)))
                .collect(),
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

    /// Publishes this worker's conservative lower bound — see
    /// `in_flight_bound`'s field doc for why a plain `Release` store to this
    /// worker's own slot is sufficient (no counter, no cross-worker
    /// contention). Returns the `WorkerId` used, so the matching
    /// `end_snapshot_registration` call touches the same slot without a
    /// second thread-local lookup.
    #[inline]
    fn begin_snapshot_registration(&self) -> WorkerId {
        let worker_id = self.worker_id();
        // Read *before* drawing the real ts_start below: `next_timestamp`'s
        // `fetch_add` only ever increases the same counter this reads, so
        // the real ts_start is guaranteed >= this bound.
        let conservative_bound = self.global_clock.current_version();
        self.in_flight_bound[worker_id as usize].store(conservative_bound, Release);
        worker_id
    }

    /// `Release`: pairs with the `Acquire` load `live_min_snapshot` does on
    /// this exact slot. Clearing back to `NOT_IN_FLIGHT` here (after
    /// `draw_snapshot_version_with`'s `register` closure — which performs
    /// the `live_tx` insert — has already run) means this worker's
    /// protection has already handed off to `live_tx` by the time this call
    /// returns; there's no gap between the two mechanisms.
    #[inline]
    fn end_snapshot_registration(&self, worker_id: WorkerId) {
        self.in_flight_bound[worker_id as usize].store(NOT_IN_FLIGHT, Release);
    }

    /// Runs `f` while page reclaim is conservatively pinned at the clock's
    /// current position, without drawing a timestamp or registering a real
    /// OSIC snapshot. An already-live transaction is itself an older (and
    /// therefore sufficient) pin, so the nested case performs no stores.
    #[inline(always)]
    pub(crate) fn with_reclamation_pin<R>(&self, f: impl FnOnce() -> R) -> R {
        if !self.block_reclaim_enabled.load(Relaxed) {
            return f();
        }
        let worker_id = self.worker_id();
        let published = self.live_tx_depth[worker_id as usize].load(Relaxed) == 0;
        if published {
            let bound = self.global_clock.current_version();
            self.in_flight_bound[worker_id as usize].store(bound, Release);
        }
        let _pin = ReclamationPin {
            ctx: self,
            worker_id,
            published,
        };
        f()
    }

    /// Draws a fresh `ts_start` and hands it to `register` before releasing
    /// the in-flight-registration guard — see `begin_snapshot`'s doc (this
    /// project's `mv_sync::version_handle` used to carry this same doc
    /// before the gap-closing logic moved here).
    #[inline(always)]
    pub(crate) fn draw_snapshot_version_with<R>(&self, register: impl FnOnce(Version) -> R) -> R {
        let worker_id = self.begin_snapshot_registration();

        let ts_start = self.global_clock.next_timestamp();
        let result = register(ts_start);

        self.end_snapshot_registration(worker_id);

        result
    }

    /// Used to be a no-op while block reclaim was disabled, on the theory
    /// that "nothing downstream (`CommitLog` pruning, `free_block`) is
    /// gated on the result" then — wrong: `MVBTSt::record_survives_gc`
    /// (via `is_snapshot_live`) also depends on `live_tx` being populated,
    /// to keep a record a still-in-flight transaction deleted physically
    /// present in case that transaction aborts and needs to reverse the
    /// delete. That protection is needed unconditionally, because the SMO
    /// compaction it guards (`MVBTSt::split`'s version-split path) runs
    /// unconditionally too — it's driven by `active`/`dead` counts, not by
    /// the GC/block-reclaim toggle. Gating this on `block_reclaim_enabled`
    /// left every deleted-but-still-reversible record unprotected whenever
    /// GC was off, so a compaction could discard the one physical entry an
    /// in-flight transaction's own later `abort()` needed to restore —
    /// confirmed as a real, repeatable crash (`DbTransaction::update`
    /// returning `ZeroAffected(KeyDoesNotExist)` for a row that a moment
    /// earlier `point()` had just read live) under sustained TPC-C load
    /// with GC disabled. `pub(crate)`: also called directly by
    /// `MVBTSt::on_acquire_reader_snapshot`/`on_release_reader_snapshot`
    /// (registering an already-known version as a reader, as opposed to
    /// `begin_snapshot`/`end_snapshot`, which draw a fresh one).
    #[inline]
    pub(crate) fn on_tx_start(&self, snapshot: SnapShot) {
        let worker_id = self.worker_id();
        // Pre-increment value, i.e. the depth *before* this call: 0 means
        // this worker had no live registration at all, so this is the
        // outermost one and must actually publish.
        let depth_before = self.live_tx_depth[worker_id as usize].fetch_add(1, Relaxed);
        if depth_before == 0 {
            let previous = self.live_tx[worker_id as usize].swap(snapshot, Release);
            debug_assert_eq!(
                previous, NOT_IN_FLIGHT,
                "TxContext::on_tx_start: worker {worker_id}'s outermost registration found \
                 snapshot {previous} already published for it — on_tx_start/on_tx_completed \
                 calls for one worker must be paired (unbalanced caller bug)"
            );
        }
    }

    #[inline]
    pub(crate) fn on_tx_completed(&self, snapshot: SnapShot) {
        let worker_id = self.worker_id();
        // Pre-decrement value: 1 means this completion brings the depth
        // back to 0, i.e. it's the outermost one and must actually clear.
        let depth_before = self.live_tx_depth[worker_id as usize].fetch_sub(1, Relaxed);
        debug_assert!(
            depth_before > 0,
            "TxContext::on_tx_completed: worker {worker_id} completed snapshot {snapshot} \
             with no live registration — on_tx_start/on_tx_completed calls for one worker \
             must be paired (unbalanced caller bug)"
        );
        if depth_before == 1 {
            let previous = self.live_tx[worker_id as usize].swap(NOT_IN_FLIGHT, Release);
            debug_assert_eq!(
                previous, snapshot,
                "TxContext::on_tx_completed: worker {worker_id}'s outermost completion \
                 expected snapshot {snapshot} but {previous} was published — \
                 on_tx_start/on_tx_completed calls for one worker don't nest correctly"
            );
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
            visibility::is_visible(
                &self.commit_logs,
                cache,
                reader_worker,
                reader_ts_start,
                stamp,
            )
        })
    }

    /// Same TLS `SnapshotCache` access `is_visible_stamp` uses, but hands
    /// back the raw `(cache, commit_logs)` pair instead of checking one
    /// stamp itself — so a caller that needs to check many stamps in one
    /// call (a scanned leaf page, a point/range query's candidate versions)
    /// can build its *own* `is_visible` closure directly in its own function
    /// body, ending up with a concrete, `Sized` closure type the compiler
    /// can inline, rather than paying for a `dyn FnMut` built on one side of
    /// a generic callback and invoked across it. Used by
    /// `mv_query::iter_query::RangeQueryIter::refill`'s leaf-scan hot loop
    /// and `mv_query::query`'s point/range reads (both can call `is_visible`
    /// once per physical record in a leaf, not just once per call).
    #[inline(always)]
    pub(crate) fn with_snapshot_cache_and_logs<R>(
        &self,
        f: impl FnOnce(&mut visibility::SnapshotCache, &[CommitLog]) -> R,
    ) -> R {
        crate::mv_sync::worker::with_snapshot_cache(&self.worker_registry, |cache| {
            f(cache, &self.commit_logs)
        })
    }

    /// Every worker's currently-published (i.e. outermost, see `live_tx`'s
    /// doc) live `ts_start`. `O(max_workers)`, each slot read independently
    /// with no cross-slot synchronization needed — same reasoning as
    /// `live_min_snapshot` below.
    #[inline]
    fn live_snapshots(&self) -> impl Iterator<Item = SnapShot> + '_ {
        self.live_tx.iter().filter_map(|slot| {
            let v = slot.load(Acquire);
            (v != NOT_IN_FLIGHT).then_some(v)
        })
    }

    /// Snapshots whose LCB commit-log entries must survive pruning. Besides
    /// fully-published transactions, include workers currently between
    /// drawing and publishing a snapshot; otherwise a concurrent commit can
    /// prune the boundary that the just-starting transaction will need.
    #[inline]
    fn pruning_snapshots(&self) -> impl Iterator<Item = SnapShot> + '_ {
        self.live_snapshots()
            .chain(self.in_flight_bound.iter().filter_map(|slot| {
                let v = slot.load(Acquire);
                (v != NOT_IN_FLIGHT).then_some(v)
            }))
    }

    #[inline(always)]
    pub(crate) fn commit_tx(&self, worker_id: WorkerId) -> Version {
        if self.block_reclaim_enabled.load(Relaxed)
            || self.freshest_si_truncate_commit_log.load(Relaxed)
        {
            self.commit_logs[worker_id as usize].commit_pruned(
                &self.global_clock,
                self.commit_logs.len(),
                self.pruning_snapshots(),
            )
        } else {
            self.commit_logs[worker_id as usize].commit(&self.global_clock)
        }
    }

    #[inline]
    pub(crate) fn newest_live_si(&self) -> Option<SnapShot> {
        self.live_snapshots().max()
    }

    /// Is `ts_start` a currently-registered (not yet committed/aborted)
    /// transaction? `MVBTSt::record_survives_gc` uses it to keep a *deleted*
    /// record physically present while its deleting transaction might
    /// still abort and need to reverse that delete — needed unconditionally,
    /// not just while `block_reclaim_enabled`, since it's protecting against
    /// SMO compaction (`MVBTSt::split`'s version-split path), which runs
    /// regardless of the GC toggle. See `on_tx_start`'s doc for the crash
    /// this being gated on the GC flag used to cause.
    #[inline]
    pub(crate) fn is_snapshot_live(&self, ts_start: Version) -> bool {
        self.live_tx
            .iter()
            .any(|slot| slot.load(Acquire) == ts_start)
    }

    /// The oldest currently-active-or-in-flight snapshot across every table
    /// sharing this context, or `None` if there are none — the "safe to
    /// reclaim anything dead strictly before this" bound
    /// `TrackerHandleSt::free_block` needs (see that method's doc: it must
    /// consult this *shared* bound, not a per-table one, since a snapshot
    /// registered once here may later read any table). Combines two sources:
    /// `live_tx` (fully-registered, possibly long-lived active transactions)
    /// and `in_flight_bound` (workers mid-registration right now — see that
    /// field's doc for why an `Acquire` load per slot is sufficient, no
    /// further cross-slot synchronization needed).
    ///
    /// Order matters: `in_flight_bound` MUST be read before `live_tx`, not
    /// after. A worker's slot only clears (back to `NOT_IN_FLIGHT`) *after*
    /// its `live_tx` insert has already happened (see
    /// `end_snapshot_registration`'s doc) — so observing a cleared slot via
    /// `Acquire` establishes happens-before with everything that preceded
    /// that clear on the writer, *including* its `live_tx` insert, making a
    /// *subsequent* `live_tx` read on this thread guaranteed to see it.
    /// Reading `live_tx` first has no such guarantee: it can race ahead of
    /// the writer and observe neither the insert (too early) nor the slot
    /// still holding its bound (already cleared by then) — a real gap this
    /// implementation hit under `tx_context_registration_tests.rs`'s
    /// regression test before the ordering was fixed here.
    #[inline]
    pub(crate) fn live_min_snapshot(&self) -> Option<SnapShot> {
        let mut min: Option<SnapShot> = None;
        for slot in &self.in_flight_bound {
            let bound = slot.load(Acquire);
            if bound != NOT_IN_FLIGHT {
                min = Some(min.map_or(bound, |m| m.min(bound)));
            }
        }

        for slot in &self.live_tx {
            let bound = slot.load(Acquire);
            if bound != NOT_IN_FLIGHT {
                min = Some(min.map_or(bound, |m| m.min(bound)));
            }
        }
        min
    }

    /// Test-only: current entry count of one worker's `CommitLog`, for tests
    /// confirming pruning keeps it bounded (or, with GC off, that it keeps
    /// growing) rather than exposing `commit_logs` itself.
    #[cfg(test)]
    pub(crate) fn commit_log_len(&self, worker_id: WorkerId) -> usize {
        self.commit_logs[worker_id as usize].len()
    }
}

// See `mv_test/mod.rs`'s `#[path]`-mod doc for why this file lives in
// `tests/` instead of next to this module.
#[cfg(test)]
#[path = "../../tests/tx_context_registration_tests.rs"]
mod tests;
