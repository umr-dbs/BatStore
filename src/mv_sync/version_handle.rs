use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::Ordering::Relaxed;
use crossbeam_channel::Receiver;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_query::SnapShot;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::Version;
use crate::mv_sync::visibility;
use crate::mv_tree::mvbt::MVBTSt;

pub(crate) const START_VERSION: Version = 1;

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    pub(crate) fn on_acquire_reader_snapshot(&self, snapshot: SnapShot) {
        self.tracker().on_tx_start(snapshot);
    }

    #[inline]
    pub(crate) fn on_release_reader_snapshot(&self, snapshot: SnapShot) {
        self.tracker().on_tx_completed(snapshot);
    }

    /// This thread's stable `WorkerId` for this tree instance, lazily
    /// assigned from the tree's fixed `WorkerRegistry` on first use (see
    /// `mv_sync::worker`).
    #[inline(always)]
    pub(crate) fn worker_id(&self) -> WorkerId {
        crate::mv_sync::worker::worker_id_for(&self.worker_registry)
    }

    /// Draws a fresh `ts_start` and hands it to `register`, which must be
    /// whatever actually records it as a protected reader — either directly
    /// via `on_acquire_reader_snapshot` (as `begin_snapshot` does), or
    /// indirectly, e.g. `mv_query::dispatch`'s `RangeIterSi` arm, where
    /// `RangeQueryIter::new`'s own `register_reader_si: true` path does the
    /// registration instead (so that *iterator* — not `RangeIterSi` itself —
    /// owns releasing it later, on completion or drop).
    ///
    /// Drawing `ts_start` (an immediate side effect other threads can
    /// observe via `current_version()`) and registering it as protected are
    /// two separate steps with a real gap between them: a concurrent GC
    /// decision (`mv_gc::tracker_handle::TrackerHandleSt::free_block`)
    /// running in that gap can't yet see this reader and may reclaim a page
    /// it's about to need (this is what caused
    /// `mv_query::query::traverse_read_key`'s "no matching entry" failures
    /// under sustained concurrent GC — see that function's doc).
    /// `registrations_in_flight` closes it: bumped before the draw, dropped
    /// only after `register` returns (i.e. after the registration it's
    /// responsible for has actually happened), and `free_block` refuses to
    /// reclaim anything at all while it's nonzero — so no reclaim decision
    /// can ever be made in the exact window this draw-then-register
    /// sequence is unprotected.
    ///
    /// (An earlier version of this fix instead pre-registered a
    /// `current_version()`-based provisional lower bound in `live_tx`
    /// itself before drawing the real `ts_start`, upgrading afterward.
    /// That's also correct, but measured 6-9x slower under sustained
    /// concurrent load: it doubles/triples skip-list insertions at
    /// `live_tx`'s monotonically-growing, therefore always-contended, tail
    /// on every single snapshot. This one shared counter is far cheaper —
    /// no allocation, no per-value skip-list entry — at the cost of making
    /// `free_block` briefly, harmlessly more conservative than strictly
    /// necessary while *any* thread is mid-registration, not just when one
    /// affecting a specific dead block is.)
    #[inline(always)]
    pub(crate) fn draw_snapshot_version_with<R>(&self, register: impl FnOnce(Version) -> R) -> R {
        let tracker = self.tracker();
        tracker.begin_snapshot_registration();

        let ts_start = self.global_clock.next_timestamp();
        let result = register(ts_start);

        tracker.end_snapshot_registration();

        result
    }

    /// Draws a fresh OSIC snapshot (`ts_start`) and registers it as an
    /// active transaction — callers must pair this with `end_snapshot` (or
    /// `commit_tx`, which does so as part of committing) once the
    /// transaction is done, so `CommitLog` pruning never drops an entry this
    /// snapshot's future `LCB` queries still need. See
    /// `draw_snapshot_version_with` for why this is gap-free.
    #[inline(always)]
    pub(crate) fn begin_snapshot(&self) -> Version {
        self.draw_snapshot_version_with(|ts_start| {
            self.on_acquire_reader_snapshot(ts_start);
            ts_start
        })
    }

    #[inline(always)]
    pub(crate) fn end_snapshot(&self, ts_start: Version) {
        self.on_release_reader_snapshot(ts_start);
    }

    /// Plain, worker-agnostic clock reads/ticks — used for structural
    /// root/page versioning (`mv_tree::smo`, `mv_wal::recovery`), which
    /// stays outside OSIC's per-record `TxStamp` scheme (see
    /// `GlobalClock::current_version`/`next_timestamp` for why that's safe).
    #[inline(always)]
    pub(crate) fn current_version(&self) -> Version {
        self.global_clock.current_version()
    }

    #[inline(always)]
    pub(crate) fn start_tx_commit(&self) -> Version {
        self.global_clock.next_timestamp()
    }

    /// OSIC visibility check (Listing 1): is `stamp` visible to a reader on
    /// `reader_worker` whose snapshot is `reader_ts_start`? The cache used
    /// is always the *calling* thread's own (see
    /// `mv_sync::worker::with_snapshot_cache`) — callers are expected to
    /// pass their own `worker_id()` as `reader_worker`.
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

    /// Same OSIC check as `is_visible_stamp`, but for a whole batch of
    /// records (a scanned leaf page, a point-query's candidate versions)
    /// rather than one stamp: fetches this thread's `SnapshotCache` *once*
    /// (`with_snapshot_cache`'s thread-local `.with()` + `RefCell::
    /// borrow_mut()` + `HashMap::entry` lookup) and hands `f` a closure that
    /// reuses it for every record, instead of `is_visible_stamp` paying that
    /// lookup again per record. Matters most for range/OLAP scans, which
    /// call this once per visited leaf page but then check visibility for
    /// every record (up to two checks each, insert + delete stamp) in it.
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

    /// Commits `worker_id`'s in-flight transaction: draws `ts_commit` from
    /// the GLC and appends it to that worker's `CommitLog` — the entire
    /// "instant commit" (no write-set revisit). Prunes against every active
    /// snapshot (`tracker().active_snapshots()`) only while block-reclaim GC
    /// is enabled — see `TrackerHandleSt`'s type doc for why pruning isn't
    /// sound otherwise (it can silently corrupt visibility for an explicit
    /// historical read at an old `version` nobody's held a live snapshot on
    /// since, once the dead records it needs outlive the pruned `LCB` data
    /// that would've explained them). With GC off, falls back to the plain,
    /// never-pruning `CommitLog::commit`, so each worker's log grows
    /// unboundedly for the run's lifetime — the same tradeoff a GC-off tree
    /// has always accepted, not a bug to paper over by pruning anyway.
    #[inline(always)]
    pub(crate) fn commit_tx(&self, worker_id: WorkerId) -> Version {
        let tracker = self.tracker();
        if tracker.block_reclaim_enabled() {
            self.commit_logs[worker_id as usize].commit_pruned(
                &self.global_clock,
                self.commit_logs.len(),
                tracker.active_snapshots(),
            )
        } else {
            self.commit_logs[worker_id as usize].commit(&self.global_clock)
        }
    }

    /// Blocks until the WAL record this `ticket` (from `wal_start_commit`)
    /// belongs to has been durably fsynced. No-op when `ticket` is `None`
    /// (WAL disabled).
    #[inline(always)]
    pub(crate) fn wal_wait_flush(&self, ticket: Option<Receiver<()>>) {
        if let Some(ticket) = ticket {
            let _ = ticket.recv();
        }
    }

    /// Early Lock Release, step 1 (paper §3.4): commits `worker_id`'s write
    /// — making it visible via the CommitLog — *before* its WAL entry (if
    /// any) is confirmed flushed, removing the flush latency from the
    /// critical path of visibility. If this write was logged (`ticket` is
    /// `Some`), immediately caps that worker's hardened watermark just
    /// below the new `ts_commit` (`CommitLog::mark_pending`), so no
    /// concurrent transaction can mistake this still-in-flight write for
    /// already-durable before `finish_elr_commit` raises it back. Call
    /// *before* waiting on `ticket`; returns `ts_commit`.
    #[inline(always)]
    pub(crate) fn commit_tx_elr(&self, worker_id: WorkerId, ticket: &Option<Receiver<()>>) -> Version {
        let ts_commit = self.commit_tx(worker_id);
        if ticket.is_some() {
            self.commit_logs[worker_id as usize].mark_pending(ts_commit);
        }
        ts_commit
    }

    /// Early Lock Release, step 2: call after waiting on the same `ticket`
    /// passed to `commit_tx_elr` (i.e. once this write's WAL entry, if any,
    /// is confirmed flushed). Resolves `worker_id`'s pending marker (see
    /// `CommitLog::mark_resolved`), then blocks until the *global* durability
    /// watermark has caught up to `reader_ts` — this write's own
    /// snapshot/stamp — i.e. until everything this write could have read
    /// is itself confirmed durable too (dependency tracking, paper §3.4:
    /// "we have to make sure that all transactions we read from are durable
    /// when we signal the commit"). No-op when this write was never logged
    /// (`logged = false`) — a write with no WAL attached has no durability
    /// contract to keep, so nothing to wait for.
    #[inline(always)]
    pub(crate) fn finish_elr_commit(&self, worker_id: WorkerId, reader_ts: Version, logged: bool) {
        if !logged {
            return;
        }
        self.commit_logs[worker_id as usize].mark_resolved();
        while self.durability_watermark() < reader_ts {
            std::thread::yield_now();
        }
    }

    /// The minimum hardened watermark across every worker (see
    /// `CommitLog`'s field doc) — the global point below which every
    /// commit, on every worker, is confirmed WAL-durable. `Version::MAX`
    /// (unconstrained) when no worker has ever logged a write, e.g. no WAL
    /// attached at all.
    #[inline(always)]
    pub(crate) fn durability_watermark(&self) -> Version {
        self.commit_logs.iter().map(|cl| cl.hardened()).min().unwrap_or(Version::MAX)
    }
}

/// Split from the block above: these two methods are the only ones that
/// actually encode a record onto the `WalWriter`, so only they need
/// `Payload: WalPayload` — every other method here (snapshots, commit log,
/// visibility, `wal_wait_flush`, ...) stays usable for any `Payload`.
impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + crate::mv_wal::record::WalPayload
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Mints a fresh OSIC stamp for a single, auto-committing write (the
    /// existing `Insert`/`Update`/`Delete` dispatch arms — each one *is* a
    /// one-operation transaction) and, if a WAL is attached, hands
    /// `build(ts_start)` to it for logging. Concurrent commits can land in
    /// the log in either order regardless of which timestamp is numerically
    /// smaller; `replay` (see `mv_wal::recovery`) accounts for this by
    /// sorting records by `ts_start` before applying them. Returns the
    /// stamp and a flush ticket to pass to `wal_wait_flush` — `None` when no
    /// WAL is attached. `build` is only ever called with
    /// `CRUDOperation::Insert`/`Update`/`Delete`.
    #[inline(always)]
    pub(crate) fn wal_start_commit(
        &self,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> (TxStamp, Option<Receiver<()>>) {
        let worker_id = self.worker_id();

        // `wal_ever_enabled` lets a tree that has never had a WAL skip
        // `ArcSwapOption::load` entirely instead of paying its guard
        // mechanism on every write just to find `None` — see the field doc.
        if !self.wal_ever_enabled.load(Relaxed) {
            return (TxStamp::new(worker_id, self.global_clock.next_timestamp()), None);
        }

        match self.wal.load().as_ref() {
            Some(shards) => {
                let (stamp, ticket)
                    = shards[worker_id as usize].start_commit_logged(&self.global_clock, worker_id, build);

                (stamp, Some(ticket))
            }
            None => (TxStamp::new(worker_id, self.global_clock.next_timestamp()), None),
        }
    }

    /// Logs one write for a multi-op `mv_query::transaction::Transaction`
    /// under `stamp` — the transaction's *own* `(worker_id, ts_start)`, not
    /// a freshly-minted one, since every write in the same transaction must
    /// share its one `ts_start` (see `WalWriter::log_with_stamp`). No-op
    /// (returns `None`) when no WAL is attached.
    #[inline(always)]
    pub(crate) fn wal_log_write(
        &self,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> Option<Receiver<()>> {
        if !self.wal_ever_enabled.load(Relaxed) {
            return None;
        }

        self.wal.load().as_ref().map(|shards| shards[stamp.worker_id() as usize].log_with_stamp(stamp, build))
    }
}