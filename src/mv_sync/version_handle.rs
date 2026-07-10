use std::fmt::Display;
use std::hash::Hash;
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
        self.tracker()
            .inspect(|tracker|
                tracker.on_tx_start(snapshot));
    }

    #[inline]
    pub(crate) fn on_release_reader_snapshot(&self, snapshot: SnapShot) {
        self.tracker()
            .inspect(|tracker|
                tracker.on_tx_completed(snapshot));
    }

    /// This thread's stable `WorkerId` for this tree instance, lazily
    /// assigned from the tree's fixed `WorkerRegistry` on first use (see
    /// `mv_sync::worker`).
    #[inline(always)]
    pub(crate) fn worker_id(&self) -> WorkerId {
        crate::mv_sync::worker::worker_id_for(&self.worker_registry)
    }

    /// Draws a fresh OSIC snapshot (`ts_start`) and registers it as an
    /// active transaction — callers must pair this with `end_snapshot` (or
    /// `commit_tx`, which does so as part of committing) once the
    /// transaction is done, so `CommitLog` pruning never drops an entry this
    /// snapshot's future `LCB` queries still need.
    #[inline(always)]
    pub(crate) fn begin_snapshot(&self) -> Version {
        let ts_start = self.global_clock.next_timestamp();
        self.on_acquire_reader_snapshot(ts_start);
        ts_start
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

    /// Commits `worker_id`'s in-flight transaction: draws `ts_commit` from
    /// the GLC and appends it to that worker's `CommitLog` — the entire
    /// "instant commit" (no write-set revisit). Pruned against every active
    /// snapshot when GC/tracking is enabled; left to grow unboundedly
    /// otherwise, since pruning without a complete active-snapshot view
    /// could drop an entry a live-but-untracked reader still needs.
    #[inline(always)]
    pub(crate) fn commit_tx(&self, worker_id: WorkerId) -> Version {
        match self.tracker() {
            Some(tracker) => self.commit_logs[worker_id as usize].commit_pruned(
                &self.global_clock,
                self.commit_logs.len(),
                tracker.active_snapshots(),
            ),
            None => self.commit_logs[worker_id as usize].commit(&self.global_clock),
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
        self.wal.load().as_ref().map(|shards| shards[stamp.worker_id as usize].log_with_stamp(stamp, build))
    }
}