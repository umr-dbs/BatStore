use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::Ordering::Relaxed;
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

    /// Every write's WAL record (if any) is handed to its shard and *never
    /// waited on* by the write itself — `dispatch_crud`/`Transaction::commit`
    /// return as soon as `commit_tx` makes the write visible, regardless of
    /// whether (or when) it's actually fsynced. There is deliberately no
    /// per-op durability wait anymore (previously: Early Lock Release,
    /// paper §3.4 — commit visibility, then block for this op's own flush
    /// plus every dependency's flush before returning). That gave every
    /// single-op caller a crash-durability guarantee at the cost of paying a
    /// flush round-trip on every op; callers who actually need a durability
    /// point-in-time now ask for one explicitly via `wal_hardened_version`/
    /// `wait_wal_hardened` below instead of every op paying for it.
    ///
    /// The minimum hardened watermark across every WAL shard (see
    /// `WalWriter::hardened_version`): every write with `ts_start` at or
    /// below this value, on every worker, is confirmed durably fsynced.
    /// Advances in batches as each shard's background thread completes a
    /// flush, not per operation. `0` when no WAL is attached (or one is
    /// attached but nothing has flushed yet) — nothing is guaranteed
    /// durable, so callers polling this get an honest "not yet" instead of
    /// a stale/optimistic value.
    #[inline(always)]
    pub fn wal_hardened_version(&self) -> Version {
        match self.wal.load().as_ref() {
            Some(shards) => shards.iter().map(|w| w.hardened_version()).min().unwrap_or(0),
            None => 0,
        }
    }

    /// Blocks until `wal_hardened_version()` reaches `target` — i.e. until
    /// every write up to that point is confirmed durable. For an explicit,
    /// caller-chosen checkpoint only (e.g. "durability-sync before reporting
    /// a batch job done"); never called automatically by the write path
    /// itself (see `wal_hardened_version`'s doc). Polls on a short sleep
    /// rather than busy-spinning: unlike the old per-op ELR wait (usually
    /// zero-iteration), this can legitimately span multiple flush intervals,
    /// so spinning would just burn CPU for no benefit.
    pub fn wait_wal_hardened(&self, target: Version) {
        while self.wal_hardened_version() < target {
            std::thread::sleep(std::time::Duration::from_micros(100));
        }
    }
}

/// Split from the block above: these two methods are the only ones that
/// actually encode a record onto the `WalWriter`, so only they need
/// `Payload: WalPayload` — every other method here (snapshots, commit log,
/// visibility, ...) stays usable for any `Payload`.
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
    /// sorting records by `ts_start` before applying them. Fire-and-forget:
    /// the caller never waits on this write's flush (see
    /// `MVBTSt::wal_hardened_version`'s doc), so there's no ticket to return
    /// here — just the stamp. `build` is only ever called with
    /// `CRUDOperation::Insert`/`Update`/`Delete`.
    #[inline(always)]
    pub(crate) fn wal_start_commit(
        &self,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> TxStamp {
        let worker_id = self.worker_id();

        // `wal_ever_enabled` lets a tree that has never had a WAL skip
        // `ArcSwapOption::load` entirely instead of paying its guard
        // mechanism on every write just to find `None` — see the field doc.
        if !self.wal_ever_enabled.load(Relaxed) {
            return TxStamp::new(worker_id, self.global_clock.next_timestamp());
        }

        match self.wal.load().as_ref() {
            Some(shards) => shards[worker_id as usize]
                .start_commit_logged(&self.global_clock, worker_id, build).0,
            None => TxStamp::new(worker_id, self.global_clock.next_timestamp()),
        }
    }

    /// Logs one write for a multi-op `mv_query::transaction::Transaction`
    /// under `stamp` — the transaction's *own* `(worker_id, ts_start)`, not
    /// a freshly-minted one, since every write in the same transaction must
    /// share its one `ts_start` (see `WalWriter::log_with_stamp`). No-op
    /// when no WAL is attached; fire-and-forget otherwise, same as
    /// `wal_start_commit`.
    #[inline(always)]
    pub(crate) fn wal_log_write(
        &self,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) {
        if !self.wal_ever_enabled.load(Relaxed) {
            return;
        }

        if let Some(shards) = self.wal.load().as_ref() {
            shards[stamp.worker_id() as usize].log_with_stamp(stamp, build);
        }
    }
}