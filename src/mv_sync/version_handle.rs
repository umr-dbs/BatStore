use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::Ordering::Relaxed;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_query::SnapShot;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::Version;
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
        self.ctx.on_tx_start(snapshot);
    }

    #[inline]
    pub(crate) fn on_release_reader_snapshot(&self, snapshot: SnapShot) {
        self.ctx.on_tx_completed(snapshot);
    }

    /// This thread's stable `WorkerId` for this tree's `ctx` (shared or
    /// private — see `TxContext`'s doc), lazily assigned on first use.
    #[inline(always)]
    pub(crate) fn worker_id(&self) -> WorkerId {
        self.ctx.worker_id()
    }

    /// Draws a fresh `ts_start` and hands it to `register`, which must be
    /// whatever actually records it as a protected reader — see
    /// `TxContext::draw_snapshot_version_with` for why this is gap-free
    /// against concurrent block reclaim.
    #[inline(always)]
    pub(crate) fn draw_snapshot_version_with<R>(&self, register: impl FnOnce(Version) -> R) -> R {
        self.ctx.draw_snapshot_version_with(register)
    }

    /// Draws a fresh OSIC snapshot (`ts_start`) and registers it as an
    /// active transaction against this tree's `ctx` — callers must pair this
    /// with `end_snapshot` (or `commit_tx`, which does so as part of
    /// committing) once the transaction is done, so `CommitLog` pruning
    /// never drops an entry this snapshot's future `LCB` queries still need.
    #[inline(always)]
    pub(crate) fn begin_snapshot(&self) -> Version {
        self.ctx.begin_snapshot()
    }

    #[inline(always)]
    pub(crate) fn end_snapshot(&self, ts_start: Version) {
        self.ctx.end_snapshot(ts_start);
    }

    /// Plain, worker-agnostic clock reads/ticks — used for structural
    /// root/page versioning (`mv_tree::smo`, `mv_wal::recovery`), which
    /// stays outside OSIC's per-record `TxStamp` scheme.
    #[inline(always)]
    pub(crate) fn current_version(&self) -> Version {
        self.ctx.current_version()
    }

    #[inline(always)]
    pub(crate) fn start_tx_commit(&self) -> Version {
        self.ctx.start_tx_commit()
    }

    /// OSIC visibility check (Listing 1): is `stamp` visible to a reader on
    /// `reader_worker` whose snapshot is `reader_ts_start`?
    #[inline(always)]
    pub(crate) fn is_visible_stamp(
        &self,
        reader_worker: WorkerId,
        reader_ts_start: Version,
        stamp: TxStamp,
    ) -> bool {
        self.ctx.is_visible_stamp(reader_worker, reader_ts_start, stamp)
    }

    /// Same OSIC check as `is_visible_stamp`, but for a whole batch of
    /// records (a scanned leaf page, a point-query's candidate versions)
    /// rather than one stamp — see `TxContext::with_visibility_checker`.
    #[inline(always)]
    pub(crate) fn with_visibility_checker<R>(
        &self,
        reader_worker: WorkerId,
        reader_ts_start: Version,
        f: impl FnOnce(&mut dyn FnMut(TxStamp) -> bool) -> R,
    ) -> R {
        self.ctx.with_visibility_checker(reader_worker, reader_ts_start, f)
    }

    /// Commits `worker_id`'s in-flight transaction against this tree's
    /// `ctx` — see `TxContext::commit_tx` for the prune-vs-plain-commit
    /// choice.
    #[inline(always)]
    pub(crate) fn commit_tx(&self, worker_id: WorkerId) -> Version {
        self.ctx.commit_tx(worker_id)
    }

    /// Every write's WAL record (if any) is handed to the writer and *never
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
    /// This tree's WAL durability watermark (see
    /// `WalWriter::hardened_version`): every write with `ts_start` at or
    /// below this value is confirmed durably fsynced. Advances in batches
    /// as the writer's background thread completes a flush, not per
    /// operation. `0` when no WAL is attached (or one is attached but
    /// nothing has flushed yet) — nothing is guaranteed durable, so callers
    /// polling this get an honest "not yet" instead of a stale/optimistic
    /// value.
    #[inline(always)]
    pub fn wal_hardened_version(&self) -> Version {
        match self.wal.load().as_ref() {
            Some(writer) => writer.hardened_version(),
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
    /// sorting records by `ts_commit` before applying them. Fire-and-forget:
    /// the caller never waits on this write's flush (see
    /// `MVBTSt::wal_hardened_version`'s doc), so there's no ticket to return
    /// here — just the stamp. `build` is only ever called with
    /// `CRUDOperation::Insert`/`Update`/`Delete`.
    ///
    /// This write is logged optimistically, before its transaction is known
    /// to commit — callers must follow up with `wal_log_commit` once (and
    /// only once) they've actually committed it, or replay will correctly
    /// never see this write at all (see `WalEntry::Commit`'s doc).
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
            return TxStamp::new(worker_id, self.ctx.start_tx_commit());
        }

        match self.wal.load().as_ref() {
            // `self.table_id` is `Some` only for a `mv_db::Database` table
            // (see `MVBTSt::table_id`'s doc) — its writer is shared with
            // every other table on that database, so every entry must carry
            // this table's id for `mv_wal::recovery::replay_database` to
            // demultiplex the interleaved file. `None` (every other caller,
            // including `TpccDatabase`'s own per-table files) keeps today's
            // plain, untagged encoding, byte-for-byte unchanged.
            Some(writer) => match self.table_id {
                Some(table_id) => writer.start_commit_logged_for_table(table_id, self.ctx.global_clock(), worker_id, build),
                None => writer.start_commit_logged(self.ctx.global_clock(), worker_id, build),
            },
            None => TxStamp::new(worker_id, self.ctx.start_tx_commit()),
        }
    }

    /// Logs one write for a multi-op `mv_db::transaction::DbTransaction`
    /// under `stamp` — the transaction's *own* `(worker_id, ts_start)`, not
    /// a freshly-minted one, since every write in the same transaction must
    /// share its one `ts_start` (see `WalWriter::log_with_stamp`). No-op
    /// when no WAL is attached; fire-and-forget otherwise, same as
    /// `wal_start_commit` — including needing a matching `wal_log_commit`
    /// once the transaction actually commits.
    #[inline(always)]
    pub(crate) fn wal_log_write(
        &self,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) {
        if !self.wal_ever_enabled.load(Relaxed) {
            return;
        }

        if let Some(writer) = self.wal.load().as_ref() {
            match self.table_id {
                Some(table_id) => { writer.log_with_stamp_for_table(table_id, stamp, build); }
                None => { writer.log_with_stamp(stamp, build); }
            }
        }
    }

    /// Logs a **Commit marker** confirming `stamp`'s transaction actually
    /// committed at `ts_commit` — see `WalEntry::Commit`'s doc. Must be
    /// called exactly once, after `commit_tx` has actually succeeded, for
    /// every write previously logged via `wal_start_commit`/`wal_log_write`
    /// under this `stamp`; replay only ever applies a write once it finds
    /// this marker. No-op when no WAL is attached; fire-and-forget
    /// otherwise, same model as every other WAL call here.
    #[inline(always)]
    pub(crate) fn wal_log_commit(&self, stamp: TxStamp, ts_commit: Version) {
        if !self.wal_ever_enabled.load(Relaxed) {
            return;
        }

        if let Some(writer) = self.wal.load().as_ref() {
            match self.table_id {
                Some(_) => { writer.log_commit_for_table(stamp, ts_commit); }
                None => { writer.log_commit(stamp, ts_commit); }
            }
        }
    }

    /// Reverts `key`'s write by the transaction identified by `stamp` — see
    /// `mv_page_model::leaf_page::LeafPage::abort_write`'s doc for the two
    /// cases (`Invalidate` an `Insert`/`Update`, or `Undelete` a plain
    /// `Delete`). Called once per key a `mv_db::transaction::DbTransaction`/
    /// `mv_bench::tpcc_txn::TpccTxn` touched, from `Drop` when it's dropped
    /// without `commit()`. Purely an in-memory reversal — `stamp`'s
    /// transaction never committed, so it never got (and never will get) a
    /// `wal_log_commit` marker for whatever `wal_start_commit`/
    /// `wal_log_write` already logged; replay skips it for that reason
    /// alone, with no separate WAL-side abort record needed here.
    #[inline]
    pub(crate) fn abort_write(&self, key: Key, stamp: TxStamp) {
        let leaf_guard = self.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        leaf_page.abort_write(key, stamp);
    }
}
