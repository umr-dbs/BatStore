use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_page_model::leaf_page::AbortOutcome;
use crate::bat_query::SnapShot;
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::Version;
use crate::bat_tree::mvbt::MVBTSt;
use std::fmt::Display;
use std::hash::Hash;

pub(crate) const START_VERSION: Version = 1;

#[cfg(debug_assertions)]
pub(crate) static ABORT_TERMINAL_WITH_REMAINING: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
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

    #[inline(always)]
    pub(crate) fn draw_snapshot_version_with<R>(&self, register: impl FnOnce(Version) -> R) -> R {
        self.ctx.draw_snapshot_version_with(register)
    }

    #[inline(always)]
    pub(crate) fn begin_snapshot(&self) -> Version {
        self.ctx.begin_snapshot()
    }

    /// Protects raw page references during a current-tree traversal without
    /// consuming a global OSIC timestamp.
    #[inline(always)]
    pub(crate) fn with_reclamation_pin<R>(&self, f: impl FnOnce() -> R) -> R {
        self.ctx.with_reclamation_pin(f)
    }

    #[inline(always)]
    pub(crate) fn end_snapshot(&self, ts_start: Version) {
        self.ctx.end_snapshot(ts_start);
    }

    /// Plain, worker-agnostic clock reads/ticks — used for structural
    /// root/page versioning (`bat_tree::smo`, `bat_wal::recovery`), which
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
        self.ctx
            .is_visible_stamp(reader_worker, reader_ts_start, stamp)
    }

    /// See `TxContext::with_snapshot_cache_and_logs`'s doc.
    #[inline(always)]
    pub(crate) fn with_snapshot_cache_and_logs<R>(
        &self,
        f: impl FnOnce(
            &mut crate::bat_sync::visibility::SnapshotCache,
            &[crate::bat_sync::commit_log::CommitLog],
        ) -> R,
    ) -> R {
        self.ctx.with_snapshot_cache_and_logs(f)
    }

    /// Commits `worker_id`'s in-flight transaction against this tree's
    /// `ctx` — see `TxContext::commit_tx` for the prune-vs-plain-commit
    /// choice.
    #[inline(always)]
    pub(crate) fn commit_tx(&self, worker_id: WorkerId) -> Version {
        self.ctx.commit_tx(worker_id)
    }

    #[inline(always)]
    pub fn wal_hardened_version(&self) -> Version {
        self.cold.wal.hardened_version()
    }

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
impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + crate::bat_wal::record::WalPayload,
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub(crate) fn wal_start_commit(
        &self,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> TxStamp {
        let worker_id = self.worker_id();

        match self.cold.wal.as_ref() {
            writer => match self.cold.table_id {
                Some(table_id) => writer.start_commit_logged_for_table(
                    table_id,
                    self.ctx.global_clock(),
                    worker_id,
                    build,
                ),
                None => writer.start_commit_logged(self.ctx.global_clock(), worker_id, build),
            },
        }
    }

    #[inline(always)]
    pub(crate) fn wal_log_write(
        &self,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) {
        match self.cold.table_id {
            Some(table_id) => {
                self.cold
                    .wal
                    .log_with_stamp_for_table(table_id, stamp, build);
            }
            None => {
                self.cold.wal.log_with_stamp(stamp, build);
            }
        }
    }

    #[inline(always)]
    pub(crate) fn wal_log_commit(&self, stamp: TxStamp, ts_commit: Version) {
        match self.cold.table_id {
            Some(_) => {
                self.cold.wal.log_commit_for_table(stamp, ts_commit);
            }
            None => {
                self.cold.wal.log_commit(stamp, ts_commit);
            }
        }
    }

    #[inline]
    pub(crate) fn abort_write(&self, key: Key, stamp: TxStamp) {
        self.abort_writes(key, stamp, 1);
    }

    #[inline]
    pub(crate) fn abort_writes(&self, key: Key, stamp: TxStamp, count: usize) {
        let mut remaining = count;
        while remaining != 0 {
            let leaf_guard = self.traversal_write_olc_registered(key);
            let leaf_deref_mut = leaf_guard.deref_mut();
            let leaf_page = leaf_deref_mut.as_leaf_page();
            let outcome = leaf_page.abort_write(key, stamp);

            if outcome == AbortOutcome::NotFound {
                // Genuinely nothing anywhere -- matches today's terminal
                // (already-fully-reverted, or count was already 0) case.
                #[cfg(debug_assertions)]
                if remaining > 1 {
                    ABORT_TERMINAL_WITH_REMAINING
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    eprintln!(
                        "[abort-diag] terminal NotFound with remaining={remaining} key={key} stamp={stamp}"
                    );
                }
                break;
            }

            remaining -= 1;
        }
    }
}
