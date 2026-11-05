//! Which deleted versions a structure modification must carry into the
//! leaves it creates.
//!
//! Two kinds of deleted versions must survive a split or merge:
//!
//! * A version whose deleting transaction has not committed. If it aborts,
//!   its rollback must find the version to undelete it.
//! * The last version of a deleted key, while some active transaction cannot
//!   see the deletion. A writer reaches a key only on the newest route, so
//!   this version is the only trace of the delete that such a transaction can
//!   meet. If the transaction inserts the key, first-committer-wins requires
//!   it to meet the deletion stamp and abort.
//!
//! Every other deleted version is dropped once its deleter has committed. A
//! version ended by an update needs no such protection: the new version
//! follows it in the same leaf and carries the updater's stamp, so a
//! concurrent writer conflicts with that one. Keeping only the last version
//! of a deleted key bounds the extra versions by the number of deleted keys;
//! the before-images of a frequently updated key do not accumulate.
//!
//! Visibility only grows with the snapshot (`LCB(w, t)` never decreases as
//! `t` grows), so a deletion that is visible at the oldest active snapshot is
//! visible to every active transaction. The rule needs one bound per
//! structure modification, `oldest`, and at most two commit-log queries per
//! worker.

use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::{Version, VersionInfo};
use crate::bat_sync::tx_context::TxContext;

/// Commit-log answers for one worker, computed at most once per modification.
struct WorkerAnswers {
    worker: WorkerId,
    /// The worker's last commit, or `0`.
    last_commit: Option<Version>,
    /// `LCB(worker, oldest)`.
    lcb_at_oldest: Option<Version>,
}

/// The retention rule of one structure modification. Create one per
/// modification, after its latches are held, and ask it about every record
/// of the nodes it replaces.
pub(crate) struct RetentionHorizon<'a> {
    ctx: &'a TxContext,
    /// The oldest snapshot that is live or still being registered, read
    /// lazily; `Some(None)` when there is none.
    oldest: Option<Option<Version>>,
    /// A leaf holds the deletions of few workers, so a short list beats a map.
    answers: Vec<WorkerAnswers>,
}

impl<'a> RetentionHorizon<'a> {
    #[inline]
    pub(crate) fn new(ctx: &'a TxContext) -> Self {
        Self {
            ctx,
            oldest: None,
            answers: Vec::new(),
        }
    }

    /// Whether a version must be copied into the new leaves. A version
    /// created by a rolled-back transaction is never copied, deleted or not.
    /// `is_newest_valid_of_key` tells whether no newer valid version of the
    /// same key follows it in its leaf; it is evaluated only when needed.
    #[inline]
    pub(crate) fn keeps(
        &mut self,
        version: &VersionInfo,
        is_newest_valid_of_key: impl FnOnce() -> bool,
    ) -> bool {
        if version.insertion_stamp().is_invalid() {
            // Created by a rolled-back transaction; nobody can see it.
            return false;
        }
        let Some(deletion) = version.deletion_stamp() else {
            return true;
        };
        if !self.has_committed(deletion) {
            // The deleter may still abort, and its rollback needs the version.
            return true;
        }
        if !is_newest_valid_of_key() {
            // Ended by an update; the newer version carries the conflict.
            return false;
        }
        self.deletion_invisible_at_oldest(deletion)
    }

    /// Whether the transaction that wrote `stamp` has committed. A worker runs
    /// one transaction at a time, so a commit of the worker after the stamp's
    /// start means the transaction has ended. A transaction that aborted has
    /// cleared its deletion stamps before its worker commits again.
    #[inline]
    fn has_committed(&mut self, stamp: TxStamp) -> bool {
        let ctx = self.ctx;
        let answers = self.answers_for(stamp.worker_id());
        let last = *answers
            .last_commit
            .get_or_insert_with(|| ctx.lcb(stamp.worker_id(), Version::MAX));
        last > stamp.ts_start()
    }

    /// Whether some active transaction cannot see this committed deletion,
    /// i.e. the deletion is invisible at the oldest active snapshot.
    #[inline]
    fn deletion_invisible_at_oldest(&mut self, deletion: TxStamp) -> bool {
        let ctx = self.ctx;
        let Some(oldest) = *self.oldest.get_or_insert_with(|| ctx.live_min_snapshot()) else {
            // No transaction is active, and none is registering: every
            // future snapshot is drawn after the deletion committed.
            return false;
        };
        // The deleter started at or after `oldest`, so it committed after
        // `oldest`: invisible there.
        if deletion.ts_start() >= oldest {
            return true;
        }
        // Otherwise the deletion is visible at `oldest` iff its worker
        // committed after the deleter's start and before `oldest`.
        let answers = self.answers_for(deletion.worker_id());
        let lcb = *answers
            .lcb_at_oldest
            .get_or_insert_with(|| ctx.lcb(deletion.worker_id(), oldest));
        lcb <= deletion.ts_start()
    }

    #[inline]
    fn answers_for(&mut self, worker: WorkerId) -> &mut WorkerAnswers {
        let index = match self.answers.iter().position(|a| a.worker == worker) {
            Some(index) => index,
            None => {
                self.answers.push(WorkerAnswers {
                    worker,
                    last_commit: None,
                    lcb_at_oldest: None,
                });
                self.answers.len() - 1
            }
        };
        &mut self.answers[index]
    }
}
