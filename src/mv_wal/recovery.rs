use std::fmt::Display;
use std::fs;
use std::hash::Hash;
use std::io;
use std::path::{Path, PathBuf};
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::tx_stamp::TxStamp;
use crate::mv_record_model::version_info::VersionInfo;
use crate::mv_tree::mvbt::{wal_shard_path, MVBTSt};
use crate::mv_wal::record::{self, WalRecord};

/// Upper bound on how many per-worker WAL shard files `replay` looks for at
/// `{base_path}.0000`, `{base_path}.0001`, ... A generous, fixed bound
/// (rather than trusting the *recovering* tree's own `max_workers`) because
/// a log written on a machine with more cores than the one recovering it
/// must still not silently drop the higher-numbered workers' shards.
/// `fs::read` on a missing path is a cheap syscall, so scanning past however
/// many shards actually exist costs a handful of failed opens, not real work.
const MAX_WAL_SHARDS_TO_SCAN: usize = 1024;

/// Scans the log at `path` and replays every well-formed record into `tree`
/// through the exact same traversal/mutation primitives the live write path
/// uses — structural repairs (splits/merges) are not logged individually,
/// and replay does not try to reproduce them, or the original absolute
/// version numbers, exactly. Instead:
///
/// - Records are sorted by their **original** logged version before
///   replaying. That version comes from a single global monotonic clock, so
///   even though the lock-free writer (see `mv_wal::writer`) can let two
///   concurrent commits land in the file in either byte order, the recorded
///   version numbers still capture the true causal/commit order between
///   logged operations. Replay reconstructs that order by sorting, not by
///   trusting file byte order.
/// - Each op is then replayed through the normal dispatch-equivalent path
///   (traversal-with-proactive-repair, mutate, commit), letting a **fresh**
///   version get minted for it — no attempt is made to match the original
///   number. Structural repairs fire naturally as a side effect, at the
///   correct *relative* points, because MVBT's repair rule ("fix an unsafe
///   node the moment any traversal passes through it") is a purely local,
///   deterministic function of tree structure — it doesn't depend on global
///   version bookkeeping. So replaying the same sequence of logical
///   operations in the same causal order reconstructs a **logically
///   equivalent** tree (same live keys/values, same relative multi-version
///   history), just addressed by a fresh, contiguous version numbering
///   rather than the original one.
///
/// This is a deliberate trade-off: a client that remembered "my write
/// returned version 57" cannot assume `Point(key, 57)` still means the same
/// thing after a crash + recovery. What's preserved is the *logical*
/// content and its relative history, not absolute version identity.
///
/// (An earlier version of this design tried to preserve exact version
/// numbers by asserting each replayed op landed on its original version and
/// stopping otherwise. That doesn't actually work under real concurrency:
/// structural repairs consume real version numbers from threads *other*
/// than the one whose logged op is being replayed, in a relative position a
/// single-threaded replay can't reconstruct — so the assertion could fail
/// on essentially the first record replayed after any genuine concurrent
/// write load. Dropping the exact-version requirement removes the need for
/// that check entirely, and with it, the two "known limitations" that used
/// to be documented here.)
///
/// Stops scanning (not replaying — every record it *did* read is still
/// replayed) at the first invalid/incomplete frame: a torn write from a
/// crash mid-fsync is expected, not an error.
///
/// Every worker logs to its own shard file (see `mv_tree::mvbt::wal_shard_path`
/// and its module doc), so replay first merges records across *all* shards —
/// by their logged version, the same causal order it always sorted by,
/// merely now drawn from several files instead of one — before applying any
/// of them; only then does each shard's own valid-prefix length mean
/// anything (a torn write in shard N doesn't affect what's valid in shard M).
///
/// Returns `(shard_path, valid_byte_length)` for every shard scanned (skips
/// ones that don't exist) — callers must truncate each shard file to its own
/// length before appending further, or a corrupt/torn tail would block all
/// future replay of that shard from ever reading past it. The clock itself
/// needs no separate bump: replaying each op already mints it a fresh
/// version via the normal path, so by the time this returns, `tree`'s clock
/// is already correctly positioned to keep issuing versions from there.
pub fn replay<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + record::WalPayload,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    base_path: &Path,
) -> io::Result<Vec<(PathBuf, u64)>> {
    let mut valid_lengths = Vec::new();
    let mut records: Vec<WalRecord<Key, Payload>> = Vec::new();

    for worker_id in 0..MAX_WAL_SHARDS_TO_SCAN {
        let shard_path = wal_shard_path(base_path, worker_id);

        let bytes = match fs::read(&shard_path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };

        let mut offset = 0usize;
        while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
            let Some(record) = record::decode::<Key, Payload>(body) else {
                break;
            };
            records.push(record);
            offset += consumed;
        }

        valid_lengths.push((shard_path, offset as u64));
    }

    records.sort_by_key(|r| r.stamp.ts_start());

    for record in records {
        tree.replay_apply(record.op);
    }

    Ok(valid_lengths)
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Applies one logged operation during recovery, via the normal
    /// traversal/mutate/commit path, minting a fresh version for it (see
    /// this module's top-level doc comment for why that's fine). Never
    /// touches `self.wal` — recovery always runs before a writer is
    /// attached, so this can't double-log.
    pub(crate) fn replay_apply(&self, op: CRUDOperation<Key, Payload>) {
        match op {
            CRUDOperation::Insert(key, payload) => self.replay_insert(key, payload),
            CRUDOperation::Update(key, payload) => self.replay_update(key, payload),
            CRUDOperation::Delete(key) => self.replay_delete(key),
            CRUDOperation::Invalidate(key) => self.replay_invalidate(key),
            // CRUDOperation::Undelete(key) => self.replay_undelete(key),
            other => unreachable!("WAL only ever logs Insert/Update/Delete/Invalidate/Undelete, got: {other}"),
        }
    }

    fn replay_insert(&self, key: Key, payload: Payload) {
        let leaf_guard = self.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        if leaf_page.as_records()
            .iter()
            .rfind(|r| r.key == key)
            .map(|r| r.version.is_live())
            .unwrap_or(false)
        {
            return; // KeyAlreadyExists live too: nothing to apply.
        }

        let current_len = leaf_page.len();
        let stamp = TxStamp::new(self.worker_id(), self.start_tx_commit());

        leaf_page.push_uncommitted(
            RecordPoint::new(key, VersionInfo::new(stamp), payload),
            current_len);
        leaf_page.commit_delta(1, 0);
        self.commit_tx(stamp.worker_id());
    }

    /// Always the normal versioned insert-then-supersede path: the
    /// "update in place" fast path never mints a version, so the live
    /// dispatch path skips that optimization entirely whenever a WAL is
    /// attached (see dispatch.rs's Update arm) — meaning a logged Update
    /// never took it either.
    fn replay_update(&self, key: Key, payload: Payload) {
        let leaf_guard = self.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();
        let current_len = leaf_page.len();

        let stamp = TxStamp::new(self.worker_id(), self.start_tx_commit());

        leaf_page.push_uncommitted(
            RecordPoint::new(key, VersionInfo::new(stamp), payload),
            current_len);
        leaf_page.commit_delta(1, 0);

        match leaf_page.delete_after_update(key, stamp) {
            Ok(Some(..)) => {
                leaf_page.commit_delta(-1, 1);
                self.commit_tx(stamp.worker_id());
            }
            Ok(None) | Err(()) => {
                // Mirrors the fix in dispatch.rs's Update arm: reverse the
                // commit_delta(1, 0) above, or the leaf's active count
                // permanently drifts from its true content.
                leaf_page.commit_delta(-1, 0);
                leaf_page.undo_uncommitted(current_len);
            }
        }
    }

    fn replay_delete(&self, key: Key) {
        let leaf_guard = self.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        let stamp = TxStamp::new(self.worker_id(), self.start_tx_commit());

        if let Ok(Some(..)) = leaf_page.delete(key, stamp) {
            leaf_page.commit_delta(-1, 1);
            self.commit_tx(stamp.worker_id());
        }
    }

    /// Reverses a replayed `Insert`/`Update`, exactly as the live
    /// `MVBTSt::abort_write` path does — see
    /// `mv_page_model::leaf_page::LeafPage::apply_invalidate`'s doc. No
    /// fresh stamp/commit needed: unlike `replay_insert`/`replay_update`/
    /// `replay_delete`, this doesn't introduce a new user-visible version,
    /// it corrects the metadata of an entry already replayed (and already
    /// committed) earlier in this same pass.
    fn replay_invalidate(&self, key: Key) {
        let leaf_guard = self.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();
        leaf_page.apply_invalidate(key);
    }

    // /// Reverses a replayed plain `Delete` — see
    // /// `mv_page_model::leaf_page::LeafPage::apply_undelete`'s doc. Same "no
    // /// fresh stamp needed" reasoning as `replay_invalidate`.
    // fn replay_undelete(&self, key: Key) {
    //     let leaf_guard = self.traversal_write_olc(key);
    //     let leaf_deref_mut = leaf_guard.deref_mut();
    //     let leaf_page = leaf_deref_mut.as_leaf_page();
    //     leaf_page.apply_undelete(key);
    // }
}
