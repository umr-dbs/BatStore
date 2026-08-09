use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::hash::Hash;
use std::io;
use std::path::Path;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::{Version, VersionInfo};
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_wal::record::{self, WalEntry, WalRecord};

/// Scans the log at `path` and replays every **committed** write into
/// `tree` through the exact same traversal/mutation primitives the live
/// write path uses — structural repairs (splits/merges) are not logged
/// individually, and replay does not try to reproduce them, or the
/// original absolute version numbers, exactly. Instead:
///
/// - The scan collects two things from the file's `WalEntry`s: every
///   `Write` (a logged op, keyed by its `(worker_id, ts_start)` stamp,
///   remembering its original file order) and every `Commit` marker
///   (`(worker_id, ts_start) -> ts_commit`, see `WalEntry::Commit`'s doc).
/// - **Commit-gated**: a `Write` is only kept for replay if its stamp
///   resolves to a `Commit` marker found anywhere in the file. A write
///   whose transaction aborted, or that a crash caught before it committed,
///   simply never gets one, so it's silently dropped — no separate
///   abort/invalidate record is needed at all.
/// - Surviving writes are sorted by `(ts_commit, original file order)`
///   before replaying — the real order transactions became visible, which
///   is a strictly more accurate causal order than sorting by `ts_start`
///   alone would be (a write is logged well before its transaction's
///   `ts_commit` is known, especially for a multi-op transaction, so two
///   transactions' writes can interleave in `ts_start` order quite
///   differently from the order they actually committed in). Ties within
///   the same transaction fall back to original file order, which is
///   already that transaction's own logged op order.
/// - Each surviving write is then replayed through the normal
///   dispatch-equivalent path (traversal-with-proactive-repair, mutate,
///   commit), letting a **fresh** version get minted for it — no attempt is
///   made to match the original number. Structural repairs fire naturally
///   as a side effect, at the correct *relative* points, because MVBT's
///   repair rule ("fix an unsafe node the moment any traversal passes
///   through it") is a purely local, deterministic function of tree
///   structure — it doesn't depend on global version bookkeeping. So
///   replaying the same sequence of logical operations in the same causal
///   order reconstructs a **logically equivalent** tree (same live
///   keys/values, same relative multi-version history), just addressed by
///   a fresh, contiguous version numbering rather than the original one.
///
/// This is a deliberate trade-off: a client that remembered "my write
/// returned version 57" cannot assume `Point(key, 57)` still means the same
/// thing after a crash + recovery. What's preserved is the *logical*
/// content and its relative history, not absolute version identity.
///
/// Stops scanning (not replaying — every record it *did* read is still
/// eligible for replay) at the first invalid/incomplete frame: a torn write
/// from a crash mid-fsync is expected, not an error.
///
/// Returns the valid byte length of `path` (`0` if the file doesn't exist
/// yet) — callers must truncate the file to this length before appending
/// further, or a corrupt/torn tail would block all future replay from ever
/// reading past it. The clock itself needs no separate bump: replaying each
/// op already mints it a fresh version via the normal path, so by the time
/// this returns, `tree`'s clock is already correctly positioned to keep
/// issuing versions from there.
pub fn replay<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + record::WalPayload,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    path: &Path,
) -> io::Result<u64> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut writes: Vec<(WalRecord<Key, Payload>, usize)> = Vec::new();
    let mut commits: HashMap<(WorkerId, Version), Version> = HashMap::new();

    let mut offset = 0usize;
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        let Some(entry) = record::decode_entry::<Key, Payload>(body) else {
            break;
        };

        match entry {
            WalEntry::Write(record) => {
                let seq = writes.len();
                writes.push((record, seq));
            }
            WalEntry::Commit { stamp, ts_commit } => {
                commits.insert((stamp.worker_id(), stamp.ts_start()), ts_commit);
            }
        }

        offset += consumed;
    }

    let mut committed: Vec<(Version, usize, WalRecord<Key, Payload>)> = writes
        .into_iter()
        .filter_map(|(record, seq)| {
            commits
                .get(&(record.stamp.worker_id(), record.stamp.ts_start()))
                .map(|&ts_commit| (ts_commit, seq, record))
        })
        .collect();

    committed.sort_by_key(|(ts_commit, seq, _)| (*ts_commit, *seq));

    for (_, _, record) in committed {
        tree.replay_apply(record.op);
    }

    Ok(offset as u64)
}

/// `mv_db::Database` counterpart to [`replay`]: a single sequential scan of
/// one shared log file whose entries are tagged with a `TableId` (see
/// `record::encode_entry_for_table`/`MVBTSt::table_id`'s doc), each `Write`
/// routed to the matching tree in `tables` — a table's `TableId` is simply
/// its index into `tables` (the same index `Database::create_table` assigns
/// it, its position in the database's table list), so this is a direct
/// slice index, not a hash lookup. Commit-gating and the `(ts_commit, seq)`
/// replay order are otherwise identical to `replay` — unaffected by table
/// splitting, since a `WalEntry::Commit` marker is transaction-scoped
/// (keyed by `(worker_id, ts_start)` alone), not table-scoped; its own
/// tagged `TableId` (always `record::TABLE_ID_COMMIT_SENTINEL`) is read and
/// discarded here.
///
/// A `Write` whose `TableId` is out of bounds for `tables` is silently
/// skipped, the same treatment `replay` already gives a write with no
/// matching commit marker — callers must pre-create (via
/// `Database::create_table`, or by recreating a database's whole table
/// catalog before recovery — see `Database::open_recovered`) every table
/// that might appear in the log, at the same index it originally had,
/// *before* calling this. Returns the valid byte length of `path`, per
/// `replay`'s same contract (`0` if the file doesn't exist yet; callers must
/// truncate to this length before appending further).
pub fn replay_database<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + record::WalPayload,
>(
    tables: &[&MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>],
    path: &Path,
) -> io::Result<u64> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut writes: Vec<(record::TableId, WalRecord<Key, Payload>, usize)> = Vec::new();
    let mut commits: HashMap<(WorkerId, Version), Version> = HashMap::new();

    let mut offset = 0usize;
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        let Some((table_id, entry)) = record::decode_entry_for_table::<Key, Payload>(body) else {
            break;
        };

        match entry {
            WalEntry::Write(record) => {
                let seq = writes.len();
                writes.push((table_id, record, seq));
            }
            WalEntry::Commit { stamp, ts_commit } => {
                commits.insert((stamp.worker_id(), stamp.ts_start()), ts_commit);
            }
        }

        offset += consumed;
    }

    let mut committed: Vec<(Version, usize, record::TableId, WalRecord<Key, Payload>)> = writes
        .into_iter()
        .filter_map(|(table_id, record, seq)| {
            commits
                .get(&(record.stamp.worker_id(), record.stamp.ts_start()))
                .map(|&ts_commit| (ts_commit, seq, table_id, record))
        })
        .collect();

    committed.sort_by_key(|(ts_commit, seq, _, _)| (*ts_commit, *seq));

    for (_, _, table_id, record) in committed {
        if let Some(tree) = tables.get(table_id as usize) {
            tree.replay_apply(record.op);
        }
    }

    Ok(offset as u64)
}

/// `replay_database` counterpart for a small, fixed set of trees that live
/// outside a `Database`'s own homogeneous table list — e.g.
/// `mv_bench::tpcc_schema`'s size-class "Big" trees, which need their own
/// `NUM_RECORDS` and so can't sit in `Database`'s uniformly-typed
/// `&[&MVBTSt<FAN_OUT, NUM_RECORDS, ..>]` slice alongside the rest. Takes
/// each tree with its own concrete `NUM_RECORDS` and its own reserved
/// `TableId` tag directly (the caller picks tags disjoint from whatever
/// range `Database::create_table` already assigned its own tables, since
/// every tree here shares one physical log file — see
/// `mv_bench::tpcc_schema`'s reserved-id constants). Otherwise identical to
/// `replay_database`: one full scan of the same shared log file,
/// commit-gated (a write only replays if some `Commit` marker anywhere in
/// the file — not just among entries tagged for these two trees, since a
/// multi-table transaction's marker doesn't care which table logged first —
/// resolves its stamp), replayed in `(ts_commit, seq)` order. An entry
/// tagged with neither `id_a` nor `id_b` is silently skipped — it belongs to
/// some other tree this function doesn't know about (`Database`'s own
/// `replay_database` call over the same file handles those).
pub fn replay_two_tables<
    const FAN_OUT: usize,
    const NUM_RECORDS_A: usize,
    const NUM_RECORDS_B: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + record::WalPayload,
>(
    tree_a: &MVBTSt<FAN_OUT, NUM_RECORDS_A, Key, Payload>,
    id_a: record::TableId,
    tree_b: &MVBTSt<FAN_OUT, NUM_RECORDS_B, Key, Payload>,
    id_b: record::TableId,
    path: &Path,
) -> io::Result<u64> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut writes: Vec<(record::TableId, WalRecord<Key, Payload>, usize)> = Vec::new();
    let mut commits: HashMap<(WorkerId, Version), Version> = HashMap::new();

    let mut offset = 0usize;
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        let Some((table_id, entry)) = record::decode_entry_for_table::<Key, Payload>(body) else {
            break;
        };

        match entry {
            WalEntry::Write(record) => {
                let seq = writes.len();
                writes.push((table_id, record, seq));
            }
            WalEntry::Commit { stamp, ts_commit } => {
                commits.insert((stamp.worker_id(), stamp.ts_start()), ts_commit);
            }
        }

        offset += consumed;
    }

    let mut committed: Vec<(Version, usize, record::TableId, WalRecord<Key, Payload>)> = writes
        .into_iter()
        .filter_map(|(table_id, record, seq)| {
            commits
                .get(&(record.stamp.worker_id(), record.stamp.ts_start()))
                .map(|&ts_commit| (ts_commit, seq, table_id, record))
        })
        .collect();

    committed.sort_by_key(|(ts_commit, seq, _, _)| (*ts_commit, *seq));

    for (_, _, table_id, record) in committed {
        if table_id == id_a {
            tree_a.replay_apply(record.op);
        } else if table_id == id_b {
            tree_b.replay_apply(record.op);
        }
    }

    Ok(offset as u64)
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
            other => unreachable!("WAL only ever logs Insert/Update/Delete, got: {other}"),
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
}

