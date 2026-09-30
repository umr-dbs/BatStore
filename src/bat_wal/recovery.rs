use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_record_model::record_point::RecordPoint;
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::{Version, VersionInfo};
use crate::bat_tree::mvbt::MVBTSt;
use crate::bat_wal::record::{self, WalEntry, WalRecord};
use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::hash::Hash;
use std::io;
use std::path::Path;

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
    while let Some((entry, consumed)) =
        record::resync_next(&bytes[offset..], record::decode_entry::<Key, Payload>)
    {
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

fn committed_database_writes<Key, Payload>(
    path: &Path,
) -> io::Result<(u64, Vec<(record::TableId, WalRecord<Key, Payload>)>)>
where
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default + record::WalPayload,
{
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((0, Vec::new())),
        Err(e) => return Err(e),
    };

    let mut writes: Vec<(record::TableId, WalRecord<Key, Payload>, usize)> = Vec::new();
    let mut commits: HashMap<(WorkerId, Version), Version> = HashMap::new();

    let mut offset = 0usize;
    while let Some(((table_id, entry), consumed)) = record::resync_next(
        &bytes[offset..],
        record::decode_entry_for_table::<Key, Payload>,
    ) {
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

    Ok((
        offset as u64,
        committed
            .into_iter()
            .map(|(_, _, table_id, record)| (table_id, record))
            .collect(),
    ))
}

pub(crate) fn replay_database_with_extra<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + record::WalPayload,
>(
    tables: &[&MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>],
    path: &Path,
    mut replay_extra: impl FnMut(record::TableId, CRUDOperation<Key, Payload>),
) -> io::Result<u64> {
    let (valid_len, committed) = committed_database_writes(path)?;
    for (table_id, record) in committed {
        if let Some(tree) = tables.get(table_id as usize) {
            tree.replay_apply(record.op);
        } else {
            replay_extra(table_id, record.op);
        }
    }
    Ok(valid_len)
}

pub fn replay_database<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + record::WalPayload,
>(
    tables: &[&MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>],
    path: &Path,
) -> io::Result<u64> {
    replay_database_with_extra(tables, path, |_, _| {})
}

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
    let (valid_len, committed) = committed_database_writes(path)?;
    for (table_id, record) in committed {
        if table_id == id_a {
            tree_a.replay_apply(record.op);
        } else if table_id == id_b {
            tree_b.replay_apply(record.op);
        }
    }
    Ok(valid_len)
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
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

        if leaf_page
            .as_records()
            .iter()
            .rfind(|r| r.key == key)
            .map(|r| r.version.is_live())
            .unwrap_or(false)
        {
            return; // KeyAlreadyExists live too: nothing to apply.
        }

        let current_len = leaf_page.len();
        let stamp = TxStamp::new(self.worker_id(), self.start_tx_commit());
        let zone_widen = self.cold.zone_map_projection.get().and_then(|f| f(&payload));

        leaf_page.push_uncommitted(
            RecordPoint::new(key, VersionInfo::new(stamp), payload),
            current_len,
        );
        leaf_page.widen_zone_map(zone_widen);
        leaf_page.commit_delta(1, 0);
        self.commit_tx(stamp.worker_id());
    }

    fn replay_update(&self, key: Key, payload: Payload) {
        let leaf_guard = self.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();
        let current_len = leaf_page.len();

        let stamp = TxStamp::new(self.worker_id(), self.start_tx_commit());
        let zone_widen = self.cold.zone_map_projection.get().and_then(|f| f(&payload));

        leaf_page.push_uncommitted(
            RecordPoint::new(key, VersionInfo::new(stamp), payload),
            current_len,
        );
        // A superset is safe, so this also stays on the undo path below.
        leaf_page.widen_zone_map(zone_widen);
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
