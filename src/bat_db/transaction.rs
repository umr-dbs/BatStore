//! Generalization of `bat_bench::tpcc_txn::TpccTxn` for a `bat_db::Database`:
//! a multi-table OSIC transaction (paper §3.5, "Putting Everything
//! Together") — one fixed transaction stamp (`ts_start`), drawn from the
//! database's shared `TxContext` at `begin`. Snapshot isolation reads use
//! that timestamp; read committed transactions advance `read_ts` automatically
//! before each public read/write operation. Writes retain `(worker_id, ts_start)` and
//! are never revisited at commit, so `commit` is just a single append
//! to this worker's `CommitLog`, i.e. OSIC's "instant commit".
//!
//! Each write is also logged to the WAL (if attached) as it happens, under
//! that same fixed stamp — fire-and-forget, same as single-op
//! `dispatch_crud` (see `MVBTSt::wal_hardened_version`'s doc): `commit`
//! never waits for any of this transaction's writes to actually flush.
//! `commit` additionally logs one WAL Commit marker for the whole
//! transaction (see `WalEntry::Commit`'s doc) — replay only ever applies a
//! write once it finds that marker, so an aborted transaction's writes
//! (below) are simply never replayed, with no separate reversal needed on
//! the WAL side. Callers that need a durability point-in-time for this
//! transaction should call `wal_hardened_version`/`wait_wal_hardened`
//! themselves after `commit` returns.
//!
//! **Abort**: dropping this transaction without calling `commit()` (e.g.
//! after a `Conflict`, or a business-logic rollback) reverts every write it
//! already applied, automatically, in `Drop` — see `MVBTSt::abort_write`.
//! Each write's *key* (paired with the table it was written on) is
//! remembered (not a full undo log: replaying it back to its live-tree
//! state is enough) — including a key written more than once by the same
//! transaction (e.g. `bat_bench::tpcc_txn::new_order` pricing two order-lines
//! for the same item): `written` gets one entry per physical write, and
//! `abort`'s reverse-order walk unwinds them one at a time, each call
//! landing on the next-older still-valid version (`LeafPage::abort_write`
//! skips already-invalidated entries for exactly this reason), so all of
//! them come back out in order, same as a real undo log would. Reversal
//! marks the record invalid (or undeletes it, for a plain `Delete`) rather
//! than physically removing it — see
//! `TxStamp::is_invalid`'s doc — and is purely in-memory: since this
//! transaction never commits, no `wal_log_commit` marker is ever logged for
//! it, so replay skips its writes regardless of whether/when a crash
//! happens relative to this abort.
//!
//! Two deliberate differences from `TpccTxn`, both consequences of
//! `Database`'s tables being looked up at runtime (`Arc<MVBTSt<..>>`, from a
//! locked map) rather than fixed struct fields borrowed for the whole
//! transaction's lifetime:
//! - `range` is always eager (`CRUDOperationResult::MatchedRecords`, never
//!   the lazy `MatchedRecordIter`): a `RangeQueryIter<'a>` borrowing from a
//!   function-local `Arc` clone can't outlive this method call. The
//!   `range_for_each`/`range_fold`/`range_count` methods keep that `Arc`
//!   local and consume the iterator before returning, providing zero-copy
//!   scans without exposing the borrowed iterator.
//! - `commit()` logs exactly one Commit marker (through any one touched
//!   table's tree — they all share the same underlying `Arc<WalWriter>`),
//!   not a loop over every touched table.
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationInnerReason::{
    KeyAlreadyDeleted, KeyAlreadyExists, KeyDoesNotExist,
};
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_page_model::BlockRef;
use crate::bat_page_model::leaf_page::LeafPage;
use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_record_model::record_point::{RecordPoint, RecordPointResult};
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::{Version, VersionInfo};
use crate::bat_tree::mvbt::MVBTSt;
use crate::bat_wal::record::{TableId, WalPayload};
use smallvec::SmallVec;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::DerefMut;
use triomphe::Arc;

use super::database::Database;

pub enum TransactionState {
    InFlight,
    Committed,
    Aborted,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IsolationLevel {
    #[default]
    SnapshotIsolation,
    ReadCommitted,
}

pub(crate) fn insert_on_tree_at<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    read_ts: Version,
    key: Key,
    payload: Payload,
) -> (
    CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>,
    bool,
) {
    let leaf_guard = tree.traversal_write_olc_registered(key);
    let leaf_deref_mut = leaf_guard.deref_mut();
    let leaf_page = leaf_deref_mut.as_leaf_page();
    let stamp = TxStamp::new(worker_id, ts_start);
    // Every physical write must widen the leaf's zone map (see `dispatch.rs`), or
    // zone-pruned scans skip leaves that hold the new value.
    let zone_widen = tree.cold.zone_map_projection.get().and_then(|f| f(&payload));

    if let Some(position) = leaf_page.latest_position(key, true) {
        if leaf_page.version_at(position).is_live() {
            let crud_error =
                if leaf_page.version_at(position).insertion_stamp().worker_id() != worker_id {
                    CRUDOperationResult::Conflict
                } else {
                    CRUDOperationResult::ZeroAffected(KeyAlreadyExists)
                };
            return (crud_error, false);
        }

        // A tombstone made by another transaction cannot free the key for
        // insertion until that delete committed before this statement's
        // snapshot. Otherwise its abort could resurrect a second live row.
        if let Some(deletion) = leaf_page.version_at(position).deletion_stamp() {
            if deletion.worker_id() != worker_id
                && !tree.is_visible_stamp(worker_id, read_ts, deletion)
            {
                return (CRUDOperationResult::Conflict, false);
            }
        }

        if leaf_page.version_at(position).insertion_stamp() == stamp
            && leaf_page.version_at(position).deletion_stamp() == Some(stamp)
        {
            tree.wal_log_write(stamp, |_| CRUDOperation::Insert(key, payload.clone()));
            leaf_page.version_mut_at(position).undelete();
            leaf_page.set_payload_at(position, payload);
            leaf_page.widen_zone_map(zone_widen);
            leaf_page.commit_delta(1, -1);
            return (CRUDOperationResult::Inserted(stamp.ts_start()), false);
        }
    }

    tree.wal_log_write(stamp, |_| CRUDOperation::Insert(key, payload.clone()));

    let current_len = leaf_page.len();

    leaf_page.push_uncommitted(
        RecordPoint::new(key, VersionInfo::new(stamp), payload),
        current_len,
    );
    leaf_page.widen_zone_map(zone_widen);

    leaf_page.commit_delta(1, 0);

    (CRUDOperationResult::Inserted(stamp.ts_start()), true)
}

pub(crate) fn insert_on_tree<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    key: Key,
    payload: Payload,
) -> (
    CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>,
    bool,
) {
    insert_on_tree_at(tree, worker_id, ts_start, ts_start, key, payload)
}

pub(crate) fn update_on_tree_at<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    read_ts: Version,
    key: Key,
    payload: Payload,
) -> (
    CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>,
    bool,
) {
    let leaf_guard = tree.traversal_write_olc_registered(key);
    let leaf_deref_mut = leaf_guard.deref_mut();
    let leaf_page = leaf_deref_mut.as_leaf_page();
    let zone_widen = tree.cold.zone_map_projection.get().and_then(|f| f(&payload));

    match leaf_page.latest_position(key, true) {
        Some(position) => {
            if let Some(deletion) = leaf_page.version_at(position).deletion_stamp() {
                if deletion.worker_id() != worker_id
                    && !tree.is_visible_stamp(worker_id, read_ts, deletion)
                {
                    return (CRUDOperationResult::Conflict, false);
                }
            }
            if tree.is_visible_stamp(
                worker_id,
                read_ts,
                leaf_page.version_at(position).insertion_stamp(),
            ) {
                let stamp = TxStamp::new(worker_id, ts_start);

                tree.wal_log_write(stamp, |_| CRUDOperation::Update(key, payload.clone()));

                // Self-overwrite fast path — see `insert_on_tree`'s doc.
                if leaf_page.version_at(position).insertion_stamp() == stamp {
                    leaf_page.set_payload_at(position, payload);
                    leaf_page.widen_zone_map(zone_widen);
                    return (CRUDOperationResult::Updated(stamp.ts_start()), false);
                }

                if !leaf_page.version_mut_at(position).delete(stamp) {
                    return (CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted), false);
                }

                let current_len = leaf_page.len();

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(stamp), payload),
                    current_len,
                );
                leaf_page.widen_zone_map(zone_widen);

                leaf_page.commit_delta(0, 1);

                (CRUDOperationResult::Updated(stamp.ts_start()), true)
            } else {
                (CRUDOperationResult::Conflict, false)
            }
        }
        None => (CRUDOperationResult::ZeroAffected(KeyDoesNotExist), false),
    }
}

/// See `insert_on_tree`'s doc.
pub(crate) fn delete_on_tree_at<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    read_ts: Version,
    key: Key,
) -> (
    CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>,
    bool,
) {
    let leaf_guard = tree.traversal_write_olc_registered(key);
    let leaf_deref_mut = leaf_guard.deref_mut();
    let leaf_page = leaf_deref_mut.as_leaf_page();

    match leaf_page.latest_position(key, true) {
        Some(position) => {
            if let Some(deletion) = leaf_page.version_at(position).deletion_stamp() {
                if deletion.worker_id() != worker_id
                    && !tree.is_visible_stamp(worker_id, read_ts, deletion)
                {
                    return (CRUDOperationResult::Conflict, false);
                }
            }
            if tree.is_visible_stamp(
                worker_id,
                read_ts,
                leaf_page.version_at(position).insertion_stamp(),
            ) {
                let stamp = TxStamp::new(worker_id, ts_start);
                tree.wal_log_write(stamp, |_| CRUDOperation::Delete(key));

                if leaf_page.version_mut_at(position).delete(stamp) {
                    leaf_page.commit_delta(-1, 1);
                    (CRUDOperationResult::Deleted(stamp.ts_start()), true)
                } else {
                    (CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted), false)
                }
            } else {
                (CRUDOperationResult::Conflict, false)
            }
        }
        None => (CRUDOperationResult::ZeroAffected(KeyDoesNotExist), false),
    }
}

/// Existing fixed-snapshot write path used by single-operation callers.
pub(crate) fn update_on_tree<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    key: Key,
    payload: Payload,
) -> (
    CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>,
    bool,
) {
    update_on_tree_at(tree, worker_id, ts_start, ts_start, key, payload)
}

pub(crate) fn delete_on_tree<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    key: Key,
) -> (
    CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>,
    bool,
) {
    delete_on_tree_at(tree, worker_id, ts_start, ts_start, key)
}

/// See `insert_on_tree`'s doc — the read-side counterpart. Always eager
/// (`MatchedRecords`), same as `DbTransaction::point`.
pub(crate) fn point_on_tree<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    key: Key,
) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
    match tree.key_point_read_from_root(tree.retrieve_root_for(ts_start), key, worker_id, ts_start)
    {
        CRUDOperationResult::MatchedRecords(v) => CRUDOperationResult::MatchedRecords(v),
        other => panic!("bat_db::point_on_tree: expected MatchedRecords, got {other}"),
    }
}

pub(crate) fn range_on_tree<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default
        + Ord
        + Copy
        + Hash
        + Display
        + Sync
        + Send
        + crate::bat_query::interval::RangeSplit
        + 'static,
    Payload: Display + Clone + Default + Sync + Send + 'static + WalPayload,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    range: Interval<Key>,
    pool: Option<&crate::bat_tree::scan_pool::ScanWorkerPool<FAN_OUT, NUM_RECORDS, Key, Payload>>,
) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
    let scan = RangeQueryIter::new(tree, ts_start, range, false, worker_id);
    CRUDOperationResult::MatchedRecords(scan.collect_parallel(pool))
}

/// See `insert_on_tree`'s doc; see `DbTransaction::range_min`'s doc for why
/// this needs a real in-leaf comparison rather than just `next()`.
pub(crate) fn range_min_on_tree<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    range: Interval<Key>,
) -> Option<RecordPointResult<Key, Payload>> {
    RangeQueryIter::new(tree, ts_start, range, false, worker_id).min_by_key()
}

struct TxTableState<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> {
    table: TableId,
    tree: Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    read_root: Option<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>,
}

pub struct DbTransaction<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> {
    db: &'a Database<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    read_ts: Version,
    isolation: IsolationLevel,
    committed: TransactionState,
    /// `(table-cache slot, key)` pairs this transaction has actually written (on a
    /// successful `Inserted`/`Updated`/`Deleted` outcome only — never on
    /// `Conflict`/`ZeroAffected`, since nothing was written there to
    /// revert). Walked by `Drop` to abort every one of them if `commit()`
    /// was never called.
    written: SmallVec<[(usize, Key); 16]>,
    /// Trees touched by this transaction and roots resolved for its current
    /// statement snapshot. Cleared when a read committed operation starts.
    /// Eight inline entries cover the common multi-table OLTP transaction without
    /// allocating; unusually wide transactions spill to the heap.
    tables: SmallVec<[TxTableState<FAN_OUT, NUM_RECORDS, Key, Payload>; 8]>,
}

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> DbTransaction<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn begin(db: &'a Database<FAN_OUT, NUM_RECORDS, Key, Payload>) -> Self {
        Self::begin_with_isolation(db, IsolationLevel::SnapshotIsolation)
    }

    pub fn begin_with_isolation(
        db: &'a Database<FAN_OUT, NUM_RECORDS, Key, Payload>,
        isolation: IsolationLevel,
    ) -> Self {
        let worker_id = db.worker_id();
        let ts_start = db.begin_snapshot();

        Self {
            db,
            worker_id,
            ts_start,
            read_ts: ts_start,
            isolation,
            committed: TransactionState::InFlight,
            written: SmallVec::new(),
            tables: SmallVec::new(),
        }
    }

    #[inline(always)]
    pub const fn ts_start(&self) -> Version {
        self.ts_start
    }

    pub const fn read_ts(&self) -> Version {
        self.read_ts
    }

    #[inline]
    fn refresh_read_snapshot(&mut self) {
        if self.isolation == IsolationLevel::ReadCommitted {
            for table in &mut self.tables {
                table.read_root = None;
            }
            self.read_ts = self.db.ctx.begin_statement_snapshot();
        }
    }

    #[inline(always)]
    pub const fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    fn table_slot(&mut self, table: TableId) -> usize {
        if let Some(slot) = self.tables.iter().position(|state| state.table == table) {
            return slot;
        }

        let tree = self.db.table(table)
            .unwrap_or_else(|| panic!("bat_db::DbTransaction: unknown TableId {table} (did you forget Database::create_table?)"));
        let slot = self.tables.len();
        self.tables.push(TxTableState {
            table,
            tree,
            read_root: None,
        });
        slot
    }

    #[inline]
    fn read_tree_and_root(
        &mut self,
        table: TableId,
    ) -> (
        Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ) {
        let slot = self.table_slot(table);
        if let Some(root) = &self.tables[slot].read_root {
            return (self.tables[slot].tree.clone(), root.clone());
        }

        let tree = self.tables[slot].tree.clone();
        let root = tree.retrieve_root_for(self.read_ts);
        self.tables[slot].read_root = Some(root.clone());
        (tree, root)
    }

    #[cfg(test)]
    pub(crate) fn cached_read_root_count(&self) -> usize {
        self.tables
            .iter()
            .filter(|state| state.read_root.is_some())
            .count()
    }

    pub fn point(
        &mut self,
        table: TableId,
        key: Key,
    ) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.refresh_read_snapshot();
        let (tree, root) = self.read_tree_and_root(table);
        tree.key_point_read_from_root(root, key, self.worker_id, self.read_ts)
    }

    /// Range read against this statement's snapshot, on `table` —
    /// always eager, see the module doc for why.
    pub fn range(
        &mut self,
        table: TableId,
        range: Interval<Key>,
    ) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.refresh_read_snapshot();
        let (tree, root) = self.read_tree_and_root(table);
        CRUDOperationResult::MatchedRecords(
            RangeQueryIter::new_with_root(&tree, self.read_ts, range, false, self.worker_id, root)
                .collect(),
        )
    }

    pub fn range_min(
        &mut self,
        table: TableId,
        range: Interval<Key>,
    ) -> Option<RecordPointResult<Key, Payload>> {
        self.refresh_read_snapshot();
        let (tree, root) = self.read_tree_and_root(table);
        RangeQueryIter::new_with_root(&tree, self.read_ts, range, false, self.worker_id, root)
            .min_by_key()
    }

    /// Fallible zero-copy range visitor against this statement's
    /// snapshot. An error stops the scan immediately.
    pub fn try_range_for_each<E>(
        &mut self,
        table: TableId,
        range: Interval<Key>,
        visit: impl FnMut(Key, &Payload) -> Result<(), E>,
    ) -> Result<(), E> {
        self.refresh_read_snapshot();
        let (tree, root) = self.read_tree_and_root(table);
        RangeQueryIter::new_with_root(&tree, self.read_ts, range, false, self.worker_id, root)
            .try_for_each_ref(visit)
    }

    /// Infallible zero-copy range visitor.
    pub fn range_for_each(
        &mut self,
        table: TableId,
        range: Interval<Key>,
        visit: impl FnMut(Key, &Payload),
    ) {
        self.refresh_read_snapshot();
        let (tree, root) = self.read_tree_and_root(table);
        RangeQueryIter::new_with_root(&tree, self.read_ts, range, false, self.worker_id, root)
            .for_each_ref(visit)
    }

    /// Zero-copy left fold over a range.
    pub fn range_fold<Acc>(
        &mut self,
        table: TableId,
        range: Interval<Key>,
        initial: Acc,
        fold: impl FnMut(Acc, Key, &Payload) -> Acc,
    ) -> Acc {
        self.refresh_read_snapshot();
        let (tree, root) = self.read_tree_and_root(table);
        RangeQueryIter::new_with_root(&tree, self.read_ts, range, false, self.worker_id, root)
            .fold_ref(initial, fold)
    }

    /// Counts visible range records without constructing result objects.
    pub fn range_count(&mut self, table: TableId, range: Interval<Key>) -> usize {
        self.refresh_read_snapshot();
        let (tree, root) = self.read_tree_and_root(table);
        RangeQueryIter::new_with_root(&tree, self.read_ts, range, false, self.worker_id, root)
            .count_ref()
    }

    pub fn insert(
        &mut self,
        table: TableId,
        key: Key,
        payload: Payload,
    ) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.refresh_read_snapshot();
        let slot = self.table_slot(table);
        let tree = self.tables[slot].tree.clone();
        let (result, track) = insert_on_tree_at(
            &tree,
            self.worker_id,
            self.ts_start,
            self.read_ts,
            key,
            payload,
        );
        if track {
            self.written.push((slot, key));
        }
        result
    }

    pub fn update(
        &mut self,
        table: TableId,
        key: Key,
        payload: Payload,
    ) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.refresh_read_snapshot();
        let slot = self.table_slot(table);
        let tree = self.tables[slot].tree.clone();
        let (result, track) = update_on_tree_at(
            &tree,
            self.worker_id,
            self.ts_start,
            self.read_ts,
            key,
            payload,
        );
        if track {
            self.written.push((slot, key));
        }
        result
    }

    pub fn delete(
        &mut self,
        table: TableId,
        key: Key,
    ) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.refresh_read_snapshot();
        let slot = self.table_slot(table);
        let tree = self.tables[slot].tree.clone();
        let (result, track) =
            delete_on_tree_at(&tree, self.worker_id, self.ts_start, self.read_ts, key);
        if track {
            self.written.push((slot, key));
        }
        result
    }

    pub const fn is_committed(&self) -> bool {
        matches!(self.committed, TransactionState::Committed)
    }

    pub const fn is_aborted(&self) -> bool {
        matches!(self.committed, TransactionState::Aborted)
    }

    fn finish_snapshots(&self) {
        if self.read_ts != self.ts_start {
            self.db.ctx.end_statement_snapshot();
        }
        self.db.end_snapshot(self.ts_start);
    }

    pub fn commit(mut self) -> Option<Version> {
        if let TransactionState::InFlight = self.committed {
            self.committed = TransactionState::Committed;

            if self.written.is_empty() {
                self.finish_snapshots();
                return None;
            }

            let ts_commit = self.db.ctx.commit_tx(self.worker_id);

            if let Some(&(slot, _)) = self.written.first() {
                let stamp = TxStamp::new(self.worker_id, self.ts_start);
                self.tables[slot].tree.wal_log_commit(stamp, ts_commit);
            }

            self.finish_snapshots();
            Some(ts_commit)
        } else {
            None
        }
    }

    pub fn abort(mut self) -> bool {
        if let TransactionState::InFlight = self.committed {
            self.committed = TransactionState::Aborted;
            self.unwind_writes();
            self.finish_snapshots();
            true
        } else {
            false
        }
    }

    fn unwind_writes(&mut self) {
        let stamp = TxStamp::new(self.worker_id, self.ts_start);
        let mut end = self.written.len();
        while end != 0 {
            let (slot, key) = self.written[end - 1];
            let mut start = end - 1;
            while start != 0 && self.written[start - 1] == (slot, key) {
                start -= 1;
            }
            self.tables[slot].tree.abort_writes(key, stamp, end - start);
            end = start;
        }
    }
}

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> Drop for DbTransaction<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn drop(&mut self) {
        if let TransactionState::InFlight = self.committed {
            // See `unwind_writes`'s doc for why this must run in reverse
            // (LIFO) order.
            self.unwind_writes();
            self.finish_snapshots();
        }
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> Database<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn dispatch_crud(
        &self,
        table: TableId,
        op: CRUDOperation<Key, Payload>,
    ) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mut tx = DbTransaction::begin(self);

        let result = match op {
            CRUDOperation::Insert(key, payload) => tx.insert(table, key, payload),
            CRUDOperation::Update(key, payload) => tx.update(table, key, payload),
            CRUDOperation::Delete(key) => tx.delete(table, key),
            CRUDOperation::Point(key, _) | CRUDOperation::PointSi(key) => tx.point(table, key),
            CRUDOperation::Range(range, _) | CRUDOperation::RangeSi(range) => {
                tx.range(table, range)
            }
            other => panic!("bat_db::Database::dispatch_crud: unsupported op {other}"),
        };

        if matches!(result, CRUDOperationResult::Conflict) {
            drop(tx);
        } else {
            tx.commit();
        }

        result
    }
}
