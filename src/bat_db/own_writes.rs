//! The transaction's own changes, and how its reads see them.
//!
//! A transaction reads at its snapshot `t`: at every internal node it takes
//! the newest entry whose version is not newer than `t`. Its own writes,
//! however, go to the key's *current* leaf. If a structure modification
//! replaced that leaf after `t` was drawn, its new entries carry a version
//! above `t`, the snapshot route still ends in the retired leaf, and that
//! leaf never received the write: without help, the transaction would not
//! see its own change (a point read misses an own insert, or returns the
//! value an own update replaced).
//!
//! So next to its writes in the tree, a transaction keeps [`OwnWrites`]:
//! every key it has changed, and whether its last change deleted the key.
//! - A deleted own key is answered from there: a point read finds nothing,
//!   and a scan leaves the key out.
//! - For an own key that still exists, first-writer-wins keeps every other
//!   transaction off it while the transaction runs, so the newest valid
//!   version of the key in the current tree is the transaction's own. A
//!   point read of it therefore routes like a writer
//!   (`MVBTSt::own_key_point_read`), and a scan skips own keys on its
//!   snapshot route and visits, for each of them, what the writer-style
//!   route finds instead (see [`try_visit_range`]).
//!
//! The payloads themselves are not kept: copying every written row costs
//! more than the occasional writer-routed read of one.
//!
//! Writes still go to the tree, which is where they live until commit; only
//! a write that the key's own state already decides is answered from it
//! (insert of a key the transaction holds, update or delete of a key it
//! deleted).
//!
//! Every other key keeps the snapshot route, and a transaction that has not
//! written the table it reads pays one binary search of its write set.
use std::convert::Infallible;
use std::fmt::Display;
use std::hash::Hash;

use smallvec::SmallVec;

use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_record_model::record_point::RecordPointResult;
use crate::bat_record_model::tx_stamp::WorkerId;
use crate::bat_record_model::version_info::Version;
use crate::bat_tree::mvbt::MVBTSt;

/// One key that a transaction has changed.
#[derive(Clone, Copy)]
pub(crate) struct OwnWrite<Table, Key> {
    pub(crate) table: Table,
    pub(crate) key: Key,
    /// Whether the transaction's last change deleted the key.
    pub(crate) deleted: bool,
}

/// The keys a transaction has changed, one entry per `(table, key)` with
/// its latest state, sorted by `(table, key)`. `Table` identifies the tree
/// (a table-cache slot, or a TPC-C `Table`).
///
/// Unlike a transaction's abort list (one entry per physical write, in
/// order), this only answers reads. Like that list, it keeps a typical OLTP
/// transaction's keys inline: allocating and growing a heap vector per
/// transaction costs measurable TPC-C throughput.
pub(crate) struct OwnWrites<Table, Key> {
    entries: SmallVec<[OwnWrite<Table, Key>; 16]>,
}

impl<Table: Ord + Copy, Key: Ord + Copy + Hash + Display> OwnWrites<Table, Key> {
    pub(crate) fn new() -> Self {
        Self {
            entries: SmallVec::new(),
        }
    }

    fn search(&self, table: Table, key: Key) -> Result<usize, usize> {
        self.entries
            .binary_search_by(|w| (w.table, w.key).cmp(&(table, key)))
    }

    /// `None` if the transaction has not changed `key` in `table`;
    /// otherwise whether its last change deleted the key.
    #[inline]
    pub(crate) fn get(&self, table: Table, key: Key) -> Option<bool> {
        if self.entries.is_empty() {
            return None;
        }
        self.search(table, key).ok().map(|i| self.entries[i].deleted)
    }

    /// Records a write the transaction has applied; `deleted` says whether
    /// it was a delete.
    pub(crate) fn record(&mut self, table: Table, key: Key, deleted: bool) {
        match self.search(table, key) {
            Ok(i) => self.entries[i].deleted = deleted,
            Err(i) => self.entries.insert(
                i,
                OwnWrite {
                    table,
                    key,
                    deleted,
                },
            ),
        }
    }

    /// The changed keys of `table` within `range`, in key order.
    #[inline]
    pub(crate) fn in_range(&self, table: Table, range: &Interval<Key>) -> &[OwnWrite<Table, Key>] {
        if self.entries.is_empty() {
            return &[];
        }
        let lo = self
            .entries
            .partition_point(|w| (w.table, w.key) < (table, range.lower));
        let hi = self
            .entries
            .partition_point(|w| (w.table, w.key) <= (table, range.upper));
        &self.entries[lo..hi.max(lo)]
    }
}

/// Records `result`, the outcome of a write of `key` in `table`, in `own`
/// if the write was applied.
pub(crate) fn record_write<
    'r,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Table: Ord + Copy,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    own: &mut OwnWrites<Table, Key>,
    table: Table,
    key: Key,
    result: &CRUDOperationResult<'r, FAN_OUT, NUM_RECORDS, Key, Payload>,
) {
    match result {
        CRUDOperationResult::Inserted(_) | CRUDOperationResult::Updated(_) => {
            own.record(table, key, false)
        }
        CRUDOperationResult::Deleted(_) => own.record(table, key, true),
        _ => {}
    }
}

/// Point read of `key`, an own key of `tree` whose last change was a delete
/// if `deleted`.
pub(crate) fn own_point_read<
    'r,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    read_ts: Version,
    key: Key,
    deleted: bool,
) -> CRUDOperationResult<'r, FAN_OUT, NUM_RECORDS, Key, Payload> {
    if deleted {
        CRUDOperationResult::MatchedRecords(Vec::new())
    } else {
        tree.own_key_point_read(key, worker_id, read_ts)
    }
}

/// The transaction's own version of `key`, an own key that it has not
/// deleted.
fn own_point<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    read_ts: Version,
    key: Key,
) -> Option<RecordPointResult<Key, Payload>> {
    match tree.own_key_point_read(key, worker_id, read_ts) {
        CRUDOperationResult::MatchedRecords(mut records) => records.pop(),
        other => panic!("bat_db::own_writes::own_point: expected MatchedRecords, got {other}"),
    }
}

/// The own versions of the keys in `own` that the transaction has not
/// deleted, in key order.
fn own_records<
    'o,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Table,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    tree: &'o MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    read_ts: Version,
    own: &'o [OwnWrite<Table, Key>],
) -> impl Iterator<Item = RecordPointResult<Key, Payload>> + 'o {
    own.iter()
        .filter(|write| !write.deleted)
        .filter_map(move |write| own_point(tree, worker_id, read_ts, write.key))
}

/// Whether `key` is one of `own`, the sorted own keys of one table.
#[inline]
fn is_own<Table, Key: Ord + Copy>(own: &[OwnWrite<Table, Key>], key: Key) -> bool {
    own.binary_search_by(|w| w.key.cmp(&key)).is_ok()
}

/// Whether an own record passes the scan's zone predicate, applied the same
/// way `RangeQueryIter::try_for_each_ref` applies it to every other record.
fn passes_zone<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    zone: Option<(u64, u64)>,
    payload: &Payload,
) -> bool {
    match (zone, tree.cold.zone_map_projection.get()) {
        (Some((lo, hi)), Some(project)) => project(payload).is_some_and(|v| v >= lo && v <= hi),
        _ => true,
    }
}

/// Visits the records of `scan`, a snapshot scan of the transaction, with
/// the keys in `own` (the transaction's own keys of the scanned table and
/// range, see [`OwnWrites::in_range`]) taken from its own changes. Visiting
/// only the scan's records would not do: for an own key the scan may return
/// the replaced value, nothing, or a row the transaction deleted. `zone`
/// must be the zone predicate set on `scan`, if any. Own records are
/// visited after the others, in key order.
pub(crate) fn try_visit_range<
    E,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Table,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    scan: RangeQueryIter<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    read_ts: Version,
    own: &[OwnWrite<Table, Key>],
    zone: Option<(u64, u64)>,
    mut visit: impl FnMut(Key, &Payload) -> Result<(), E>,
) -> Result<(), E> {
    if own.is_empty() {
        return scan.try_for_each_ref(visit);
    }
    scan.try_for_each_ref(|key, payload| {
        if is_own(own, key) {
            Ok(())
        } else {
            visit(key, payload)
        }
    })?;
    for record in own_records(tree, worker_id, read_ts, own) {
        if passes_zone(tree, zone, &record.payload) {
            visit(record.key, &record.payload)?;
        }
    }
    Ok(())
}

/// Infallible form of [`try_visit_range`].
pub(crate) fn visit_range<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Table,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    scan: RangeQueryIter<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    read_ts: Version,
    own: &[OwnWrite<Table, Key>],
    zone: Option<(u64, u64)>,
    mut visit: impl FnMut(Key, &Payload),
) {
    let result: Result<(), Infallible> =
        try_visit_range(scan, tree, worker_id, read_ts, own, zone, |key, payload| {
            visit(key, payload);
            Ok(())
        });
    match result {
        Ok(()) => {}
        Err(never) => match never {},
    }
}

/// Replaces, in the materialized result of a snapshot scan, the records of
/// the keys in `own` by the transaction's own versions (see
/// [`try_visit_range`]). Like the scan's result, it is not sorted: the own
/// records follow the others, in key order.
pub(crate) fn overlay_records<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Table,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    mut records: Vec<RecordPointResult<Key, Payload>>,
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    read_ts: Version,
    own: &[OwnWrite<Table, Key>],
) -> Vec<RecordPointResult<Key, Payload>> {
    if own.is_empty() {
        return records;
    }
    records.retain(|record| !is_own(own, record.key));
    records.extend(own_records(tree, worker_id, read_ts, own));
    records
}

/// The record with the smallest key that the transaction sees in the range
/// of `scan`, its own changes (`own`, see [`try_visit_range`]) included.
pub(crate) fn overlay_min<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Table,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    scan: RangeQueryIter<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    read_ts: Version,
    own: &[OwnWrite<Table, Key>],
) -> Option<RecordPointResult<Key, Payload>> {
    if own.is_empty() {
        return scan.min_by_key();
    }
    let best = scan.min_by_key_where(|key| !is_own(own, key));
    // `own` is sorted, so its first key with an own version is its minimum.
    let own_best = own_records(tree, worker_id, read_ts, own).next();
    match (best, own_best) {
        (Some(b), Some(o)) => Some(if o.key < b.key { o } else { b }),
        (b, o) => b.or(o),
    }
}
