//! Reads of keys that the reading transaction has changed itself.
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
//! While the transaction runs, first-writer-wins keeps every other
//! transaction off a key it has changed, so the newest valid version of
//! that key in the current tree is the transaction's own. Hence:
//! - a point read of such a key routes like a writer
//!   (`MVBTSt::own_key_point_read`);
//! - a scan skips such keys on its snapshot route and visits, for each of
//!   them in its range, what the writer-style route finds instead.
//!
//! Both only apply to keys in the transaction's own write set; every other
//! read keeps the snapshot route, and a transaction that has not written
//! the table it reads pays one scan of its (small) write set.
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

/// The keys of one table and range that a transaction has changed, sorted
/// and without duplicates.
pub(crate) type OwnKeys<Key> = SmallVec<[Key; 8]>;

pub(crate) fn own_keys_in<Key: Ord + Copy + Hash + Display>(
    written: impl IntoIterator<Item = Key>,
    range: &Interval<Key>,
) -> OwnKeys<Key> {
    let mut keys: OwnKeys<Key> = written
        .into_iter()
        .filter(|key| range.contains(*key))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// The transaction's own version of `key`, or `None` if its change deleted
/// the key.
pub(crate) fn own_point<
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
/// the versions of the keys in `own` taken from the transaction's own
/// changes. `zone` must be the zone predicate set on `scan`, if any.
/// Own records are visited after the others, in key order.
pub(crate) fn try_visit_range<
    E,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    scan: RangeQueryIter<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    read_ts: Version,
    own: &[Key],
    zone: Option<(u64, u64)>,
    mut visit: impl FnMut(Key, &Payload) -> Result<(), E>,
) -> Result<(), E> {
    if own.is_empty() {
        return scan.try_for_each_ref(visit);
    }
    scan.try_for_each_ref(|key, payload| {
        if own.binary_search(&key).is_ok() {
            Ok(())
        } else {
            visit(key, payload)
        }
    })?;
    for &key in own {
        if let Some(record) = own_point(tree, worker_id, read_ts, key) {
            if passes_zone(tree, zone, &record.payload) {
                visit(key, &record.payload)?;
            }
        }
    }
    Ok(())
}

/// Infallible form of [`try_visit_range`].
pub(crate) fn visit_range<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    scan: RangeQueryIter<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    read_ts: Version,
    own: &[Key],
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
/// the keys in `own` by the transaction's own versions. With own keys
/// present, the result is returned sorted by key.
pub(crate) fn overlay_records<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    mut records: Vec<RecordPointResult<Key, Payload>>,
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    read_ts: Version,
    own: &[Key],
) -> Vec<RecordPointResult<Key, Payload>> {
    if own.is_empty() {
        return records;
    }
    records.retain(|record| own.binary_search(&record.key).is_err());
    records.extend(
        own.iter()
            .filter_map(|&key| own_point(tree, worker_id, read_ts, key)),
    );
    records.sort_unstable_by_key(|record| record.key);
    records
}

/// The record with the smallest key that the transaction sees in the range
/// of `scan`, its own changes included.
pub(crate) fn overlay_min<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    scan: RangeQueryIter<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    read_ts: Version,
    own: &[Key],
) -> Option<RecordPointResult<Key, Payload>> {
    if own.is_empty() {
        return scan.min_by_key();
    }
    // Rare path: only a transaction that has changed a key of this range
    // takes it, so a full pass over the range is acceptable here.
    let mut best: Option<RecordPointResult<Key, Payload>> = None;
    scan.for_each_ref(|key, payload| {
        if own.binary_search(&key).is_err() && best.as_ref().is_none_or(|b| key < b.key) {
            best = Some(RecordPointResult::new(key, payload.clone()));
        }
    });
    // `own` is sorted, so its first key with an own version is its minimum.
    let own_best = own
        .iter()
        .find_map(|&key| own_point(tree, worker_id, read_ts, key));
    match (best, own_best) {
        (Some(b), Some(o)) => Some(if o.key < b.key { o } else { b }),
        (b, o) => b.or(o),
    }
}
