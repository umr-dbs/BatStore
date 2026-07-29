//! Verifies `iter_query` (`CRUDOperation::Range`/`RangeIter`, backed by
//! `RangeQueryIter`) actually returns the correct data after/around
//! concurrent inserts — nothing missing, nothing phantom, and (for the
//! snapshot-isolation test) nothing from a transaction that hadn't committed
//! as of the read's own version. Existing concurrency tests
//! (`tree_wal_consistency_tests.rs`) only ever verify via `Point` lookups;
//! this file is the first coverage of the range/iterator path itself.
//!
//! Keys are always inserted in shuffled (not ascending sequential) order:
//! `Range` has a separate, pre-existing, unrelated bug with ascending
//! sequential-key splits (see `query_dispatch_tests.rs`'s
//! `repeated_failed_updates_do_not_corrupt_later_state` doc) that would
//! otherwise contaminate these results with an unrelated failure.

use std::collections::HashMap;

use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_query::interval::Interval;
use crate::mv_query::iter_query::RangeQueryIter;
use crate::mv_record_model::version_info::Version;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_tree::mvbt::MVBTSt;
use rand::prelude::SliceRandom;

const FAN: usize = 8;
type TestTree = MVBTSt<FAN, FAN, u64, u64>;

fn make_tree() -> TestTree {
    TestTree::make_standard(RootIndexType::default())
}

fn shuffled(range: std::ops::Range<u64>) -> Vec<u64> {
    let mut keys: Vec<u64> = range.collect();
    keys.shuffle(&mut rand::rng());
    keys
}

fn payload_for(key: u64) -> u64 {
    key * 31 + 7
}

fn collect_range(tree: &TestTree, range: Interval<u64>, version: Version) -> HashMap<u64, u64> {
    match tree.dispatch_crud(CRUDOperation::Range(range, version)) {
        CRUDOperationResult::MatchedRecords(records) =>
            records.into_iter().map(|r| (r.key, *r.payload)).collect(),
        other => panic!("unexpected Range result: {other}"),
    }
}

fn collect_range_iter(tree: &TestTree, range: Interval<u64>, version: Version) -> HashMap<u64, u64> {
    match tree.dispatch_crud(CRUDOperation::RangeIter(range, version)) {
        CRUDOperationResult::MatchedRecordIter(iter) =>
            iter.map(|r| (r.key, *r.payload)).collect(),
        other => panic!("unexpected RangeIter result: {other}"),
    }
}

/// Many threads insert disjoint keys concurrently (shuffled insertion order,
/// both across and within threads); once every insert has returned, both the
/// eager `Range` and the lazy `RangeIter` path must yield exactly the full
/// set of (key, payload) pairs actually written — verified two ways: an
/// exact match over the whole key space, and an exact match over an
/// arbitrary sub-range that spans a split boundary.
#[test]
fn range_and_iter_query_return_exactly_the_concurrently_inserted_records() {
    let tree = make_tree();

    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 200;
    const TOTAL: u64 = THREADS * PER_THREAD;

    let mut keys = shuffled(0..TOTAL);
    // Round-robin the shuffled keys across threads rather than contiguous
    // chunks, so no single thread's insert sequence is even locally sorted.
    let mut per_thread_keys: Vec<Vec<u64>> = (0..THREADS).map(|_| Vec::new()).collect();
    for (i, k) in keys.drain(..).enumerate() {
        per_thread_keys[i % THREADS as usize].push(k);
    }

    std::thread::scope(|scope| {
        for chunk in &per_thread_keys {
            let tree = &tree;
            scope.spawn(move || {
                for &key in chunk {
                    match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
                        CRUDOperationResult::Inserted(_) => {}
                        other => panic!("insert of key {key} failed: {other}"),
                    }
                }
            });
        }
    });

    let expected: HashMap<u64, u64> = (0..TOTAL).map(|k| (k, payload_for(k))).collect();
    let version = tree.current_version();
    let full_range = Interval::new(0, TOTAL - 1);

    assert_eq!(collect_range(&tree, full_range, version), expected, "Range missed or corrupted concurrently inserted records");
    assert_eq!(collect_range_iter(&tree, full_range, version), expected, "RangeIter missed or corrupted concurrently inserted records");

    // Sub-range spanning several splits (FAN == 8, TOTAL == 1600 keys): must
    // return exactly the keys inside it, no more, no fewer.
    let lo = TOTAL / 3;
    let hi = lo + TOTAL / 4;
    let sub_range = Interval::new(lo, hi);
    let expected_sub: HashMap<u64, u64> = expected.iter()
        .filter(|&(&k, _)| k >= lo && k <= hi)
        .map(|(&k, &v)| (k, v))
        .collect();

    assert_eq!(collect_range(&tree, sub_range, version), expected_sub, "Range over a sub-interval returned the wrong records");
    assert_eq!(collect_range_iter(&tree, sub_range, version), expected_sub, "RangeIter over a sub-interval returned the wrong records");
}

/// A range query taken at a version from *before* a concurrent batch of
/// inserts must see only the pre-existing data — none of the concurrently
/// inserted keys — even though, by the time the query actually runs, the
/// tree already physically contains them. A query taken *after* must see
/// everything. This is the property the whole `registrations_in_flight`/
/// `live_tx` machinery (see `tx_context_registration_tests.rs`,
/// `loom_registration_ordering.rs`) exists to keep safe under GC, and the
/// property a reader actually cares about: not just "do writes eventually
/// show up" but "does *my* snapshot see exactly what it should."
#[test]
fn range_query_respects_snapshot_isolation_across_concurrent_inserts() {
    let tree = make_tree();

    const BEFORE_COUNT: u64 = 100;
    const THREADS: u64 = 8;
    const AFTER_PER_THREAD: u64 = 100;
    const AFTER_TOTAL: u64 = THREADS * AFTER_PER_THREAD;
    const AFTER_BASE: u64 = 10_000; // disjoint from the "before" key space

    for key in shuffled(0..BEFORE_COUNT) {
        match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert of before-key {key} failed: {other}"),
        }
    }

    let version_before = tree.current_version();

    let mut after_keys = shuffled(AFTER_BASE..AFTER_BASE + AFTER_TOTAL);
    let mut per_thread_keys: Vec<Vec<u64>> = (0..THREADS).map(|_| Vec::new()).collect();
    for (i, k) in after_keys.drain(..).enumerate() {
        per_thread_keys[i % THREADS as usize].push(k);
    }

    std::thread::scope(|scope| {
        for chunk in &per_thread_keys {
            let tree = &tree;
            scope.spawn(move || {
                for &key in chunk {
                    match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
                        CRUDOperationResult::Inserted(_) => {}
                        other => panic!("insert of after-key {key} failed: {other}"),
                    }
                }
            });
        }
    });

    let version_after = tree.current_version();
    let full_range = Interval::new(0, AFTER_BASE + AFTER_TOTAL);

    let expected_before: HashMap<u64, u64> = (0..BEFORE_COUNT).map(|k| (k, payload_for(k))).collect();
    let mut expected_after = expected_before.clone();
    expected_after.extend((AFTER_BASE..AFTER_BASE + AFTER_TOTAL).map(|k| (k, payload_for(k))));

    assert_eq!(
        collect_range(&tree, full_range, version_before),
        expected_before,
        "Range at the pre-insert version saw concurrently inserted keys it shouldn't have"
    );
    assert_eq!(
        collect_range_iter(&tree, full_range, version_before),
        expected_before,
        "RangeIter at the pre-insert version saw concurrently inserted keys it shouldn't have"
    );

    assert_eq!(
        collect_range(&tree, full_range, version_after),
        expected_after,
        "Range at the post-insert version is missing some concurrently inserted keys"
    );
    assert_eq!(
        collect_range_iter(&tree, full_range, version_after),
        expected_after,
        "RangeIter at the post-insert version is missing some concurrently inserted keys"
    );
}

/// `LeafPage` records are append-ordered, never re-sorted by key
/// (`LeafPage::push_uncommitted` always writes at the next free slot) — so
/// `RangeQueryIter::min_by_key` can't just trust `next()`'s first result,
/// it has to actually compare every match within the first matching leaf.
/// Inserting keys in *descending* order specifically catches a naive "just
/// take next()" implementation, which would return the first-inserted
/// (largest, physically-first) key instead of the true minimum — exactly
/// the bug this test guards against regressing (it's what `mv_bench::
/// tpcc_txn::deliver_one_district` relies on `range_min` for: finding the
/// oldest — smallest-key — queued new-order).
#[test]
fn range_min_by_key_finds_the_true_minimum_despite_descending_insertion_order() {
    let tree = make_tree();

    // FAN == 8: comfortably fits in a single leaf (no split forced), so
    // this exercises purely within-leaf ordering, not cross-leaf.
    for key in (0..6u64).rev() {
        match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert of key {key} failed: {other}"),
        }
    }

    let version = tree.current_version();
    let min = RangeQueryIter::new(&tree, version, Interval::new(0, 5), false, tree.worker_id())
        .min_by_key()
        .expect("range should have at least one match");

    assert_eq!(min.key, 0, "min_by_key must return the smallest key in range, not whichever the leaf happened to store first");
    assert_eq!(*min.payload, payload_for(0));
}
