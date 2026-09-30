//! Regression coverage for the borrowed-record split/merge copy path.
//!
//! Structural operations keep retired source leaves intact for old snapshots,
//! sort a lightweight `LeafRecordRef` plan, and clone each survivor directly
//! into its final replacement page. These tests stress that path while reads
//! overlap publication, then compare every committed record with an independent
//! deterministic oracle.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_db::{Database, DbTransaction};
use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_tree::mvbt::MVBTSt;

const FAN: usize = 8;
type TestTree = MVBTSt<FAN, FAN, u64, u64>;
type TestDb = Database<FAN, FAN, u64, u64>;

fn initial_value(key: u64) -> u64 {
    key * 17 + 3
}

fn updated_value(key: u64) -> u64 {
    key * 17 + 1_000_003
}

fn collect_visible(tree: &TestTree, upper: u64) -> BTreeMap<u64, u64> {
    let version = tree.current_version();
    let mut seen = HashSet::new();
    let mut records = BTreeMap::new();
    RangeQueryIter::new(
        tree,
        version,
        Interval::new(0, upper),
        false,
        tree.worker_id(),
    )
    .for_each_ref(|key, payload| {
        assert!(
            seen.insert(key),
            "snapshot {version} returned key {key} twice"
        );
        records.insert(key, *payload);
    });
    records
}

fn assert_point(tree: &TestTree, key: u64, expected: Option<u64>) {
    let result = tree.dispatch_crud(CRUDOperation::Point(key, tree.current_version()));
    match (result, expected) {
        (CRUDOperationResult::MatchedRecords(records), Some(value)) => {
            assert_eq!(
                records.len(),
                1,
                "point lookup returned duplicate key {key}"
            );
            assert_eq!(*records[0].payload, value, "wrong payload for key {key}");
        }
        (CRUDOperationResult::MatchedRecords(records), None) => assert!(
            records.is_empty(),
            "deleted key {key} remained visible with {} versions",
            records.len()
        ),
        (other, expected) => {
            panic!("point lookup for {key} returned {other}; expected {expected:?}")
        }
    }
}

#[test]
fn concurrent_splits_preserve_every_record_during_direct_copy() {
    const WRITERS: usize = 4;
    const READERS: usize = 2;
    const TOTAL: u64 = 1_024;
    const ITERATIONS: usize = 3;

    for iteration in 0..ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        let start = Barrier::new(WRITERS + READERS);
        let writers_left = AtomicUsize::new(WRITERS);

        std::thread::scope(|scope| {
            for writer in 0..WRITERS {
                let tree = &tree;
                let start = &start;
                let writers_left = &writers_left;
                scope.spawn(move || {
                    start.wait();
                    // Interleaving writer key ranges makes all workers contend
                    // on the same succession of filling/splitting leaves.
                    for key in (writer as u64..TOTAL).step_by(WRITERS) {
                        match tree.dispatch_crud(CRUDOperation::Insert(key, initial_value(key))) {
                            CRUDOperationResult::Inserted(_) => {}
                            other => panic!("iteration {iteration}: insert {key} failed: {other}"),
                        }
                        if key % 32 == writer as u64 {
                            std::thread::yield_now();
                        }
                    }
                    writers_left.fetch_sub(1, Ordering::Release);
                });
            }

            for _ in 0..READERS {
                let tree = &tree;
                let start = &start;
                let writers_left = &writers_left;
                scope.spawn(move || {
                    start.wait();
                    loop {
                        for (key, payload) in collect_visible(tree, TOTAL - 1) {
                            assert_eq!(
                                payload,
                                initial_value(key),
                                "iteration {iteration}: torn/wrong record during split"
                            );
                        }
                        if writers_left.load(Ordering::Acquire) == 0 {
                            break;
                        }
                    }
                });
            }
        });

        let actual = collect_visible(&tree, TOTAL - 1);
        let expected: BTreeMap<_, _> = (0..TOTAL).map(|k| (k, initial_value(k))).collect();
        assert_eq!(actual, expected, "iteration {iteration}: final split state");
        for key in 0..TOTAL {
            assert_point(&tree, key, Some(initial_value(key)));
        }
    }
}

#[test]
fn concurrent_updates_deletes_and_merges_match_the_final_oracle() {
    const WRITERS: usize = 4;
    const READERS: usize = 2;
    const TOTAL: u64 = 768;
    const ITERATIONS: usize = 3;

    for iteration in 0..ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        // A permutation avoids making this test depend on ascending-insert
        // behavior while still creating many tiny-FAN leaves.
        for i in 0..TOTAL {
            let key = (i * 73) % TOTAL;
            assert!(matches!(
                tree.dispatch_crud(CRUDOperation::Insert(key, initial_value(key))),
                CRUDOperationResult::Inserted(_)
            ));
        }

        let start = Barrier::new(WRITERS + READERS);
        let writers_left = AtomicUsize::new(WRITERS);
        std::thread::scope(|scope| {
            for writer in 0..WRITERS {
                let tree = &tree;
                let start = &start;
                let writers_left = &writers_left;
                scope.spawn(move || {
                    start.wait();
                    for key in (writer as u64..TOTAL).step_by(WRITERS) {
                        match tree.dispatch_crud(CRUDOperation::Update(key, updated_value(key))) {
                            CRUDOperationResult::Updated(_) => {}
                            other => panic!("iteration {iteration}: update {key} failed: {other}"),
                        }
                        // Leave one third live. Distributed removals drive
                        // underflow and two-source leaf merges across the tree.
                        if key % 3 != 0 {
                            match tree.dispatch_crud(CRUDOperation::Delete(key)) {
                                CRUDOperationResult::Deleted(_) => {}
                                other => {
                                    panic!("iteration {iteration}: delete {key} failed: {other}")
                                }
                            }
                        }
                        if key % 32 == writer as u64 {
                            std::thread::yield_now();
                        }
                    }
                    writers_left.fetch_sub(1, Ordering::Release);
                });
            }

            for _ in 0..READERS {
                let tree = &tree;
                let start = &start;
                let writers_left = &writers_left;
                scope.spawn(move || {
                    start.wait();
                    loop {
                        for (key, payload) in collect_visible(tree, TOTAL - 1) {
                            assert!(
                                payload == initial_value(key) || payload == updated_value(key),
                                "iteration {iteration}: invalid payload {payload} for key {key}"
                            );
                        }
                        if writers_left.load(Ordering::Acquire) == 0 {
                            break;
                        }
                    }
                });
            }
        });

        let expected: BTreeMap<_, _> = (0..TOTAL)
            .filter(|key| key % 3 == 0)
            .map(|key| (key, updated_value(key)))
            .collect();
        assert_eq!(
            collect_visible(&tree, TOTAL - 1),
            expected,
            "iteration {iteration}: final merge state"
        );
        for key in 0..TOTAL {
            assert_point(&tree, key, (key % 3 == 0).then(|| updated_value(key)));
        }
    }
}

fn inc(key: u64) -> u64 {
    key.saturating_add(1)
}

fn dec(key: u64) -> u64 {
    key.saturating_sub(1)
}

#[test]
fn long_lived_snapshot_remains_exact_during_concurrent_replacement_copying() {
    const WRITERS: usize = 4;
    // Large enough for several generations of FAN=8 leaf/root splits while
    // staying below the known lightweight-GC split-convergence stress case
    // caused by retaining hundreds of versions behind one old snapshot.
    const INITIAL: u64 = 64;
    const ADDED: u64 = 64;

    let db = Arc::new(TestDb::new(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
    ));
    let table = db.create_table("direct-copy-history").table_id().unwrap();
    db.enable_gc(false, None);

    let mut setup = DbTransaction::begin(&db);
    for key in 0..INITIAL {
        assert!(matches!(
            setup.insert(table, key, initial_value(key)),
            CRUDOperationResult::Inserted(_)
        ));
    }
    setup.commit();

    // Keep a snapshot older than every update, split, and replacement below.
    let mut old = DbTransaction::begin(&db);
    std::thread::scope(|scope| {
        for writer in 0..WRITERS {
            let db = db.clone();
            scope.spawn(move || {
                for key in (writer as u64..INITIAL).step_by(WRITERS) {
                    let mut tx = DbTransaction::begin(&db);
                    assert!(matches!(
                        tx.update(table, key, updated_value(key)),
                        CRUDOperationResult::Updated(_)
                    ));
                    tx.commit();
                }
                for key in ((INITIAL + writer as u64)..INITIAL + ADDED).step_by(WRITERS) {
                    let mut tx = DbTransaction::begin(&db);
                    assert!(matches!(
                        tx.insert(table, key, updated_value(key)),
                        CRUDOperationResult::Inserted(_)
                    ));
                    tx.commit();
                }
            });
        }
    });

    let mut old_records = BTreeMap::new();
    old.range_for_each(
        table,
        Interval::new(0, INITIAL + ADDED - 1),
        |key, payload| {
            assert!(
                old_records.insert(key, *payload).is_none(),
                "old snapshot returned key {key} twice"
            );
        },
    );
    let expected_old: BTreeMap<_, _> = (0..INITIAL).map(|key| (key, initial_value(key))).collect();
    assert_eq!(old_records, expected_old);
    old.commit();

    let mut current = DbTransaction::begin(&db);
    let mut current_records = BTreeMap::new();
    current.range_for_each(
        table,
        Interval::new(0, INITIAL + ADDED - 1),
        |key, payload| {
            assert!(
                current_records.insert(key, *payload).is_none(),
                "current snapshot returned key {key} twice"
            );
        },
    );
    let expected_current: BTreeMap<_, _> = (0..INITIAL + ADDED)
        .map(|key| (key, updated_value(key)))
        .collect();
    assert_eq!(current_records, expected_current);
    current.commit();
}
