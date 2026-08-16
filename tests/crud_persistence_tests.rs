//! Straightforward proof that insert/update/delete actually land in the
//! tree (not just that the CRUD call *returns* success), read back through a
//! fresh transaction/snapshot afterwards - once single-threaded, once with
//! several threads each owning a disjoint key range concurrently.

use std::sync::Arc;
use std::thread;

use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_db::Database;
use crate::mv_db::DbTransaction;
use crate::mv_query::interval::Interval;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_tree::mvbt::INIT_TREE_HEIGHT;
use rand::prelude::SliceRandom;

type TestDb = Database<16, 16, u64, u64>;

fn inc(k: u64) -> u64 {
    k.checked_add(1).unwrap_or(u64::MAX)
}
fn dec(k: u64) -> u64 {
    k.checked_sub(1).unwrap_or(u64::MIN)
}

fn new_db() -> TestDb {
    Database::new(RootIndexType::default(), inc, dec, u64::MIN, u64::MAX)
}

#[test]
fn single_threaded_insert_update_delete_lands_in_tree() {
    let db = new_db();
    let table = db.create_table("t").table_id().unwrap();
    let all = Interval::new(u64::MIN, u64::MAX);

    let mut insert_tx = DbTransaction::begin(&db);
    for key in 0..100u64 {
        assert!(matches!(
            insert_tx.insert(table, key, key * 10),
            CRUDOperationResult::Inserted(_)
        ));
    }
    insert_tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert_eq!(check.range_count(table, all), 100);
    for key in 0..100u64 {
        assert!(
            matches!(check.point(table, key), CRUDOperationResult::MatchedRecords(r)
            if r.len() == 1 && r[0].payload == key * 10),
            "key {key} must be present with its inserted payload"
        );
    }
    check.commit();

    // Update every even key; odd keys must be untouched.
    let mut update_tx = DbTransaction::begin(&db);
    for key in (0..100u64).step_by(2) {
        assert!(matches!(
            update_tx.update(table, key, key * 1000),
            CRUDOperationResult::Updated(_)
        ));
    }
    update_tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert_eq!(check.range_count(table, all), 100);
    for key in 0..100u64 {
        let expected = if key % 2 == 0 { key * 1000 } else { key * 10 };
        assert!(
            matches!(check.point(table, key), CRUDOperationResult::MatchedRecords(r)
            if r.len() == 1 && r[0].payload == expected),
            "key {key} must reflect the update only if it was targeted"
        );
    }
    check.commit();

    // Delete every key that's a multiple of 4.
    let mut delete_tx = DbTransaction::begin(&db);
    for key in (0..100u64).step_by(4) {
        assert!(matches!(
            delete_tx.delete(table, key),
            CRUDOperationResult::Deleted(_)
        ));
    }
    delete_tx.commit();

    let mut check = DbTransaction::begin(&db);
    let expected_remaining = 100 - (0..100u64).step_by(4).count();
    assert_eq!(check.range_count(table, all), expected_remaining);
    for key in 0..100u64 {
        if key % 4 == 0 {
            assert!(
                matches!(check.point(table, key), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
                "deleted key {key} must no longer be visible"
            );
        } else {
            let expected = if key % 2 == 0 { key * 1000 } else { key * 10 };
            assert!(
                matches!(check.point(table, key), CRUDOperationResult::MatchedRecords(r)
                if r.len() == 1 && r[0].payload == expected),
                "surviving key {key} must keep its post-update payload"
            );
        }
    }
    check.commit();
}

/// Insertion order and deletion order are two independent shuffles of the
/// same key set, so deletion doesn't just retrace insertion.
#[test]
fn shuffled_insert_then_shuffled_delete_of_same_keys_leaves_tree_logically_empty() {
    let db = new_db();
    let table = db.create_table("t").table_id().unwrap();
    let all = Interval::new(u64::MIN, u64::MAX);

    let mut keys: Vec<u64> = (0..50u64).collect();
    keys.shuffle(&mut rand::rng());

    let mut insert_tx = DbTransaction::begin(&db);
    for &key in &keys {
        assert!(matches!(
            insert_tx.insert(table, key, key),
            CRUDOperationResult::Inserted(_)
        ));
    }
    insert_tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert_eq!(check.range_count(table, all), keys.len());
    check.commit();

    keys.shuffle(&mut rand::rng());
    let mut delete_tx = DbTransaction::begin(&db);
    for &key in &keys {
        assert!(matches!(
            delete_tx.delete(table, key),
            CRUDOperationResult::Deleted(_)
        ));
    }
    delete_tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert_eq!(
        check.range_count(table, all),
        0,
        "tree must be logically empty once every inserted key has been deleted"
    );
    check.commit();
}

#[test]
fn insert_then_delete_same_keys_leaves_tree_empty() {
    let db = new_db();
    let table = db.create_table("t").table_id().unwrap();
    let all = Interval::new(u64::MIN, u64::MAX);

    let mut insert_tx = DbTransaction::begin(&db);
    for key in 0..20u64 {
        assert!(matches!(
            insert_tx.insert(table, key, key),
            CRUDOperationResult::Inserted(_)
        ));
    }
    insert_tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert_eq!(check.range_count(table, all), 20);
    check.commit();

    let mut delete_tx = DbTransaction::begin(&db);
    for key in 0..20u64 {
        assert!(matches!(
            delete_tx.delete(table, key),
            CRUDOperationResult::Deleted(_)
        ));
    }
    delete_tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert_eq!(
        check.range_count(table, all),
        0,
        "tree must be logically empty once every inserted key has been deleted"
    );
    check.commit();
}

/// Large enough, at this file's `FAN_OUT = NUM_RECORDS = 16` test config,
/// to force several real root splits during insertion (a 16-slot leaf
/// overflows almost immediately, and their parents keep splitting in turn
/// as more leaves accumulate) and, symmetrically, several real merges
/// cascading the root back down to a single leaf as every key is deleted -
/// not just the couple of one-leaf-worth-of-keys scenarios the smaller
/// tests elsewhere in this file cover.
const SPLIT_MERGE_KEY_COUNT: u64 = 5_000;

/// Root/leaf underflow classification (`mv_tree::smo::unsafe_degree`) is
/// checked reactively, at the *start* of a write's own traversal - it can
/// never react to that same write's own consequences (a leaf's last record
/// going dead, a parent's child count dropping to 1) until some *later*
/// write revisits the same path. A transaction that deletes every key in
/// one batch and then commits therefore cannot observe full convergence
/// immediately: right up to that commit, every one of its own deletions'
/// dead records is still snapshot-protected by that same open transaction,
/// so `unsafe_degree`'s "is this leaf's dead weight still protected by
/// someone" check correctly refuses to merge it (merging while genuinely
/// protected garbage exists is exactly the shape of the pre-fix livelock -
/// see `has_protected_garbage`'s doc in `smo.rs`). The instant that
/// transaction commits, every one of those dead records stops being
/// protected - but nothing re-checks until a *later* write happens to
/// traverse the same leaf.
///
/// This performs exactly one such write (insert then delete a throwaway key
/// that both land on the tree's rightmost path): harmless on its own, and
/// - now that dead-but-unprotected leaves are correctly classified as
/// `ActiveUnderflow` again - enough to cascade the whole tree's pending
/// collapse to completion in one shot (confirmed empirically: before this
/// fix, the equivalent probe needed ~7 rounds for a 5-level tree to
/// converge; with it, one).
fn settle_pending_collapse<const FAN_OUT: usize, const NUM_RECORDS: usize>(
    db: &Database<FAN_OUT, NUM_RECORDS, u64, u64>,
    table: crate::mv_wal::record::TableId,
    settle_key: u64,
) {
    let mut tx = DbTransaction::begin(db);
    tx.insert(table, settle_key, settle_key);
    tx.commit();
    let mut tx = DbTransaction::begin(db);
    tx.delete(table, settle_key);
    tx.commit();
}

/// Ascending insertion order (unlike `shuffled_insert_then_shuffled_delete_
/// of_same_keys_leaves_tree_logically_empty`'s shuffled insert), deleted in
/// an independent random shuffle - regression coverage, at a different
/// deletion order and at a scale that genuinely exercises multi-level root
/// splits and merges, for the single-threaded delete livelock fixed in
/// `mv_tree::smo::unsafe_degree()` (originally found via `insert_then_
/// delete_same_keys_leaves_tree_empty` below, whose delete order is plain
/// ascending too, at a one-leaf scale).
#[test]
fn ascending_insert_then_random_order_delete_leaves_tree_empty() {
    let db = new_db();
    let tree = db.create_table("t");
    let table = tree.table_id().unwrap();
    let all = Interval::new(u64::MIN, u64::MAX);

    let mut insert_tx = DbTransaction::begin(&db);
    for key in 0..SPLIT_MERGE_KEY_COUNT {
        assert!(matches!(
            insert_tx.insert(table, key, key),
            CRUDOperationResult::Inserted(_)
        ));
    }
    insert_tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert_eq!(check.range_count(table, all), SPLIT_MERGE_KEY_COUNT as usize);
    check.commit();
    assert!(
        tree.root.height() > INIT_TREE_HEIGHT,
        "{SPLIT_MERGE_KEY_COUNT} keys at FAN_OUT=NUM_RECORDS=16 must have split the root \
         past a single leaf (height={})",
        tree.root.height()
    );

    let mut delete_order: Vec<u64> = (0..SPLIT_MERGE_KEY_COUNT).collect();
    delete_order.shuffle(&mut rand::rng());
    let mut delete_tx = DbTransaction::begin(&db);
    for &key in &delete_order {
        assert!(matches!(
            delete_tx.delete(table, key),
            CRUDOperationResult::Deleted(_)
        ));
    }
    delete_tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert_eq!(
        check.range_count(table, all),
        0,
        "tree must be logically empty once every ascending-inserted key has been deleted in random order"
    );
    check.commit();

    settle_pending_collapse(&db, table, SPLIT_MERGE_KEY_COUNT + 1_000_000);
    assert_eq!(
        tree.root.height(),
        INIT_TREE_HEIGHT,
        "deleting every key, plus one settling write (see settle_pending_collapse's doc), \
         must merge the tree all the way back down to a single empty leaf, not leave a taller \
         structure of now-empty internal pages behind"
    );
}

/// Mirror image of the test above: descending insertion order, again
/// deleted in an independent random shuffle, at the same split/merge-
/// forcing scale.
#[test]
fn descending_insert_then_random_order_delete_leaves_tree_empty() {
    let db = new_db();
    let tree = db.create_table("t");
    let table = tree.table_id().unwrap();
    let all = Interval::new(u64::MIN, u64::MAX);

    let mut insert_tx = DbTransaction::begin(&db);
    for key in (0..SPLIT_MERGE_KEY_COUNT).rev() {
        assert!(matches!(
            insert_tx.insert(table, key, key),
            CRUDOperationResult::Inserted(_)
        ));
    }
    insert_tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert_eq!(check.range_count(table, all), SPLIT_MERGE_KEY_COUNT as usize);
    check.commit();
    assert!(
        tree.root.height() > INIT_TREE_HEIGHT,
        "{SPLIT_MERGE_KEY_COUNT} keys at FAN_OUT=NUM_RECORDS=16 must have split the root \
         past a single leaf (height={})",
        tree.root.height()
    );

    let mut delete_order: Vec<u64> = (0..SPLIT_MERGE_KEY_COUNT).collect();
    delete_order.shuffle(&mut rand::rng());
    let mut delete_tx = DbTransaction::begin(&db);
    for &key in &delete_order {
        assert!(matches!(
            delete_tx.delete(table, key),
            CRUDOperationResult::Deleted(_)
        ));
    }
    delete_tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert_eq!(
        check.range_count(table, all),
        0,
        "tree must be logically empty once every descending-inserted key has been deleted in random order"
    );
    check.commit();

    settle_pending_collapse(&db, table, SPLIT_MERGE_KEY_COUNT + 1_000_000);
    assert_eq!(
        tree.root.height(),
        INIT_TREE_HEIGHT,
        "deleting every key, plus one settling write (see settle_pending_collapse's doc), \
         must merge the tree all the way back down to a single empty leaf, not leave a taller \
         structure of now-empty internal pages behind"
    );
}

const CONCURRENT_THREADS: u64 = 8;
const KEYS_PER_THREAD: u64 = 200;

fn keys_for_thread(t: u64) -> std::ops::Range<u64> {
    (t * KEYS_PER_THREAD)..((t + 1) * KEYS_PER_THREAD)
}

#[test]
fn concurrent_insert_update_delete_lands_in_tree() {
    // Workers are handed out once per (tree, thread) and never returned (see
    // `mv_sync::worker::WorkerRegistry`'s doc), and each of the 3 phases below
    // spawns a fresh batch of `CONCURRENT_THREADS` threads plus the checking
    // thread in between - budget generously so registration never runs out.
    let db = Arc::new(TestDb::new_with_max_workers(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
        4 * CONCURRENT_THREADS as usize,
    ));
    let table = db.create_table("t").table_id().unwrap();
    let all = Interval::new(u64::MIN, u64::MAX);
    let total_keys = CONCURRENT_THREADS * KEYS_PER_THREAD;

    // Phase 1: each thread inserts its own disjoint key range, payload == key.
    thread::scope(|scope| {
        for t in 0..CONCURRENT_THREADS {
            let db = db.clone();
            scope.spawn(move || {
                let mut tx = DbTransaction::begin(&db);
                for key in keys_for_thread(t) {
                    assert!(matches!(
                        tx.insert(table, key, key),
                        CRUDOperationResult::Inserted(_)
                    ));
                }
                tx.commit();
            });
        }
    });

    {
        let mut check = DbTransaction::begin(&db);
        assert_eq!(check.range_count(table, all), total_keys as usize);
        for key in 0..total_keys {
            assert!(
                matches!(check.point(table, key), CRUDOperationResult::MatchedRecords(r)
                if r.len() == 1 && r[0].payload == key),
                "key {key} must be present with its inserted payload after concurrent insert"
            );
        }
        check.commit();
    }

    // Phase 2: each thread doubles the payload of its own keys.
    thread::scope(|scope| {
        for t in 0..CONCURRENT_THREADS {
            let db = db.clone();
            scope.spawn(move || {
                let mut tx = DbTransaction::begin(&db);
                for key in keys_for_thread(t) {
                    assert!(matches!(
                        tx.update(table, key, key * 2),
                        CRUDOperationResult::Updated(_)
                    ));
                }
                tx.commit();
            });
        }
    });

    {
        let mut check = DbTransaction::begin(&db);
        assert_eq!(check.range_count(table, all), total_keys as usize);
        let sum = check.range_fold(table, all, 0u64, |acc, _, payload| acc + *payload);
        let expected_sum: u64 = (0..total_keys).map(|k| k * 2).sum();
        assert_eq!(sum, expected_sum);
        check.commit();
    }

    // Phase 3: each thread deletes the even half of its own keys.
    thread::scope(|scope| {
        for t in 0..CONCURRENT_THREADS {
            let db = db.clone();
            scope.spawn(move || {
                let mut tx = DbTransaction::begin(&db);
                for key in keys_for_thread(t).filter(|k| k % 2 == 0) {
                    assert!(matches!(
                        tx.delete(table, key),
                        CRUDOperationResult::Deleted(_)
                    ));
                }
                tx.commit();
            });
        }
    });

    {
        let mut check = DbTransaction::begin(&db);
        let remaining: Vec<u64> = (0..total_keys).filter(|k| k % 2 != 0).collect();
        assert_eq!(check.range_count(table, all), remaining.len());
        for key in 0..total_keys {
            if key % 2 == 0 {
                assert!(
                    matches!(check.point(table, key), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
                    "deleted key {key} must no longer be visible after concurrent delete"
                );
            } else {
                assert!(
                    matches!(check.point(table, key), CRUDOperationResult::MatchedRecords(r)
                    if r.len() == 1 && r[0].payload == key * 2),
                    "surviving key {key} must keep its post-update payload after concurrent delete"
                );
            }
        }
        check.commit();
    }
}
