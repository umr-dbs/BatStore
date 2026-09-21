use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_page_model::node::{Active, Dead};
use crate::bat_root::index_root::RootIndexType;
use crate::bat_tree::mvbt::MVBTSt;

const FAN: usize = 8;
type TestTree = MVBTSt<FAN, FAN, u64, u64>;

fn leaf_counts(tree: &TestTree, key: u64) -> (Active, Dead) {
    let leaf_guard = tree.traversal_write_olc(key);
    let leaf_deref_mut = leaf_guard.deref_mut();
    leaf_deref_mut.as_leaf_page().active_dead_count()
}

#[test]
fn commit_log_grows_unbounded_without_gc_enabled() {
    let tree = TestTree::make_standard(RootIndexType::default());
    tree.allow_historic_query(true);

    for k in 0..10_000u64 {
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Insert(k, k)),
            CRUDOperationResult::Inserted(_)
        ));
    }

    let worker_id = tree.worker_id();
    let max_workers = tree.ctx.max_workers();
    let len = tree.ctx.commit_log_len(worker_id);
    assert!(
        len > max_workers,
        "expected the commit log to grow unbounded with GC off, got only {len} entries (max_workers = {max_workers})"
    );
}

#[test]
fn commit_log_stays_bounded_with_gc_enabled() {
    let tree = TestTree::make_standard(RootIndexType::default());
    tree.enable_gc(false);

    for k in 0..10_000u64 {
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Insert(k, k)),
            CRUDOperationResult::Inserted(_)
        ));
    }

    let worker_id = tree.worker_id();
    let max_workers = tree.ctx.max_workers();
    let len = tree.ctx.commit_log_len(worker_id);
    assert!(
        len <= max_workers,
        "expected the commit log to stay pruned near max_workers ({max_workers}) with GC on, got {len} entries"
    );
}

#[test]
fn failed_update_leaves_counts_unchanged() {
    let tree = TestTree::make_standard(RootIndexType::default());

    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Insert(1, 100)),
        CRUDOperationResult::Inserted(_)
    ));

    // Update on a key that was never inserted: delete_after_update finds
    // no prior record for it at all -> Ok(None).
    let before = leaf_counts(&tree, 1);
    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Update(999, 1)),
        CRUDOperationResult::ZeroAffected(_)
    ));
    assert_eq!(
        leaf_counts(&tree, 1),
        before,
        "counts changed after an Ok(None) (KeyDoesNotExist) failure"
    );

    // Insert key=2, delete it, then Update it again: delete_after_update
    // finds the prior record but it's already deleted -> Err(()).
    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Insert(2, 200)),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Delete(2)),
        CRUDOperationResult::Deleted(_)
    ));

    let before2 = leaf_counts(&tree, 2);
    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Update(2, 201)),
        CRUDOperationResult::ZeroAffected(_)
    ));
    assert_eq!(
        leaf_counts(&tree, 2),
        before2,
        "counts changed after an Err(()) (KeyAlreadyDeleted) failure"
    );
}

#[test]
fn repeated_failed_updates_do_not_corrupt_later_state() {
    let tree = TestTree::make_standard(RootIndexType::default());

    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Insert(1, 100)),
        CRUDOperationResult::Inserted(_)
    ));

    let before = leaf_counts(&tree, 1);
    for k in 1000..1000 + (FAN as u64) * 4 {
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Update(k, 999)),
            CRUDOperationResult::ZeroAffected(_)
        ));
    }
    assert_eq!(
        leaf_counts(&tree, 1),
        before,
        "counts drifted after a batch of failed updates"
    );

    for k in 2..=(FAN as u64) * 3 {
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Insert(k, k * 10)),
            CRUDOperationResult::Inserted(_)
        ));
    }

    let version = tree.current_version();
    for k in 1..=(FAN as u64) * 3 {
        let expected_payload = if k == 1 { 100 } else { k * 10 };
        match tree.dispatch_crud(CRUDOperation::Point(k, version)) {
            CRUDOperationResult::MatchedRecords(records)
                if records.len() == 1 && records[0].payload == expected_payload => {}
            other => {
                panic!("key {k} missing or wrong after failed updates + real inserts: {other}")
            }
        }
    }
}

#[test]
fn update_in_place_still_logs_a_fresh_stamp_while_wal_attached() {
    let path = std::env::temp_dir().join(format!(
        "batstore_dispatch_wal_test_{}.log",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);

    let tree = TestTree::make_standard(RootIndexType::default())
        .with_wal(&path, std::time::Duration::from_millis(2))
        .unwrap();
    tree.enable_gc(true);

    let CRUDOperationResult::Inserted(insert_version) =
        tree.dispatch_crud(CRUDOperation::Insert(1, 100))
    else {
        panic!("expected Inserted");
    };

    let CRUDOperationResult::Updated(update_version) =
        tree.dispatch_crud(CRUDOperation::Update(1, 200))
    else {
        panic!("expected Updated");
    };
    assert!(
        update_version > insert_version,
        "Update must report a fresh version while a WAL is attached, even on the in-place path"
    );

    let _ = std::fs::remove_file(&path);
}
