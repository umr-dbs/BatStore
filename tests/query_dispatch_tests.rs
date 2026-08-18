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

/// Regression test: `MVBTSt::commit_tx` must NOT prune a worker's
/// `CommitLog` while block-reclaim GC is disabled *and* historic-query
/// truncation is disabled — pruning assumes any record whose `LCB` data
/// gets dropped is itself unreachable, which is only true when block
/// reclaim is actually removing dead pages in lockstep (see
/// `TrackerHandleSt`'s type doc). Without GC, dead records — and an
/// explicit historical read at an old `version` — stay reachable
/// forever, so the log must grow unboundedly instead of silently
/// losing the `LCB` data such a read would need.
///
/// A fresh tree's *default* config (`freshest_si_truncate_commit_log =
/// true`) now prunes the commit log down to just each live snapshot's
/// `LCB` even with GC off (see `TxContext::commit_tx`) — so exercising
/// genuinely unbounded growth requires opting out via
/// `allow_historic_query(true)`, which also disables that pruning (see
/// its doc), not just leaving GC untouched.
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

/// Counterpart to `commit_log_grows_unbounded_without_gc_enabled`: once
/// `enable_gc` has actually been called, pruning is sound again (block
/// reclaim is now removing dead pages in the same lockstep `LCB` pruning
/// assumes), so the log should stay bounded near `max_workers`.
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

/// Regression test for a bug found while building the WAL: `Update`'s
/// `Ok(None)` (KeyDoesNotExist) and `Err(())` (KeyAlreadyDeleted)
/// failure branches called `undo_uncommitted` without reversing the
/// `commit_delta(1, 0)` applied just before, permanently inflating the
/// leaf's tracked active count even though nothing was actually
/// inserted. A failed op mutates nothing, so the leaf's tracked
/// (active, dead) counts must be *exactly* the same before and after —
/// checked directly, not via a rescan through `as_records()`, which is
/// itself bounded by the same (possibly-corrupted) length and so can't
/// independently catch this.
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

/// Same bug, repeated many times on a small-fanout tree, then verified
/// two independent ways: the tracked counts must still match their
/// pre-batch value, and driving enough real inserts afterwards to force
/// real splits must both (a) not panic inside smo.rs on a bad fill-ratio
/// read and (b) leave the exact expected key set behind — not one key
/// short, and not with a phantom extra key.
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

    // Point queries, not Range: Range has a separate, pre-existing bug
    // with ascending sequential-key splits (some leaves become
    // unreachable from the root's fence intervals) that's unrelated to
    // the counter-drift fix under test here. Also: `current_version()`,
    // not `current_version_for_reader()` — the latter aggregates across
    // a process-global thread registry (see clock.rs), so under `cargo
    // test`'s parallel test threads it can be dragged down by a
    // completely unrelated test's tree/thread.
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

/// New invariant from simplifying the WAL to log `CRUDOperation`
/// directly: since one logged record must equal one minted version,
/// `Update`'s in-place fast path (which mints none) must never fire
/// while a WAL is attached — every Update must go through the normal
/// versioned path and get a fresh version instead.
#[test]
fn update_in_place_disabled_while_wal_attached() {
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

    // No live readers registered, and GC+update-in-place is on: with no
    // WAL this would take the in-place fast path (see
    // wal_disabled_path_unaffected in wal_integration_tests.rs) and reuse
    // the current version. With a WAL attached it must mint a fresh one.
    let CRUDOperationResult::Updated(update_version) =
        tree.dispatch_crud(CRUDOperation::Update(1, 200))
    else {
        panic!("expected Updated");
    };
    assert!(
        update_version > insert_version,
        "Update must mint a fresh version while a WAL is attached"
    );

    let _ = std::fs::remove_file(&path);
}
