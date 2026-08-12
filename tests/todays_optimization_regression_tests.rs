//! Focused regressions for the worker/cache, pruning, leaf-update, and cold
//! tree-layout optimizations. These intentionally overlap broader workload
//! tests at a smaller scale so failures identify the optimized component.

use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_db::Database;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_sync::clock::GlobalClock;
use crate::mv_sync::commit_log::CommitLog;
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_wal::backend::WalBackend;

type TinyTree = MVBTSt<8, 8, u64, u64>;
type TinyDb = Database<8, 8, u64, u64>;

fn point(tree: &TinyTree, key: u64) -> Option<u64> {
    match tree.dispatch_crud(CRUDOperation::PointSi(key)) {
        CRUDOperationResult::MatchedRecords(rows) => rows.first().map(|row| *row.payload),
        CRUDOperationResult::ZeroAffected(_) => None,
        other => panic!("unexpected point result: {other}"),
    }
}

#[test]
fn one_thread_can_cycle_across_three_exactly_sized_trees_without_reacquiring_ids() {
    // Each registry has room for exactly this one thread. Cycling through a
    // third tree forces the two inline TLS slots to use their lazy overflow;
    // losing an old WorkerId would make the next access panic on acquire().
    let trees = [
        TinyTree::make_standard_with_max_workers(RootIndexType::default(), 1),
        TinyTree::make_standard_with_max_workers(RootIndexType::default(), 1),
        TinyTree::make_standard_with_max_workers(RootIndexType::default(), 1),
    ];

    for (i, tree) in trees.iter().enumerate() {
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Insert(1, 100 + i as u64)),
            CRUDOperationResult::Inserted(_)
        ));
    }
    for _ in 0..32 {
        for (i, tree) in trees.iter().enumerate() {
            assert_eq!(point(tree, 1), Some(100 + i as u64));
        }
    }
}

#[test]
fn repeated_commit_log_pruning_reuses_its_scratch_allocation_and_preserves_lcb() {
    let clock = GlobalClock::new();
    let log = CommitLog::new();

    for _ in 0..8 {
        log.commit_pruned(&clock, 4, [1, 2, 3].into_iter());
    }
    let warmed_capacity = log.prune_scratch_capacity();
    assert!(warmed_capacity >= 4);

    for _ in 0..1_000 {
        let before = clock.current_version();
        let committed = log.commit_pruned(&clock, 4, [before].into_iter());
        assert_eq!(log.lcb(committed + 1), committed);
    }

    assert_eq!(
        log.prune_scratch_capacity(),
        warmed_capacity,
        "steady-state prune cycles should not grow/reallocate scratch storage"
    );
    assert!(
        log.len() <= 4,
        "pruning must remain bounded while reusing scratch"
    );
}

#[test]
fn consolidated_leaf_lookup_updates_only_the_physically_newest_version() {
    let tree = TinyTree::make_standard(RootIndexType::default());
    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Insert(7, 10)),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Update(7, 20)),
        CRUDOperationResult::Updated(_)
    ));
    assert!(matches!(
        tree.update_with(7, |value| value + 1),
        CRUDOperationResult::Updated(_)
    ));
    assert_eq!(point(&tree, 7), Some(21));

    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Delete(7)),
        CRUDOperationResult::Deleted(_)
    ));
    assert!(matches!(
        tree.update_with(7, |value| value + 1),
        CRUDOperationResult::ZeroAffected(_)
    ));
    assert_eq!(
        point(&tree, 7),
        None,
        "an optimized update must not resurrect a deleted lineage"
    );
}

#[test]
fn cold_configuration_is_shared_by_database_tables_and_remains_immutable() {
    fn inc(k: u64) -> u64 {
        k.saturating_add(2)
    }
    fn dec(k: u64) -> u64 {
        k.saturating_sub(2)
    }

    let db: TinyDb =
        Database::new_with_max_workers(RootIndexType::default(), inc, dec, 10, 1_000, 1);
    let first = db.create_table("first");
    let second = db.create_table("second");

    assert_eq!(first.cold.min_key, 10);
    assert_eq!(first.cold.max_key, 1_000);
    assert_eq!((first.cold.inc_key)(10), 12);
    assert_eq!((first.cold.dec_key)(12), 10);
    assert!(matches!(first.cold.wal.as_ref(), WalBackend::Off));
    assert!(matches!(second.cold.wal.as_ref(), WalBackend::Off));
    assert_eq!(first.table_id(), Some(0));
    assert_eq!(second.table_id(), Some(1));

    assert!(matches!(
        first.dispatch_crud(CRUDOperation::Insert(12, 99)),
        CRUDOperationResult::Inserted(_)
    ));
    assert_eq!(point(&first, 12), Some(99));
    assert_eq!(first.cold.min_key, 10);
    assert!(matches!(first.cold.wal.as_ref(), WalBackend::Off));
}
