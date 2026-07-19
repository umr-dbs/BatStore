use std::time::Duration;

use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_wal::record::{self, WalEntry};

use crate::mv_db::Database;
use crate::mv_db::DbTransaction;

type TestDb = Database<8, 8, u64, u64>;

fn inc(k: u64) -> u64 { k.checked_add(1).unwrap_or(u64::MAX) }
fn dec(k: u64) -> u64 { k.checked_sub(1).unwrap_or(u64::MIN) }

fn new_db() -> TestDb {
    Database::new(RootIndexType::default(), inc, dec, u64::MIN, u64::MAX)
}

/// The cross-table analogue of `mv_bench::tpcc_txn::tests::
/// cross_table_transaction_is_atomic_across_tables`: one `DbTransaction`
/// writes to two different tables, and both writes must become visible to
/// other transactions atomically, as one unit, not one table at a time.
#[test]
fn db_cross_table_transaction_is_atomic_across_tables() {
    let db = new_db();
    let t_a = db.create_table("a").table_id().unwrap();
    let t_b = db.create_table("b").table_id().unwrap();

    let tx1 = DbTransaction::begin(&db);
    assert!(matches!(tx1.insert(t_a, 1, 100), CRUDOperationResult::Inserted(_)));
    assert!(matches!(tx1.insert(t_b, 2, 200), CRUDOperationResult::Inserted(_)));

    // Own writes, across both tables, are visible within the same still-open transaction.
    assert!(matches!(tx1.point(t_a, 1), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));
    assert!(matches!(tx1.point(t_b, 2), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));

    let db_ref = &db;

    // A transaction on a different worker, snapshotting before tx1 commits,
    // must see NEITHER table's write.
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let tx2 = DbTransaction::begin(db_ref);
            assert!(matches!(tx2.point(t_a, 1), CRUDOperationResult::MatchedRecords(r) if r.is_empty()));
            assert!(matches!(tx2.point(t_b, 2), CRUDOperationResult::MatchedRecords(r) if r.is_empty()));
            tx2.commit();
        }).join().unwrap();
    });

    tx1.commit();

    // A transaction snapshotting after tx1's commit must now see both writes.
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let tx3 = DbTransaction::begin(db_ref);
            assert!(matches!(tx3.point(t_a, 1), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));
            assert!(matches!(tx3.point(t_b, 2), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));
            tx3.commit();
        }).join().unwrap();
    });
}

/// The cross-table analogue of `mv_bench::tpcc_txn::tests::
/// dropped_tpcc_txn_reverts_writes_across_tables_on_conflict`: one
/// `DbTransaction` writes two different tables, then loses a
/// first-writer-wins race on a later op and drops without `commit()` — both
/// of its earlier writes, across both tables, must be reverted.
#[test]
fn db_dropped_transaction_reverts_writes_across_tables_on_conflict() {
    let db = new_db();
    let t_a = db.create_table("a").table_id().unwrap();
    let t_b = db.create_table("b").table_id().unwrap();

    let tx1 = DbTransaction::begin(&db);
    assert!(matches!(tx1.insert(t_a, 1, 100), CRUDOperationResult::Inserted(_)));
    assert!(matches!(tx1.insert(t_b, 2, 200), CRUDOperationResult::Inserted(_)));

    // A concurrent transaction inserts and commits a third key *after* tx1's
    // snapshot was already taken.
    let db_ref = &db;
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let tx2 = DbTransaction::begin(db_ref);
            assert!(matches!(tx2.insert(t_b, 3, 300), CRUDOperationResult::Inserted(_)));
            tx2.commit();
        }).join().unwrap();
    });

    // tx1's snapshot predates tx2's insert, so tx1's own attempt to write
    // the same key must lose the race.
    assert!(matches!(tx1.insert(t_b, 3, 999), CRUDOperationResult::Conflict));

    // tx1 is dropped here without commit — both earlier writes must be reverted.
    drop(tx1);

    let tx3 = DbTransaction::begin(&db);
    assert!(matches!(tx3.point(t_a, 1), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
        "table a's write by since-aborted tx1 must not be visible");
    assert!(matches!(tx3.point(t_b, 2), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
        "table b's write by since-aborted tx1 must not be visible");
    tx3.commit();
}

/// The `Database` counterpart to `mv_bench::tpcc_wal_codec::tests::
/// tpcc_database_crash_recovery_round_trip_across_tables`, but additionally
/// asserting only **one** file exists on disk — the concrete proof of "one
/// shared log," not per-table siblings (unlike `TpccDatabase`, whose
/// `enable_wal` creates one file per table via `table_wal_path`).
#[test]
fn db_crash_recovery_round_trip_across_tables() {
    let path = std::env::temp_dir().join(format!("cmvbt_db_crash_test_{}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);

    {
        let db = new_db();
        db.create_table("a");
        db.create_table("b");
        db.enable_wal(&path, Duration::from_millis(2)).unwrap();

        let t_a = db.table_named("a").unwrap().table_id().unwrap();
        let t_b = db.table_named("b").unwrap().table_id().unwrap();

        let tx = DbTransaction::begin(&db);
        let ts_start = tx.ts_start();
        assert!(matches!(tx.insert(t_a, 1, 100), CRUDOperationResult::Inserted(_)));
        assert!(matches!(tx.insert(t_b, 2, 200), CRUDOperationResult::Inserted(_)));
        tx.commit();
        // `wal_hardened_version` tracks the highest flushed *ts_start*, not
        // ts_commit (see `WalWriter::hardened`'s doc) — waiting on ts_commit
        // here would spin forever, since nothing ever logs an entry stamped
        // with that later value.
        db.table_named("a").unwrap().wait_wal_hardened(ts_start);
    } // db drops here: every table's tree is dropped normally, exactly like a real crash would leave nothing behind but the WAL file.

    assert!(path.exists(), "expected the shared WAL file to exist");
    // No per-table sibling files (the TpccDatabase-style `path.<table>` shape).
    let sibling_a = std::path::PathBuf::from(format!("{}.a", path.display()));
    let sibling_b = std::path::PathBuf::from(format!("{}.b", path.display()));
    assert!(!sibling_a.exists() && !sibling_b.exists(),
        "expected a single shared WAL file, found per-table sibling(s) instead");

    let recovered = TestDb::open_recovered(
        RootIndexType::default(), inc, dec, u64::MIN, u64::MAX,
        &path, Duration::from_millis(2),
    ).unwrap();

    let version = recovered.current_version();
    let tree_a = recovered.table_named("a").unwrap();
    let tree_b = recovered.table_named("b").unwrap();

    match tree_a.dispatch_crud(CRUDOperation::Point(1, version)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 100 => {}
        other => panic!("table a should have key 1 after recovery, got {other}"),
    }
    match tree_b.dispatch_crud(CRUDOperation::Point(2, version)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 200 => {}
        other => panic!("table b should have key 2 after recovery, got {other}"),
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.meta", path.display()));
}

/// Proof that the "N commit markers per cross-table transaction" problem
/// `TpccTxn::commit` has (one marker per touched table, since each has its
/// own file) is actually gone with a single shared WAL: after a
/// `DbTransaction` writes 3 different tables and commits, the raw file must
/// contain **exactly one** `WalEntry::Commit` for that transaction's
/// `(worker_id, ts_start)`.
#[test]
fn db_single_commit_marker_per_cross_table_transaction() {
    let path = std::env::temp_dir().join(format!("cmvbt_db_commit_marker_test_{}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let db = new_db();
    db.create_table("a");
    db.create_table("b");
    db.create_table("c");
    db.enable_wal(&path, Duration::from_millis(2)).unwrap();

    let t_a = db.table_named("a").unwrap().table_id().unwrap();
    let t_b = db.table_named("b").unwrap().table_id().unwrap();
    let t_c = db.table_named("c").unwrap().table_id().unwrap();

    let tx = DbTransaction::begin(&db);
    let worker_id = tx.worker_id();
    let ts_start = tx.ts_start();
    assert!(matches!(tx.insert(t_a, 1, 10), CRUDOperationResult::Inserted(_)));
    assert!(matches!(tx.insert(t_b, 2, 20), CRUDOperationResult::Inserted(_)));
    assert!(matches!(tx.insert(t_c, 3, 30), CRUDOperationResult::Inserted(_)));
    tx.commit();
    // Wait on ts_start, not ts_commit — see the note in
    // `db_crash_recovery_round_trip_across_tables`.
    db.table_named("a").unwrap().wait_wal_hardened(ts_start);

    let bytes = std::fs::read(&path).unwrap();
    let mut offset = 0usize;
    let mut commit_markers_for_this_tx = 0usize;
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        if let Some((_, WalEntry::Commit { stamp, .. })) = record::decode_entry_for_table::<u64, u64>(body) {
            if stamp.worker_id() == worker_id && stamp.ts_start() == ts_start {
                commit_markers_for_this_tx += 1;
            }
        }
        offset += consumed;
    }
    assert_eq!(commit_markers_for_this_tx, 1,
        "a transaction touching 3 tables on one shared WAL must log exactly one Commit marker");

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.meta", path.display()));
}

/// A table created *after* `enable_wal`/`enable_gc` are already on must
/// still inherit both — the gap `TpccDatabase` never has to close, since its
/// 14 tables are all built before anything is toggled (see
/// `Database::create_table`'s doc).
#[test]
fn dynamic_table_created_after_wal_and_gc_enabled_inherits_both() {
    let path = std::env::temp_dir().join(format!("cmvbt_db_dynamic_table_test_{}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let db = new_db();
    db.create_table("existing");
    db.enable_wal(&path, Duration::from_millis(2)).unwrap();
    db.enable_gc(false);

    // Created strictly after WAL/GC were turned on.
    let late = db.create_table("late");
    let t_late = late.table_id().unwrap();

    // GC inheritance: many single-op writes through "late" must keep its
    // (shared) commit log pruned near max_workers — same property
    // `mv_query::dispatch::tests::commit_log_stays_bounded_with_gc_enabled`
    // proves for a tree that had GC on from construction.
    for k in 0..10_000u64 {
        assert!(matches!(db.dispatch_crud(t_late, CRUDOperation::Insert(k, k)), CRUDOperationResult::Inserted(_)));
    }
    let worker_id = late.worker_id();
    let max_workers = late.ctx.max_workers();
    let len = late.ctx.commit_log_len(worker_id);
    assert!(len <= max_workers,
        "expected late table's inherited-GC commit log to stay pruned near max_workers ({max_workers}), got {len}");

    // WAL inheritance: a write through "late" must survive crash + recovery.
    let ts_start = {
        let tx = DbTransaction::begin(&db);
        let ts_start = tx.ts_start();
        assert!(matches!(tx.insert(t_late, 99_999, 12_345), CRUDOperationResult::Inserted(_)));
        tx.commit();
        ts_start
    };
    // Wait on ts_start, not ts_commit — see the note in
    // `db_crash_recovery_round_trip_across_tables`.
    late.wait_wal_hardened(ts_start);
    drop(db);

    let recovered = TestDb::open_recovered(
        RootIndexType::default(), inc, dec, u64::MIN, u64::MAX,
        &path, Duration::from_millis(2),
    ).unwrap();

    let version = recovered.current_version();
    match recovered.table_named("late").unwrap().dispatch_crud(CRUDOperation::Point(99_999, version)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 12_345 => {}
        other => panic!("late table's write should survive crash+recovery (WAL inheritance), got {other}"),
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.meta", path.display()));
}

/// `TableId`s are plain sequential indices, assigned in creation order — no
/// hashing, no collisions to worry about — and `create_table` is idempotent
/// by name: re-"creating" an existing table returns the very same id.
#[test]
fn table_ids_are_assigned_sequentially_by_creation_order() {
    let db = new_db();
    let a = db.create_table("a").table_id().unwrap();
    let b = db.create_table("b").table_id().unwrap();
    let c = db.create_table("c").table_id().unwrap();

    assert_eq!((a, b, c), (0, 1, 2));
    assert_eq!(db.create_table("b").table_id().unwrap(), b, "re-creating an existing table must return its original id");
}

/// The concrete proof of the catalog-file mechanism `create_table`/
/// `open_recovered` are built on: enabling WAL persists every table that
/// already exists, in order; each subsequent `create_table` appends one
/// more line; and `open_recovered` reads it back to reconstruct the exact
/// same name order (and therefore the exact same `TableId`s) with no
/// per-name resolution of its own.
#[test]
fn catalog_file_records_tables_in_creation_order_and_survives_recovery() {
    let path = std::env::temp_dir().join(format!("cmvbt_db_catalog_test_{}.log", std::process::id()));
    let meta_path = format!("{}.meta", path.display());
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);

    {
        let db = new_db();
        db.create_table("first"); // created before WAL is on
        db.enable_wal(&path, Duration::from_millis(2)).unwrap();
        db.create_table("second"); // created after — must be appended
        db.create_table("third");
    }

    let catalog = std::fs::read_to_string(&meta_path).unwrap();
    assert_eq!(catalog.lines().collect::<Vec<_>>(), vec!["first", "second", "third"]);

    let recovered = TestDb::open_recovered(
        RootIndexType::default(), inc, dec, u64::MIN, u64::MAX,
        &path, Duration::from_millis(2),
    ).unwrap();

    assert_eq!(recovered.table_named("first").unwrap().table_id(), Some(0));
    assert_eq!(recovered.table_named("second").unwrap().table_id(), Some(1));
    assert_eq!(recovered.table_named("third").unwrap().table_id(), Some(2));

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);
}
