use std::sync::Arc;
use std::time::Duration;

use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_wal::record::{self, WalEntry};

use crate::bat_db::Database;
use crate::bat_db::DbTransaction;

type TestDb = Database<8, 8, u64, u64>;

fn inc(k: u64) -> u64 {
    k.checked_add(1).unwrap_or(u64::MAX)
}
fn dec(k: u64) -> u64 {
    k.checked_sub(1).unwrap_or(u64::MIN)
}

fn new_db() -> TestDb {
    Database::new(RootIndexType::default(), inc, dec, u64::MIN, u64::MAX)
}

#[cfg(feature = "tree-viz")]
#[test]
fn explorer_bundle_exports_two_tables_at_one_snapshot() {
    use crate::bat_db::database::DumpColumn;
    let db = new_db();
    let a = db.create_table("accounts").table_id().unwrap();
    let b = db.create_table("orders").table_id().unwrap();
    let mut tx = DbTransaction::begin(&db);
    assert!(matches!(
        tx.insert(a, 7, 70),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tx.insert(b, 9, 90),
        CRUDOperationResult::Inserted(_)
    ));
    tx.commit();

    let path = std::env::temp_dir().join(format!("batstore-explorer-{}.json", std::process::id()));
    let schemas = vec!["balance", "amount"]
        .into_iter()
        .map(|name| {
            vec![
                DumpColumn {
                    name: "key".into(),
                    data_type: "integer".into(),
                },
                DumpColumn {
                    name: name.into(),
                    data_type: "integer".into(),
                },
            ]
        })
        .collect::<Vec<_>>();
    db.dump_explorer_bundle(&path, &schemas, |table, payload| {
        let field = if table == a { "balance" } else { "amount" };
        serde_json::Map::from_iter([(field.into(), serde_json::json!(payload))])
    })
    .unwrap();
    let bundle: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&path).unwrap()).unwrap();
    std::fs::remove_file(path).unwrap();
    assert_eq!(bundle["format"], "batstore-explorer-bundle-v1");
    assert_eq!(bundle["tables"][0]["name"], "accounts");
    assert_eq!(bundle["tables"][1]["name"], "orders");
    assert_eq!(bundle["tables"][0]["rows"][0]["balance"], 70);
    assert_eq!(bundle["tables"][1]["rows"][0]["amount"], 90);
    assert!(
        bundle["tables"][0]["tree"]["roots"]
            .as_array()
            .unwrap()
            .len()
            > 0
    );
    assert!(
        bundle["tables"][1]["tree"]["roots"]
            .as_array()
            .unwrap()
            .len()
            > 0
    );
    assert!(bundle["glc_last"].as_str().unwrap().parse::<u64>().unwrap() > 0);
    assert_eq!(bundle["tables"][0]["tree"]["glc_last"], bundle["glc_last"]);
    assert!(bundle["tables"][0]["tree"]["commit_logs"].is_array());
}

fn new_db_with_wal(path: &std::path::Path) -> TestDb {
    Database::new_with_wal(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
        path,
        Duration::from_millis(2),
    )
    .unwrap()
}

#[test]
fn empty_read_only_commit_does_not_advance_the_global_clock() {
    let db = new_db();
    let tx = DbTransaction::begin(&db);
    let after_begin = db.current_version();
    assert_eq!(tx.commit(), None);
    assert_eq!(
        db.current_version(),
        after_begin,
        "an empty read-only commit must only unregister its snapshot"
    );
}

#[test]
fn db_transaction_zero_copy_range_terminals_share_its_snapshot() {
    let db = new_db();
    let table = db.create_table("scan").table_id().unwrap();
    let mut load = DbTransaction::begin(&db);
    for key in 0..20 {
        assert!(matches!(
            load.insert(table, key, key * 10),
            CRUDOperationResult::Inserted(_)
        ));
    }
    load.commit();

    let mut tx = DbTransaction::begin(&db);
    let range = crate::bat_query::interval::Interval::new(5, 14);
    assert_eq!(tx.range_count(table, range), 10);
    assert_eq!(
        tx.range_fold(table, range, 0u64, |sum, _, payload| sum + *payload),
        950
    );

    let mut keys = Vec::new();
    tx.range_for_each(table, range, |key, _| keys.push(key));
    keys.sort_unstable();
    assert_eq!(keys, (5..=14).collect::<Vec<_>>());

    let mut visited = 0;
    let stopped = tx.try_range_for_each(table, range, |_, _| {
        visited += 1;
        if visited == 4 { Err("stop") } else { Ok(()) }
    });
    assert_eq!(stopped, Err("stop"));
    assert_eq!(visited, 4);
    tx.commit();
}

#[test]
fn old_snapshot_reads_retired_pre_split_blocks_while_gc_reuse_is_enabled() {
    let db = Arc::new(new_db());
    let table = db.create_table("history").table_id().unwrap();
    db.enable_gc(false, None);

    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(table, 1, 10),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    // This snapshot predates every split and replacement below. It is not
    // pinned to a physical root object; each read must route by ts_start to
    // the retained historical root/child entries.
    let mut old = DbTransaction::begin(&db);
    let writer_db = db.clone();
    std::thread::spawn(move || {
        let mut update = DbTransaction::begin(&writer_db);
        assert!(matches!(
            update.update(table, 1, 20),
            CRUDOperationResult::Updated(_)
        ));
        for key in 2..=256 {
            assert!(matches!(
                update.insert(table, key, key * 10),
                CRUDOperationResult::Inserted(_)
            ));
        }
        update.commit();

        // More allocation after retirement gives GC ample opportunity to
        // reuse eligible blocks; blocks needed by `old` must remain exempt.
        for key in 257..=512 {
            let mut tx = DbTransaction::begin(&writer_db);
            assert!(matches!(
                tx.insert(table, key, key * 10),
                CRUDOperationResult::Inserted(_)
            ));
            tx.commit();
        }
    })
    .join()
    .unwrap();

    assert!(
        matches!(old.point(table, 1), CRUDOperationResult::MatchedRecords(r)
        if r.len() == 1 && r[0].payload == 10)
    );
    assert_eq!(
        old.range_count(
            table,
            crate::bat_query::interval::Interval::new(u64::MIN, u64::MAX)
        ),
        1
    );
    old.commit();

    let mut current = DbTransaction::begin(&db);
    assert!(
        matches!(current.point(table, 1), CRUDOperationResult::MatchedRecords(r)
        if r.len() == 1 && r[0].payload == 20)
    );
    assert_eq!(
        current.range_count(
            table,
            crate::bat_query::interval::Interval::new(u64::MIN, u64::MAX)
        ),
        512
    );
    current.commit();
}

#[test]
fn repeated_delete_reinsert_round_trips_through_wal_recovery() {
    let path = std::env::temp_dir().join(format!(
        "batstore_db_reinsert_recovery_test_{}.log",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.meta", path.display()));

    let db = new_db_with_wal(&path);
    let table = db.create_table("t").table_id().unwrap();
    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(table, 1, 10),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut tx = DbTransaction::begin(&db);
    let ts_start = tx.ts_start();
    for value in 11..=100 {
        assert!(matches!(
            tx.delete(table, 1),
            CRUDOperationResult::Deleted(_)
        ));
        assert!(matches!(
            tx.insert(table, 1, value),
            CRUDOperationResult::Inserted(_)
        ));
    }
    tx.commit();
    db.table(table).unwrap().wait_wal_hardened(ts_start);
    drop(db);

    let recovered = TestDb::open_recovered(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
        &path,
        Duration::from_millis(2),
    )
    .unwrap();
    let mut check = DbTransaction::begin(&recovered);
    assert!(
        matches!(check.point(table, 1), CRUDOperationResult::MatchedRecords(r)
        if r.len() == 1 && r[0].payload == 100)
    );
    check.commit();

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.meta", path.display()));
}

#[test]
fn db_cross_table_transaction_is_atomic_across_tables() {
    let db = new_db();
    let t_a = db.create_table("a").table_id().unwrap();
    let t_b = db.create_table("b").table_id().unwrap();

    let mut tx1 = DbTransaction::begin(&db);
    assert!(matches!(
        tx1.insert(t_a, 1, 100),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tx1.insert(t_b, 2, 200),
        CRUDOperationResult::Inserted(_)
    ));

    // Own writes, across both tables, are visible within the same still-open transaction.
    assert!(matches!(tx1.point(t_a, 1), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));
    assert!(matches!(tx1.point(t_b, 2), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));

    let db_ref = &db;

    // A transaction on a different worker, snapshotting before tx1 commits,
    // must see NEITHER table's write.
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let mut tx2 = DbTransaction::begin(db_ref);
            assert!(matches!(tx2.point(t_a, 1), CRUDOperationResult::MatchedRecords(r) if r.is_empty()));
            assert!(matches!(tx2.point(t_b, 2), CRUDOperationResult::MatchedRecords(r) if r.is_empty()));
            tx2.commit();
        }).join().unwrap();
    });

    tx1.commit();

    // A transaction snapshotting after tx1's commit must now see both writes.
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let mut tx3 = DbTransaction::begin(db_ref);
            assert!(matches!(tx3.point(t_a, 1), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));
            assert!(matches!(tx3.point(t_b, 2), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));
            tx3.commit();
        }).join().unwrap();
    });
}

#[test]
fn db_dropped_transaction_reverts_writes_across_tables_on_conflict() {
    let db = new_db();
    let t_a = db.create_table("a").table_id().unwrap();
    let t_b = db.create_table("b").table_id().unwrap();

    let mut tx1 = DbTransaction::begin(&db);
    assert!(matches!(
        tx1.insert(t_a, 1, 100),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tx1.insert(t_b, 2, 200),
        CRUDOperationResult::Inserted(_)
    ));

    // A concurrent transaction inserts and commits a third key *after* tx1's
    // snapshot was already taken.
    let db_ref = &db;
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut tx2 = DbTransaction::begin(db_ref);
                assert!(matches!(
                    tx2.insert(t_b, 3, 300),
                    CRUDOperationResult::Inserted(_)
                ));
                tx2.commit();
            })
            .join()
            .unwrap();
    });

    // tx1's snapshot predates tx2's insert, so tx1's own attempt to write
    // the same key must lose the race.
    assert!(matches!(
        tx1.insert(t_b, 3, 999),
        CRUDOperationResult::Conflict
    ));

    // tx1 is dropped here without commit — both earlier writes must be reverted.
    drop(tx1);

    let mut tx3 = DbTransaction::begin(&db);
    assert!(
        matches!(tx3.point(t_a, 1), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
        "table a's write by since-aborted tx1 must not be visible"
    );
    assert!(
        matches!(tx3.point(t_b, 2), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
        "table b's write by since-aborted tx1 must not be visible"
    );
    tx3.commit();
}

#[test]
fn db_crash_recovery_round_trip_across_tables() {
    let path =
        std::env::temp_dir().join(format!("batstore_db_crash_test_{}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);

    {
        let db = new_db_with_wal(&path);
        db.create_table("a");
        db.create_table("b");

        let t_a = db.table_named("a").unwrap().table_id().unwrap();
        let t_b = db.table_named("b").unwrap().table_id().unwrap();

        let mut tx = DbTransaction::begin(&db);
        let ts_start = tx.ts_start();
        assert!(matches!(
            tx.insert(t_a, 1, 100),
            CRUDOperationResult::Inserted(_)
        ));
        assert!(matches!(
            tx.insert(t_b, 2, 200),
            CRUDOperationResult::Inserted(_)
        ));
        tx.commit();
        db.table_named("a").unwrap().wait_wal_hardened(ts_start);
    } // db drops here: every table's tree is dropped normally, exactly like a real crash would leave nothing behind but the WAL file.

    assert!(path.exists(), "expected the shared WAL file to exist");
    // No per-table sibling files (the TpccDatabase-style `path.<table>` shape).
    let sibling_a = std::path::PathBuf::from(format!("{}.a", path.display()));
    let sibling_b = std::path::PathBuf::from(format!("{}.b", path.display()));
    assert!(
        !sibling_a.exists() && !sibling_b.exists(),
        "expected a single shared WAL file, found per-table sibling(s) instead"
    );

    let recovered = TestDb::open_recovered(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
        &path,
        Duration::from_millis(2),
    )
    .unwrap();

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

#[test]
fn db_single_commit_marker_per_cross_table_transaction() {
    let path = std::env::temp_dir().join(format!(
        "batstore_db_commit_marker_test_{}.log",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);

    let db = new_db_with_wal(&path);
    db.create_table("a");
    db.create_table("b");
    db.create_table("c");

    let t_a = db.table_named("a").unwrap().table_id().unwrap();
    let t_b = db.table_named("b").unwrap().table_id().unwrap();
    let t_c = db.table_named("c").unwrap().table_id().unwrap();

    let mut tx = DbTransaction::begin(&db);
    let worker_id = tx.worker_id();
    let ts_start = tx.ts_start();
    assert!(matches!(
        tx.insert(t_a, 1, 10),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tx.insert(t_b, 2, 20),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tx.insert(t_c, 3, 30),
        CRUDOperationResult::Inserted(_)
    ));
    tx.commit();
    // Wait on ts_start, not ts_commit — see the note in
    // `db_crash_recovery_round_trip_across_tables`.
    db.table_named("a").unwrap().wait_wal_hardened(ts_start);

    let bytes = std::fs::read(&path).unwrap();
    let mut offset = 0usize;
    let mut commit_markers_for_this_tx = 0usize;
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        if let Some((_, WalEntry::Commit { stamp, .. })) =
            record::decode_entry_for_table::<u64, u64>(body)
        {
            if stamp.worker_id() == worker_id && stamp.ts_start() == ts_start {
                commit_markers_for_this_tx += 1;
            }
        }
        offset += consumed;
    }
    assert_eq!(
        commit_markers_for_this_tx, 1,
        "a transaction touching 3 tables on one shared WAL must log exactly one Commit marker"
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.meta", path.display()));
}

#[test]
fn dynamic_table_created_after_wal_and_gc_enabled_inherits_both() {
    let path = std::env::temp_dir().join(format!(
        "batstore_db_dynamic_table_test_{}.log",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);

    let db = new_db_with_wal(&path);
    db.create_table("existing");
    db.enable_gc(false, None);

    // Created strictly after WAL/GC were turned on.
    let late = db.create_table("late");
    let t_late = late.table_id().unwrap();

    for k in 0..10_000u64 {
        assert!(matches!(
            db.dispatch_crud(t_late, CRUDOperation::Insert(k, k)),
            CRUDOperationResult::Inserted(_)
        ));
    }
    let worker_id = late.worker_id();
    let max_workers = late.ctx.max_workers();
    let len = late.ctx.commit_log_len(worker_id);
    assert!(
        len <= max_workers,
        "expected late table's inherited-GC commit log to stay pruned near max_workers ({max_workers}), got {len}"
    );

    // WAL inheritance: a write through "late" must survive crash + recovery.
    let ts_start = {
        let mut tx = DbTransaction::begin(&db);
        let ts_start = tx.ts_start();
        assert!(matches!(
            tx.insert(t_late, 99_999, 12_345),
            CRUDOperationResult::Inserted(_)
        ));
        tx.commit();
        ts_start
    };
    // Wait on ts_start, not ts_commit — see the note in
    // `db_crash_recovery_round_trip_across_tables`.
    late.wait_wal_hardened(ts_start);
    drop(db);

    let recovered = TestDb::open_recovered(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
        &path,
        Duration::from_millis(2),
    )
    .unwrap();

    let version = recovered.current_version();
    match recovered
        .table_named("late")
        .unwrap()
        .dispatch_crud(CRUDOperation::Point(99_999, version))
    {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 12_345 => {}
        other => panic!(
            "late table's write should survive crash+recovery (WAL inheritance), got {other}"
        ),
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
    assert_eq!(
        db.create_table("b").table_id().unwrap(),
        b,
        "re-creating an existing table must return its original id"
    );
}

#[test]
fn catalog_file_records_tables_in_creation_order_and_survives_recovery() {
    let path = std::env::temp_dir().join(format!(
        "batstore_db_catalog_test_{}.log",
        std::process::id()
    ));
    let meta_path = format!("{}.meta", path.display());
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);

    {
        let db = new_db_with_wal(&path);
        db.create_table("first");
        db.create_table("second"); // created after — must be appended
        db.create_table("third");
    }

    let catalog = std::fs::read_to_string(&meta_path).unwrap();
    assert_eq!(
        catalog.lines().collect::<Vec<_>>(),
        vec!["first", "second", "third"]
    );

    let recovered = TestDb::open_recovered(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
        &path,
        Duration::from_millis(2),
    )
    .unwrap();

    assert_eq!(recovered.table_named("first").unwrap().table_id(), Some(0));
    assert_eq!(recovered.table_named("second").unwrap().table_id(), Some(1));
    assert_eq!(recovered.table_named("third").unwrap().table_id(), Some(2));

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);
}
