use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_db::{Database, DbTransaction};
use crate::bat_root::index_root::RootIndexType;

const FAN: usize = 8;
type TestDb = Database<FAN, FAN, u64, u64>;

fn inc(k: u64) -> u64 {
    k.checked_add(1).unwrap_or(u64::MAX)
}
fn dec(k: u64) -> u64 {
    k.checked_sub(1).unwrap_or(u64::MIN)
}

fn new_db() -> TestDb {
    Database::new(RootIndexType::default(), inc, dec, u64::MIN, u64::MAX)
}

fn new_db_with_wal(path: &std::path::Path) -> TestDb {
    Database::new_with_wal(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
        path,
        std::time::Duration::from_millis(2),
    )
    .unwrap()
}

#[test]
fn explicit_abort_reverts_every_kind_of_write_with_no_conflict() {
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();

    // Committed baseline key, so this transaction has something to update
    // and delete, not just insert.
    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(t, 1, 1),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        setup.insert(t, 2, 2),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut tx = DbTransaction::begin(&db);
    assert!(matches!(
        tx.insert(t, 3, 300),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tx.update(t, 1, 999),
        CRUDOperationResult::Updated(_)
    ));
    assert!(matches!(tx.delete(t, 2), CRUDOperationResult::Deleted(_)));

    // Own writes are visible within the transaction, right up to the abort.
    assert!(matches!(tx.point(t, 3), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));

    assert!(
        tx.abort(),
        "abort() must report that it actually aborted an in-flight transaction"
    );

    // A later transaction on the same worker must see none of it: key 1
    // back at its original value, key 2 still present (delete reverted),
    // key 3 gone entirely (insert reverted).
    let mut check = DbTransaction::begin(&db);
    match check.point(t, 1) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 1 => {}
        other => panic!("key 1's update must be reverted to its original value, got {other}"),
    }
    match check.point(t, 2) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 2 => {}
        other => panic!("key 2's delete must be reverted (undeleted), got {other}"),
    }
    match check.point(t, 3) {
        CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
        other => panic!("key 3's insert must be reverted (invisible), got {other}"),
    }
    check.commit();
}

/// `abort()` on a transaction that never wrote anything must still report
/// success and must not panic walking an empty write set.
#[test]
fn explicit_abort_on_a_transaction_with_no_writes_is_a_safe_noop() {
    let db = new_db();
    db.create_table("t");

    let mut tx = DbTransaction::begin(&db);
    assert!(tx.abort());
}

#[test]
fn repeated_delete_reinsert_reuses_one_transaction_owned_tuple_and_commits_final_value() {
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();
    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(t, 1, 10),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut tx = DbTransaction::begin(&db);
    for value in 11..=1_000 {
        assert!(matches!(tx.delete(t, 1), CRUDOperationResult::Deleted(_)));
        assert!(matches!(
            tx.insert(t, 1, value),
            CRUDOperationResult::Inserted(_)
        ));
    }
    assert!(
        matches!(tx.point(t, 1), CRUDOperationResult::MatchedRecords(r)
        if r.len() == 1 && r[0].payload == 1_000)
    );
    tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert!(
        matches!(check.point(t, 1), CRUDOperationResult::MatchedRecords(r)
        if r.len() == 1 && r[0].payload == 1_000)
    );
    check.commit();
}

#[test]
fn repeated_delete_reinsert_abort_restores_pre_transaction_value() {
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();
    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(t, 1, 10),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut tx = DbTransaction::begin(&db);
    for value in 11..=1_000 {
        assert!(matches!(tx.delete(t, 1), CRUDOperationResult::Deleted(_)));
        assert!(matches!(
            tx.insert(t, 1, value),
            CRUDOperationResult::Inserted(_)
        ));
    }
    assert!(tx.abort());

    let mut check = DbTransaction::begin(&db);
    assert!(
        matches!(check.point(t, 1), CRUDOperationResult::MatchedRecords(r)
        if r.len() == 1 && r[0].payload == 10)
    );
    check.commit();
}

#[test]
fn explicit_abort_reverts_writes_across_tables_atomically() {
    let db = new_db();
    let t_a = db.create_table("a").table_id().unwrap();
    let t_b = db.create_table("b").table_id().unwrap();

    let mut tx = DbTransaction::begin(&db);
    assert!(matches!(
        tx.insert(t_a, 1, 100),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tx.insert(t_b, 2, 200),
        CRUDOperationResult::Inserted(_)
    ));

    assert!(tx.abort());

    let mut check = DbTransaction::begin(&db);
    assert!(
        matches!(check.point(t_a, 1), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
        "table a's write by the aborted transaction must not be visible"
    );
    assert!(
        matches!(check.point(t_b, 2), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
        "table b's write by the aborted transaction must not be visible"
    );
    check.commit();
}

#[test]
fn explicit_abort_after_insert_conflict_reverts_earlier_writes() {
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();

    let mut tx1 = DbTransaction::begin(&db);
    assert!(matches!(
        tx1.insert(t, 1, 100),
        CRUDOperationResult::Inserted(_)
    ));

    // A concurrent transaction on another worker inserts and commits key 2
    // *after* tx1's snapshot was already taken.
    let db_ref = &db;
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut tx2 = DbTransaction::begin(db_ref);
                assert!(matches!(
                    tx2.insert(t, 2, 999),
                    CRUDOperationResult::Inserted(_)
                ));
                tx2.commit();
            })
            .join()
            .unwrap();
    });

    assert!(matches!(
        tx1.insert(t, 2, 111),
        CRUDOperationResult::Conflict
    ));

    // The caller reacts to the conflict by aborting explicitly, instead of
    // just dropping tx1.
    assert!(tx1.abort());

    let mut check = DbTransaction::begin(&db);
    assert!(
        matches!(check.point(t, 1), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
        "key 1 (written by the explicitly-aborted tx1) must not be visible"
    );
    assert!(
        matches!(check.point(t, 2), CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 999),
        "key 2 must keep tx2's committed value, untouched by tx1's abort"
    );
    check.commit();
}

#[test]
fn explicit_abort_after_update_conflict_reverts_earlier_writes() {
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();

    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(t, 1, 1),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        setup.insert(t, 2, 2),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut tx1 = DbTransaction::begin(&db);
    assert!(matches!(
        tx1.update(t, 1, 111),
        CRUDOperationResult::Updated(_)
    ));

    // A concurrent transaction updates and commits key 2 after tx1's
    // snapshot was taken, so tx1's own later update of key 2 must conflict.
    let db_ref = &db;
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut tx2 = DbTransaction::begin(db_ref);
                assert!(matches!(
                    tx2.update(t, 2, 222),
                    CRUDOperationResult::Updated(_)
                ));
                tx2.commit();
            })
            .join()
            .unwrap();
    });

    assert!(matches!(
        tx1.update(t, 2, 333),
        CRUDOperationResult::Conflict
    ));

    assert!(tx1.abort());

    let mut check = DbTransaction::begin(&db);
    match check.point(t, 1) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 1 => {}
        other => {
            panic!("key 1's update by the explicitly-aborted tx1 must be reverted, got {other}")
        }
    }
    match check.point(t, 2) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 222 => {}
        other => {
            panic!("key 2 must keep tx2's committed value, untouched by tx1's abort, got {other}")
        }
    }
    check.commit();
}

#[test]
fn explicit_abort_after_delete_conflict_reverts_earlier_writes() {
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();

    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(t, 1, 1),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        setup.insert(t, 2, 2),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut tx1 = DbTransaction::begin(&db);
    assert!(matches!(tx1.delete(t, 1), CRUDOperationResult::Deleted(_)));

    // A concurrent transaction updates (not deletes) key 2 after tx1's
    // snapshot was taken, pushing a fresh insertion stamp tx1 cannot see.
    let db_ref = &db;
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut tx2 = DbTransaction::begin(db_ref);
                assert!(matches!(
                    tx2.update(t, 2, 222),
                    CRUDOperationResult::Updated(_)
                ));
                tx2.commit();
            })
            .join()
            .unwrap();
    });

    assert!(matches!(tx1.delete(t, 2), CRUDOperationResult::Conflict));

    assert!(tx1.abort());

    let mut check = DbTransaction::begin(&db);
    match check.point(t, 1) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 1 => {}
        other => panic!(
            "key 1's delete by the explicitly-aborted tx1 must be reverted (undeleted), got {other}"
        ),
    }
    match check.point(t, 2) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 222 => {}
        other => {
            panic!("key 2 must keep tx2's committed update, untouched by tx1's abort, got {other}")
        }
    }
    check.commit();
}

#[test]
fn explicit_abort_reverts_every_self_written_version_of_a_repeatedly_written_key() {
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();

    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(t, 1, 1),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        setup.insert(t, 2, 2),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut tx1 = DbTransaction::begin(&db);
    // Two writes to the *same* key within one transaction - both stamped
    // identically (one `TxStamp` per transaction, not per write).
    assert!(matches!(
        tx1.update(t, 1, 111),
        CRUDOperationResult::Updated(_)
    ));
    assert!(matches!(
        tx1.update(t, 1, 222),
        CRUDOperationResult::Updated(_)
    ));

    // A concurrent transaction updates and commits key 2 after tx1's
    // snapshot was taken, so tx1's own later update of key 2 must conflict.
    let db_ref = &db;
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut tx2 = DbTransaction::begin(db_ref);
                assert!(matches!(
                    tx2.update(t, 2, 999),
                    CRUDOperationResult::Updated(_)
                ));
                tx2.commit();
            })
            .join()
            .unwrap();
    });

    assert!(matches!(
        tx1.update(t, 2, 333),
        CRUDOperationResult::Conflict
    ));
    assert!(tx1.abort());

    let mut check = DbTransaction::begin(&db);
    match check.point(t, 1) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 1 => {}
        other => panic!(
            "key 1 must be reverted all the way back to its pre-transaction value (1), got {other}"
        ),
    }
    check.commit();
}

#[test]
fn explicit_abort_write_does_not_resurface_after_recovery() {
    let path = std::env::temp_dir().join(format!(
        "batstore_tx_explicit_abort_wal_test_{}.log",
        std::process::id()
    ));
    let meta_path = format!("{}.meta", path.display());
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);

    {
        let db = new_db_with_wal(&path);
        db.create_table("t");
        let t = db.table_named("t").unwrap().table_id().unwrap();

        // A committed key first, so there's something durable to compare
        // the aborted key against after recovery.
        let mut setup = DbTransaction::begin(&db);
        let setup_ts = setup.ts_start();
        assert!(matches!(
            setup.insert(t, 1, 100),
            CRUDOperationResult::Inserted(_)
        ));
        setup.commit();
        db.table_named("t").unwrap().wait_wal_hardened(setup_ts);

        let mut tx = DbTransaction::begin(&db);
        assert!(matches!(
            tx.insert(t, 2, 200),
            CRUDOperationResult::Inserted(_)
        ));
        assert!(tx.abort());
    } // db drops here, exactly like a real crash would leave nothing more behind.

    assert!(path.exists());

    let recovered = TestDb::open_recovered(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
        &path,
        std::time::Duration::from_millis(2),
    )
    .unwrap();
    let version = recovered.current_version();
    let tree = recovered.table_named("t").unwrap();

    match tree.dispatch_crud(CRUDOperation::Point(1, version)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 100 => {}
        other => panic!("committed key 1 should survive recovery, got {other}"),
    }
    match tree.dispatch_crud(CRUDOperation::Point(2, version)) {
        CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
        other => panic!("explicitly-aborted key 2 must not resurface after recovery, got {other}"),
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);
}

#[test]
fn explicit_abort_releases_the_snapshot_for_gc() {
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();
    db.enable_gc(false, None);

    assert_eq!(db.ctx.live_min_snapshot(), None, "no transaction open yet");

    let mut tx = DbTransaction::begin(&db);
    let ts_start = tx.ts_start();
    assert!(matches!(
        tx.insert(t, 1, 100),
        CRUDOperationResult::Inserted(_)
    ));

    assert_eq!(
        db.ctx.live_min_snapshot(),
        Some(ts_start),
        "the still-open transaction's snapshot must be tracked as live"
    );

    assert!(tx.abort());

    assert_eq!(
        db.ctx.live_min_snapshot(),
        None,
        "abort() must release the snapshot, same as commit() does"
    );
}
