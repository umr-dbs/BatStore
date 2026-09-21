use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_db::{Database, DbTransaction, IsolationLevel};
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_root::index_root::RootIndexType;
use crate::bat_sync::clock::GlobalClock;
use crate::bat_sync::commit_log::CommitLog;
use crate::bat_sync::visibility::{SnapshotCache, is_visible};

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
fn transaction_resolves_each_tables_read_root_only_once() {
    let db = new_db();
    let first = db.create_table("first").table_id().unwrap();
    let second = db.create_table("second").table_id().unwrap();

    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(first, 1, 10),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        setup.insert(second, 2, 20),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut tx = DbTransaction::begin(&db);
    assert_eq!(tx.cached_read_root_count(), 0);
    assert!(
        matches!(tx.point(first, 1), CRUDOperationResult::MatchedRecords(rows) if rows.len() == 1)
    );
    assert_eq!(tx.cached_read_root_count(), 1);
    assert!(
        matches!(tx.point(first, 1), CRUDOperationResult::MatchedRecords(rows) if rows.len() == 1)
    );
    assert_eq!(
        tx.cached_read_root_count(),
        1,
        "a repeated point read must reuse the root"
    );
    assert_eq!(
        tx.range_count(first, crate::bat_query::interval::Interval::new(0, 10)),
        1
    );
    assert_eq!(
        tx.cached_read_root_count(),
        1,
        "point and range reads share the same cache"
    );
    assert!(
        matches!(tx.point(second, 2), CRUDOperationResult::MatchedRecords(rows) if rows.len() == 1)
    );
    assert_eq!(tx.cached_read_root_count(), 2);
    tx.commit();
}

#[test]
fn read_committed_refreshes_automatically_and_keeps_write_stamp() {
    let db = new_db();
    let table = db.create_table("rc").table_id().unwrap();
    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(table, 1, 10),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut tx = DbTransaction::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
    let write_stamp = tx.ts_start();
    assert!(
        matches!(tx.point(table, 1), CRUDOperationResult::MatchedRecords(r) if r[0].payload == 10)
    );

    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut writer = DbTransaction::begin(&db);
                assert!(matches!(
                    writer.update(table, 1, 20),
                    CRUDOperationResult::Updated(_)
                ));
                writer.commit();
            })
            .join()
            .unwrap();
    });

    // The next operation automatically sees the later commit.
    assert!(
        matches!(tx.point(table, 1), CRUDOperationResult::MatchedRecords(r) if r[0].payload == 20)
    );
    assert!(tx.read_ts() > write_stamp);
    assert!(matches!(tx.update(table, 1, 30), CRUDOperationResult::Updated(v) if v == write_stamp));
    assert_eq!(tx.ts_start(), write_stamp);
    tx.commit();

    let mut check = DbTransaction::begin(&db);
    assert!(
        matches!(check.point(table, 1), CRUDOperationResult::MatchedRecords(r) if r[0].payload == 30)
    );
    check.commit();
}

#[test]
fn read_committed_abort_after_refresh_reverts_original_stamp() {
    let db = new_db();
    let table = db.create_table("rc_abort").table_id().unwrap();
    let mut tx = DbTransaction::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
    assert!(matches!(
        tx.insert(table, 7, 70),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(
        matches!(tx.point(table, 7), CRUDOperationResult::MatchedRecords(r) if r[0].payload == 70)
    );
    assert!(tx.abort());
    assert_eq!(db.ctx.live_min_snapshot(), None);
    let mut check = DbTransaction::begin(&db);
    assert!(
        matches!(check.point(table, 7), CRUDOperationResult::MatchedRecords(r) if r.is_empty())
    );
}

#[test]
fn read_committed_statement_survives_commit_log_pruning() {
    let db = new_db();
    let table = db.create_table("rc_prune").table_id().unwrap();
    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(table, 1, 0),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut reader = DbTransaction::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
    let (first_done_tx, first_done_rx) = std::sync::mpsc::sync_channel(0);
    let (continue_tx, continue_rx) = std::sync::mpsc::sync_channel(0);
    std::thread::scope(|scope| {
        let writer_db = &db;
        let writer = scope.spawn(move || {
            for value in 1..=40 {
                let mut writer = DbTransaction::begin(writer_db);
                assert!(matches!(
                    writer.update(table, 1, value),
                    CRUDOperationResult::Updated(_)
                ));
                writer.commit();
                if value == 1 {
                    first_done_tx.send(()).unwrap();
                    continue_rx.recv().unwrap();
                }
            }
        });
        first_done_rx.recv().unwrap();
        assert!(
            matches!(reader.point(table, 1), CRUDOperationResult::MatchedRecords(r) if r[0].payload == 1)
        );
        continue_tx.send(()).unwrap();
        writer.join().unwrap();
    });
    assert!(
        matches!(reader.point(table, 1), CRUDOperationResult::MatchedRecords(r) if r[0].payload == 40)
    );
    reader.commit();
}

#[test]
fn read_committed_insert_conflicts_with_uncommitted_foreign_delete() {
    let db = new_db();
    let table = db.create_table("rc_delete_race").table_id().unwrap();
    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(table, 1, 10),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut deleting = DbTransaction::begin(&db);
    assert!(matches!(
        deleting.delete(table, 1),
        CRUDOperationResult::Deleted(_)
    ));
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut inserting =
                    DbTransaction::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
                assert!(matches!(
                    inserting.insert(table, 1, 20),
                    CRUDOperationResult::Conflict
                ));
            })
            .join()
            .unwrap();
    });
    assert!(deleting.abort());
    let mut check = DbTransaction::begin(&db);
    assert!(
        matches!(check.point(table, 1), CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 10)
    );
}

#[test]
fn read_committed_refreshes_and_replaces_cached_roots_for_each_operation() {
    let db = new_db();
    let a = db.create_table("rc_a").table_id().unwrap();
    let b = db.create_table("rc_b").table_id().unwrap();
    let all = crate::bat_query::interval::Interval::new(0, 100);
    let mut setup = DbTransaction::begin(&db);
    for key in 1..=8 {
        assert!(matches!(
            setup.insert(a, key, key),
            CRUDOperationResult::Inserted(_)
        ));
        assert!(matches!(
            setup.insert(b, key, key),
            CRUDOperationResult::Inserted(_)
        ));
    }
    setup.commit();

    db.enable_gc(false, None);
    let mut reader = DbTransaction::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
    assert_eq!(reader.range_count(a, all), 8);
    let first = reader.read_ts();
    assert_eq!(reader.range_count(b, all), 8);
    assert_eq!(reader.cached_read_root_count(), 1);

    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut writer = DbTransaction::begin(&db);
                for key in 9..=32 {
                    assert!(matches!(
                        writer.insert(a, key, key),
                        CRUDOperationResult::Inserted(_)
                    ));
                    assert!(matches!(
                        writer.insert(b, key, key),
                        CRUDOperationResult::Inserted(_)
                    ));
                }
                writer.commit();
            })
            .join()
            .unwrap();
    });

    assert_eq!(reader.range_count(a, all), 32);
    assert!(reader.read_ts() > first);
    assert_eq!(reader.range_count(b, all), 32);
    assert_eq!(reader.cached_read_root_count(), 1);
    reader.commit();
    assert_eq!(db.ctx.live_min_snapshot(), None);
}

#[test]
fn read_committed_rechecks_committed_delete_automatically() {
    let db = new_db();
    let table = db.create_table("rc_reinsert").table_id().unwrap();
    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(table, 1, 10),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut tx = DbTransaction::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut deleting = DbTransaction::begin(&db);
                assert!(matches!(
                    deleting.delete(table, 1),
                    CRUDOperationResult::Deleted(_)
                ));
                deleting.commit();
            })
            .join()
            .unwrap();
    });
    assert!(matches!(
        tx.insert(table, 1, 20),
        CRUDOperationResult::Inserted(_)
    ));
    tx.commit();
    let mut check = DbTransaction::begin(&db);
    assert!(
        matches!(check.point(table, 1), CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 20)
    );
}

#[test]
fn read_committed_update_and_delete_conflict_with_uncommitted_delete() {
    let db = new_db();
    let table = db.create_table("rc_tombstone").table_id().unwrap();
    let mut setup = DbTransaction::begin(&db);
    assert!(matches!(
        setup.insert(table, 1, 10),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();
    let mut deleting = DbTransaction::begin(&db);
    assert!(matches!(
        deleting.delete(table, 1),
        CRUDOperationResult::Deleted(_)
    ));
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut tx =
                    DbTransaction::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
                assert!(matches!(
                    tx.update(table, 1, 20),
                    CRUDOperationResult::Conflict
                ));
                assert!(matches!(tx.delete(table, 1), CRUDOperationResult::Conflict));
            })
            .join()
            .unwrap();
    });
    assert!(deleting.abort());
}

#[test]
fn read_committed_writes_across_statements_recover_as_one_transaction() {
    let path =
        std::env::temp_dir().join(format!("batstore_rc_recovery_{}.log", std::process::id()));
    let meta = std::path::PathBuf::from(format!("{}.meta", path.display()));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta);

    {
        let db = new_db_with_wal(&path);
        let a = db.create_table("a").table_id().unwrap();
        let b = db.create_table("b").table_id().unwrap();
        let mut tx = DbTransaction::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
        let write_stamp = tx.ts_start();
        assert!(
            matches!(tx.insert(a, 1, 10), CRUDOperationResult::Inserted(v) if v == write_stamp)
        );
        assert!(matches!(tx.update(a, 1, 11), CRUDOperationResult::Updated(v) if v == write_stamp));
        assert!(
            matches!(tx.insert(b, 2, 20), CRUDOperationResult::Inserted(v) if v == write_stamp)
        );
        tx.commit();
        db.table(a).unwrap().wait_wal_hardened(write_stamp);

        let mut aborted = DbTransaction::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
        assert!(matches!(
            aborted.insert(b, 3, 30),
            CRUDOperationResult::Inserted(_)
        ));
        assert!(aborted.abort());
    }

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
    let a = recovered.table_named("a").unwrap().table_id().unwrap();
    let b = recovered.table_named("b").unwrap().table_id().unwrap();
    let mut check = DbTransaction::begin(&recovered);
    assert!(
        matches!(check.point(a, 1), CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 11)
    );
    assert!(
        matches!(check.point(b, 2), CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 20)
    );
    assert!(matches!(check.point(b, 3), CRUDOperationResult::MatchedRecords(r) if r.is_empty()));
    check.commit();
    drop(recovered);
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(meta);
}

#[test]
fn automatic_refresh_keeps_snapshot_isolation_default_unchanged() {
    let db = new_db();
    let table = db.create_table("si_default").table_id().unwrap();
    let mut tx = DbTransaction::begin(&db);
    let start = tx.ts_start();
    let clock = db.current_version();
    assert_eq!(tx.read_ts(), start);
    assert!(matches!(tx.insert(table, 1, 1), CRUDOperationResult::Inserted(v) if v == start));
    assert_eq!(tx.read_ts(), start);
    assert_eq!(db.current_version(), clock);
    tx.commit();
}

#[test]
fn read_committed_drop_releases_both_snapshots_and_rolls_back() {
    let db = new_db();
    let table = db.create_table("rc_drop").table_id().unwrap();
    {
        let mut tx = DbTransaction::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
        assert!(matches!(
            tx.insert(table, 1, 10),
            CRUDOperationResult::Inserted(_)
        ));
        assert!(matches!(
            tx.update(table, 1, 20),
            CRUDOperationResult::Updated(_)
        ));
    }
    assert_eq!(db.ctx.live_min_snapshot(), None);
    let mut check = DbTransaction::begin(&db);
    assert!(
        matches!(check.point(table, 1), CRUDOperationResult::MatchedRecords(r) if r.is_empty())
    );
}

#[test]
fn transaction_owned_olc_traversals_do_not_draw_nested_snapshots() {
    let db = new_db();
    let table = db.create_table("t").table_id().unwrap();
    let mut tx = DbTransaction::begin(&db);
    let clock_after_outer_snapshot = db.current_version();

    assert!(matches!(
        tx.insert(table, 1, 10),
        CRUDOperationResult::Inserted(_)
    ));
    assert_eq!(
        db.current_version(),
        clock_after_outer_snapshot,
        "the transaction's existing registration must cover its OLC write traversal"
    );

    assert!(tx.abort());
    assert_eq!(
        db.current_version(),
        clock_after_outer_snapshot,
        "rollback traversal must reuse the same still-live outer registration"
    );
}

#[test]
fn transitive_commit_invariant_worked_example() {
    let glc = GlobalClock::new();
    let logs = [CommitLog::new(), CommitLog::new(), CommitLog::new()];
    const W1: WorkerId = 0;
    const W2: WorkerId = 1;
    const W3: WorkerId = 2;

    let stamp_a = TxStamp::new(W1, glc.next_timestamp());
    logs[W1 as usize].commit(&glc);

    let stamp_e = TxStamp::new(W1, glc.next_timestamp());
    logs[W1 as usize].commit(&glc);

    // W2 starts "d" but never commits it.
    let stamp_d = TxStamp::new(W2, glc.next_timestamp());

    // f's snapshot: after a/e committed, while d is still in flight.
    let f_ts_start = glc.next_timestamp();
    let mut f_cache = SnapshotCache::new(3);

    assert!(
        is_visible(&logs, &mut f_cache, W3, f_ts_start, stamp_a),
        "f must see a: committed on W1 before f's snapshot"
    );
    assert!(
        is_visible(&logs, &mut f_cache, W3, f_ts_start, stamp_e),
        "f must see e: also committed on W1 before f's snapshot"
    );
    assert!(
        !is_visible(&logs, &mut f_cache, W3, f_ts_start, stamp_d),
        "f must NOT see d: W2 never committed it"
    );
}

#[test]
fn multi_op_transaction_sees_own_writes_and_isolates_others() {
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();

    let mut tx1 = DbTransaction::begin(&db);
    assert!(matches!(
        tx1.insert(t, 1, 100),
        CRUDOperationResult::Inserted(_)
    ));

    // Own writes are visible within the same still-open transaction.
    match tx1.point(t, 1) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 100 => {}
        other => panic!("tx1 should see its own uncommitted write, got {other}"),
    }

    let db_ref = &db;

    // A transaction on a different worker, snapshotting before tx1
    // commits, must not see tx1's (still uncommitted) insert.
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut tx2 = DbTransaction::begin(db_ref);
                match tx2.point(t, 1) {
                    CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
                    other => panic!("tx2 should not see tx1's uncommitted insert yet, got {other}"),
                }
                tx2.commit();
            })
            .join()
            .unwrap();
    });

    tx1.commit();

    // A transaction on yet another worker, snapshotting after tx1's
    // commit, must now see it.
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut tx3 = DbTransaction::begin(db_ref);
                match tx3.point(t, 1) {
                    CRUDOperationResult::MatchedRecords(r)
                        if r.len() == 1 && r[0].payload == 100 => {}
                    other => panic!("tx3 should see tx1's now-committed insert, got {other}"),
                }
                tx3.commit();
            })
            .join()
            .unwrap();
    });
}

#[test]
fn first_writer_wins_conflict() {
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();
    assert!(matches!(
        db.dispatch_crud(t, CRUDOperation::Insert(1, 100)),
        CRUDOperationResult::Inserted(_)
    ));

    let mut tx1 = DbTransaction::begin(&db);

    // A concurrent transaction on another worker updates and commits
    // key 1 *after* tx1's snapshot was already taken.
    let db_ref = &db;
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut tx2 = DbTransaction::begin(db_ref);
                assert!(matches!(
                    tx2.update(t, 1, 200),
                    CRUDOperationResult::Updated(_)
                ));
                tx2.commit();
            })
            .join()
            .unwrap();
    });

    // tx1's snapshot predates tx2's write, so tx1 must lose the race
    // instead of silently overwriting it.
    assert!(matches!(
        tx1.update(t, 1, 999),
        CRUDOperationResult::Conflict
    ));
}

#[test]
fn multi_op_transaction_writes_are_durable_across_recovery() {
    let path =
        std::env::temp_dir().join(format!("batstore_tx_wal_test_{}.log", std::process::id()));
    let meta_path = format!("{}.meta", path.display());
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);

    {
        let db = new_db_with_wal(&path);
        db.create_table("t");
        let t = db.table_named("t").unwrap().table_id().unwrap();

        let mut tx = DbTransaction::begin(&db);
        let ts_start = tx.ts_start();
        assert!(matches!(
            tx.insert(t, 1, 100),
            CRUDOperationResult::Inserted(_)
        ));
        assert!(matches!(
            tx.insert(t, 2, 200),
            CRUDOperationResult::Inserted(_)
        ));
        assert!(matches!(
            tx.update(t, 1, 101),
            CRUDOperationResult::Updated(_)
        ));
        assert!(matches!(tx.delete(t, 2), CRUDOperationResult::Deleted(_)));
        tx.commit();
        // `wait_wal_hardened` tracks the highest flushed *ts_start*, not
        // ts_commit — see the note in the bat_db integration tests.
        db.table_named("t").unwrap().wait_wal_hardened(ts_start);
        // db drops here: commit() already waited for durability, so
        // this isn't relied on for correctness, just cleanup ordering.
    }

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
    let recovered_version = recovered.current_version();
    let tree = recovered.table_named("t").unwrap();

    match tree.dispatch_crud(CRUDOperation::Point(1, recovered_version)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 101 => {}
        other => panic!("key 1 should survive recovery with its updated payload, got {other}"),
    }
    match tree.dispatch_crud(CRUDOperation::Point(2, recovered_version)) {
        CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
        other => panic!("key 2 should stay deleted after recovery, got {other}"),
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);
}

#[test]
fn dropped_transaction_reverts_its_earlier_writes_on_conflict() {
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();

    let mut tx1 = DbTransaction::begin(&db);
    assert!(matches!(
        tx1.insert(t, 1, 100),
        CRUDOperationResult::Inserted(_)
    ));

    // A concurrent transaction on another worker inserts and commits
    // key 2 *after* tx1's snapshot was already taken.
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

    // tx1's snapshot predates tx2's insert of key 2, so tx1's own
    // attempt to write key 2 must lose the race.
    assert!(matches!(
        tx1.insert(t, 2, 111),
        CRUDOperationResult::Conflict
    ));

    // tx1 is dropped here without commit — its earlier write (key 1)
    // must be reverted, not left stuck as if committed.
    drop(tx1);

    // A later transaction on the *same* worker (same thread) must not
    // see the aborted insert — before this feature, the same-worker
    // visibility fast path would have shown it forever.
    let mut tx3 = DbTransaction::begin(&db);
    match tx3.point(t, 1) {
        CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
        other => panic!("key 1 (written by since-aborted tx1) must not be visible, got {other}"),
    }
    tx3.commit();
}

#[test]
fn aborted_transaction_write_does_not_resurface_after_recovery() {
    let path = std::env::temp_dir().join(format!(
        "batstore_tx_abort_wal_test_{}.log",
        std::process::id()
    ));
    let meta_path = format!("{}.meta", path.display());
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);

    {
        let db = new_db_with_wal(&path);
        db.create_table("t");
        let t = db.table_named("t").unwrap().table_id().unwrap();

        // Pre-existing, committed key that the aborting transaction
        // will delete — its abort must undelete it.
        assert!(matches!(
            db.dispatch_crud(t, CRUDOperation::Insert(2, 200)),
            CRUDOperationResult::Inserted(_)
        ));

        let mut tx = DbTransaction::begin(&db);
        assert!(matches!(
            tx.insert(t, 1, 100),
            CRUDOperationResult::Inserted(_)
        ));
        assert!(matches!(tx.delete(t, 2), CRUDOperationResult::Deleted(_)));
        // Dropped without commit(): both writes must be reverted, and
        // both reversals WAL-logged.
        drop(tx);

        // db drops here, simulating a crash.
    }

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
    let recovered_version = recovered.current_version();
    let tree = recovered.table_named("t").unwrap();

    match tree.dispatch_crud(CRUDOperation::Point(1, recovered_version)) {
        CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
        other => panic!("key 1's aborted insert must not resurface after recovery, got {other}"),
    }
    match tree.dispatch_crud(CRUDOperation::Point(2, recovered_version)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 200 => {}
        other => {
            panic!("key 2's aborted delete must be undone (restored) after recovery, got {other}")
        }
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);
}
