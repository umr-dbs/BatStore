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
fn own_worker_historical_snapshot_hides_later_commit() {
    // Worker A changes a key in a transaction that starts at s, worker B
    // draws snapshot t, and A commits after t. Read as of t, A must not see
    // its write, exactly like B: it committed after t.
    let glc = GlobalClock::new();
    let logs = [CommitLog::new(), CommitLog::new()];
    const A: WorkerId = 0;
    const B: WorkerId = 1;

    let stamp = TxStamp::new(A, glc.next_timestamp()); // s
    let t = glc.next_timestamp(); // B's snapshot, s < t

    let mut cache_a = SnapshotCache::new(2);
    assert!(
        is_visible(&logs, &mut cache_a, A, t, stamp),
        "A's still running transaction must see its own write"
    );

    logs[A as usize].commit(&glc); // c > t

    let mut cache_b = SnapshotCache::new(2);
    assert!(
        !is_visible(&logs, &mut cache_b, B, t, stamp),
        "B at t must not see A's write: it committed after t"
    );
    assert!(
        !is_visible(&logs, &mut cache_a, A, t, stamp),
        "A reading as of t must agree with B"
    );
    assert!(
        !is_visible(&logs, &mut SnapshotCache::new(2), A, t, stamp),
        "also with a fresh cache"
    );

    let now = glc.next_timestamp();
    assert!(
        is_visible(&logs, &mut cache_a, A, now, stamp),
        "after the commit, A's write is visible to A's later snapshots"
    );

    // A's next transaction sees its own writes at its start, but a
    // historical snapshot older than that transaction does not.
    let own = TxStamp::new(A, glc.next_timestamp());
    assert!(is_visible(&logs, &mut cache_a, A, own.ts_start(), own));
    assert!(!is_visible(&logs, &mut cache_a, A, t, own));
}

#[test]
fn own_worker_historical_snapshot_hides_aborted_transaction() {
    // Worker A writes in a transaction that starts at s, B draws snapshot
    // t, and A aborts. An older leaf, replaced after t, may still hold the
    // write with a valid stamp, since rollback works in the current leaf
    // only. A historical read by A's next transaction must not accept it.
    let glc = GlobalClock::new();
    let logs = [CommitLog::new(), CommitLog::new()];
    const A: WorkerId = 0;
    const B: WorkerId = 1;

    let aborted = TxStamp::new(A, glc.next_timestamp()); // s
    let t = glc.next_timestamp(); // B's snapshot

    assert!(
        is_visible(&logs, &mut SnapshotCache::new(2), A, t, aborted),
        "while it runs, A's transaction sees its own write"
    );
    logs[A as usize].note_abort(aborted.ts_start());

    let current = TxStamp::new(A, glc.next_timestamp()); // A's next transaction
    let mut cache_a = SnapshotCache::new(2);
    assert!(
        !is_visible(&logs, &mut cache_a, A, t, aborted),
        "A's historical read must not accept the aborted write"
    );
    assert!(!is_visible(&logs, &mut SnapshotCache::new(2), B, t, aborted));
    assert!(
        is_visible(&logs, &mut cache_a, A, current.ts_start(), current),
        "A's next transaction still sees its own writes"
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

// ---------------------------------------------------------------------------
// Own writes into leaves that a structure modification created after the
// transaction began (see `bat_db::own_writes`). A transaction reads at its
// start snapshot and routes past entries newer than it, so without the
// write-set overlay it would read the retired leaf and miss its own change.

/// Begins `tx`, then lets another worker insert enough keys to split every
/// leaf of `t` several times, so that the leaves the transaction writes to
/// afterwards were all created after its snapshot.
fn split_after_begin(db: &TestDb, t: crate::bat_db::TableId) {
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                for k in 100..400u64 {
                    assert!(matches!(
                        db.dispatch_crud(t, CRUDOperation::Insert(k, k)),
                        CRUDOperationResult::Inserted(_)
                    ));
                }
            })
            .join()
            .unwrap();
    });
}

fn point_payload(tx: &mut DbTransaction<'_, FAN, FAN, u64, u64>, t: crate::bat_db::TableId, key: u64) -> Option<u64> {
    match tx.point(t, key) {
        CRUDOperationResult::MatchedRecords(r) => {
            assert!(r.len() <= 1, "a point read returned {} records", r.len());
            r.first().map(|r| *r.payload)
        }
        other => panic!("unexpected {other}"),
    }
}

fn range_pairs(tx: &mut DbTransaction<'_, FAN, FAN, u64, u64>, t: crate::bat_db::TableId, lo: u64, hi: u64) -> Vec<(u64, u64)> {
    use crate::bat_query::interval::Interval;
    match tx.range(t, Interval::new(lo, hi)) {
        CRUDOperationResult::MatchedRecords(r) => {
            let mut pairs: Vec<_> = r.iter().map(|r| (r.key, *r.payload)).collect();
            pairs.sort_unstable();
            pairs
        }
        other => panic!("unexpected {other}"),
    }
}

#[test]
fn own_insert_into_leaf_split_after_begin_is_visible() {
    use crate::bat_query::interval::Interval;
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();
    for k in 0..4u64 {
        db.dispatch_crud(t, CRUDOperation::Insert(k, k));
    }
    let mut tx = DbTransaction::begin(&db);
    split_after_begin(&db, t);
    assert!(matches!(tx.insert(t, 50, 5000), CRUDOperationResult::Inserted(_)));

    assert_eq!(point_payload(&mut tx, t, 50), Some(5000));
    assert_eq!(tx.range_count(t, Interval::new(0, 99)), 5);
    assert_eq!(
        range_pairs(&mut tx, t, 0, 99),
        vec![(0, 0), (1, 1), (2, 2), (3, 3), (50, 5000)]
    );
    assert_eq!(tx.range_fold(t, Interval::new(0, 99), 0, |s, _, p| s + p), 5006);
    assert_eq!(tx.range_min(t, Interval::new(4, 99)).map(|r| r.key), Some(50));
    // The keys inserted by the other worker committed after the snapshot.
    assert_eq!(tx.range_count(t, Interval::new(0, 1000)), 5);
    tx.commit();
}

#[test]
fn own_update_and_delete_after_split_are_visible_exactly_once() {
    use crate::bat_query::interval::Interval;
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();
    for k in 0..4u64 {
        db.dispatch_crud(t, CRUDOperation::Insert(k, k));
    }
    let mut tx = DbTransaction::begin(&db);
    // A first update before the split, a second one after it: the second
    // overwrites the transaction's version in place, in the new leaf only.
    assert!(matches!(tx.update(t, 1, 10), CRUDOperationResult::Updated(_)));
    split_after_begin(&db, t);
    assert!(matches!(tx.update(t, 1, 11), CRUDOperationResult::Updated(_)));
    assert!(matches!(tx.update(t, 2, 20), CRUDOperationResult::Updated(_)));
    assert!(matches!(tx.delete(t, 3), CRUDOperationResult::Deleted(_)));

    assert_eq!(point_payload(&mut tx, t, 1), Some(11));
    assert_eq!(point_payload(&mut tx, t, 2), Some(20));
    assert_eq!(point_payload(&mut tx, t, 3), None);
    assert_eq!(range_pairs(&mut tx, t, 0, 99), vec![(0, 0), (1, 11), (2, 20)]);
    assert_eq!(tx.range_count(t, Interval::new(0, 99)), 3);
    let mut seen = Vec::new();
    tx.range_for_each(t, Interval::new(0, 99), |k, p| seen.push((k, *p)));
    seen.sort_unstable();
    assert_eq!(seen, vec![(0, 0), (1, 11), (2, 20)]);
    tx.commit();

    let mut after = DbTransaction::begin(&db);
    assert_eq!(range_pairs(&mut after, t, 0, 3), vec![(0, 0), (1, 11), (2, 20)]);
    after.commit();
}

/// Why only the transaction's own keys may be read through the newest
/// nodes: a split drops versions whose deletion has committed, although an
/// older snapshot still sees them, and only the retired leaf on the
/// snapshot route keeps them.
#[test]
fn snapshot_reads_of_other_keys_keep_the_snapshot_route() {
    use crate::bat_query::interval::Interval;
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();
    for k in 0..4u64 {
        db.dispatch_crud(t, CRUDOperation::Insert(k, k));
    }
    let mut tx = DbTransaction::begin(&db);
    let db_ref = &db;
    // Another worker updates key 2 and commits after tx's snapshot ...
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                assert!(matches!(
                    db_ref.dispatch_crud(t, CRUDOperation::Update(2, 20)),
                    CRUDOperationResult::Updated(_)
                ));
            })
            .join()
            .unwrap();
    });
    // ... and the leaf is split afterwards, which drops key 2's old version:
    // the update has committed, and its new version follows it. (The last
    // version of a *deleted* key would stay while tx runs; see
    // `bat_sync::retention`.)
    split_after_begin(&db, t);
    assert!(matches!(tx.insert(t, 50, 5000), CRUDOperationResult::Inserted(_)));

    // Key 2 is still in tx's snapshot.
    assert_eq!(point_payload(&mut tx, t, 2), Some(2));
    assert_eq!(
        range_pairs(&mut tx, t, 0, 99),
        vec![(0, 0), (1, 1), (2, 2), (3, 3), (50, 5000)]
    );
    // Routing key 2 like a write finds only the new version, which tx does
    // not see.
    let tree = db.table(t).unwrap();
    assert!(matches!(
        tree.own_key_point_read(2, tx.worker_id(), tx.ts_start()),
        CRUDOperationResult::MatchedRecords(r) if r.is_empty()
    ));
    assert_eq!(tx.range_count(t, Interval::new(0, 99)), 5);
    tx.commit();
}

/// Repeated writes of one key in one transaction: each read and each write
/// that the key's own state decides is answered from the write set, and the
/// tree ends up in the state those answers promised, after commit as well
/// as after abort.
#[test]
fn repeated_own_writes_of_a_key_match_the_tree_after_commit_and_abort() {
    use crate::bat_crud_model::crud_operation_result::CRUDOperationInnerReason::{
        KeyAlreadyDeleted, KeyAlreadyExists,
    };
    use crate::bat_query::interval::Interval;
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();
    for k in 0..4u64 {
        db.dispatch_crud(t, CRUDOperation::Insert(k, k));
    }

    let mut tx = DbTransaction::begin(&db);
    // Insert, delete, re-insert of a new key.
    assert!(matches!(tx.insert(t, 10, 100), CRUDOperationResult::Inserted(_)));
    assert!(matches!(
        tx.insert(t, 10, 101),
        CRUDOperationResult::ZeroAffected(KeyAlreadyExists)
    ));
    assert!(matches!(tx.delete(t, 10), CRUDOperationResult::Deleted(_)));
    assert_eq!(point_payload(&mut tx, t, 10), None);
    assert!(matches!(
        tx.update(t, 10, 102),
        CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
    ));
    assert!(matches!(
        tx.delete(t, 10),
        CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
    ));
    split_after_begin(&db, t);
    assert!(matches!(tx.insert(t, 10, 103), CRUDOperationResult::Inserted(_)));
    assert_eq!(point_payload(&mut tx, t, 10), Some(103));
    // Update, delete, re-insert of a committed key.
    assert!(matches!(tx.update(t, 1, 11), CRUDOperationResult::Updated(_)));
    assert!(matches!(tx.delete(t, 1), CRUDOperationResult::Deleted(_)));
    assert!(matches!(tx.insert(t, 1, 12), CRUDOperationResult::Inserted(_)));
    assert!(matches!(tx.update(t, 1, 13), CRUDOperationResult::Updated(_)));
    assert_eq!(point_payload(&mut tx, t, 1), Some(13));
    // Delete of a committed key.
    assert!(matches!(tx.delete(t, 2), CRUDOperationResult::Deleted(_)));
    assert!(matches!(
        tx.update(t, 2, 22),
        CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
    ));

    let expected = vec![(0, 0), (1, 13), (3, 3), (10, 103)];
    assert_eq!(range_pairs(&mut tx, t, 0, 99), expected);
    assert_eq!(tx.range_count(t, Interval::new(0, 99)), 4);
    assert_eq!(tx.range_min(t, Interval::new(1, 99)).map(|r| r.key), Some(1));
    assert_eq!(tx.range_min(t, Interval::new(2, 99)).map(|r| r.key), Some(3));
    tx.commit();

    let mut after = DbTransaction::begin(&db);
    assert_eq!(range_pairs(&mut after, t, 0, 99), expected);
    assert_eq!(point_payload(&mut after, t, 2), None);
    after.commit();

    // The same kind of sequence, aborted, leaves the committed state.
    let mut aborted = DbTransaction::begin(&db);
    assert!(matches!(aborted.update(t, 0, 50), CRUDOperationResult::Updated(_)));
    assert!(matches!(aborted.delete(t, 0), CRUDOperationResult::Deleted(_)));
    assert!(matches!(aborted.insert(t, 0, 51), CRUDOperationResult::Inserted(_)));
    assert!(matches!(aborted.insert(t, 2, 52), CRUDOperationResult::Inserted(_)));
    assert!(matches!(aborted.delete(t, 3), CRUDOperationResult::Deleted(_)));
    assert_eq!(
        range_pairs(&mut aborted, t, 0, 99),
        vec![(0, 51), (1, 13), (2, 52), (10, 103)]
    );
    assert!(aborted.abort());

    let mut last = DbTransaction::begin(&db);
    assert_eq!(range_pairs(&mut last, t, 0, 99), expected);
    last.commit();
}

/// `range_min` with own keys stops at the first leaf that holds a visible
/// key the transaction has not changed; leaves holding only own keys have
/// to be skipped, and an own version may still undercut that leaf's
/// minimum. Checked against the minimum of the full range result.
#[test]
fn range_min_skips_leaves_of_own_keys() {
    use crate::bat_query::interval::Interval;
    let db = new_db();
    let t = db.create_table("t").table_id().unwrap();
    for k in 0..64u64 {
        db.dispatch_crud(t, CRUDOperation::Insert(k, k));
    }
    let mut tx = DbTransaction::begin(&db);
    let min_of = |tx: &mut DbTransaction<'_, FAN, FAN, u64, u64>, lo, hi| {
        (
            tx.range_min(t, Interval::new(lo, hi)).map(|r| (r.key, *r.payload)),
            range_pairs(tx, t, lo, hi).first().copied(),
        )
    };
    // The first leaves hold only keys this transaction deleted.
    for k in 0..20u64 {
        assert!(matches!(tx.delete(t, k), CRUDOperationResult::Deleted(_)));
    }
    let (min, expected) = min_of(&mut tx, 0, 99);
    assert_eq!(min, Some((20, 20)));
    assert_eq!(min, expected);
    // An own update of the smallest remaining key, after a split.
    split_after_begin(&db, t);
    assert!(matches!(tx.update(t, 20, 2000), CRUDOperationResult::Updated(_)));
    let (min, expected) = min_of(&mut tx, 0, 99);
    assert_eq!(min, Some((20, 2000)));
    assert_eq!(min, expected);
    // An own insert below every other key.
    assert!(matches!(tx.insert(t, 5, 500), CRUDOperationResult::Inserted(_)));
    let (min, expected) = min_of(&mut tx, 0, 99);
    assert_eq!(min, Some((5, 500)));
    assert_eq!(min, expected);
    // A range of own keys only, and an empty one.
    assert_eq!(min_of(&mut tx, 6, 19), (None, None));
    assert!(matches!(tx.delete(t, 5), CRUDOperationResult::Deleted(_)));
    assert_eq!(min_of(&mut tx, 0, 20), (Some((20, 2000)), Some((20, 2000))));
    tx.commit();
}
