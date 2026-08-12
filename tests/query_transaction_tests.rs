use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_db::{Database, DbTransaction};
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_root::index_root::RootIndexType;
use crate::mv_sync::clock::GlobalClock;
use crate::mv_sync::commit_log::CommitLog;
use crate::mv_sync::visibility::{SnapshotCache, is_visible};

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
        tx.range_count(first, crate::mv_query::interval::Interval::new(0, 10)),
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

/// Mirrors the paper's Figure 3 worked example: worker W1 commits two
/// transactions ("a" then "e"), worker W2 starts a transaction ("d")
/// that never commits, and a transaction "f" on a third worker takes
/// its snapshot after a/e committed but while d is still open. f must
/// see a and e, but not d — exactly the Transitive Commit Invariant
/// this project's `CommitLog`/`is_visible` are built on.
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

/// A multi-op `DbTransaction`'s writes must survive a crash: each
/// `insert`/`update`/`delete` logs to the WAL as it happens (under the
/// transaction's one fixed stamp), and `commit` waits for all of them to
/// be durably flushed before returning — so once `commit()` has
/// returned, every write the transaction made must reappear after
/// `open_recovered`, even though recovery mints fresh stamps for
/// everything it replays and has no notion of transaction boundaries.
#[test]
fn multi_op_transaction_writes_are_durable_across_recovery() {
    let path = std::env::temp_dir().join(format!("cmvbt_tx_wal_test_{}.log", std::process::id()));
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
        // ts_commit — see the note in the mv_db integration tests.
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

/// The motivating gap from `mv_db::transaction`'s type doc: before this
/// feature, `mv_sync::visibility::is_visible`'s same-worker fast path
/// treated a transaction's own writes as visible forever, regardless of
/// whether it ever committed. A transaction that writes a key, then hits a
/// `Conflict` on a later op in the *same* transaction and drops without
/// `commit()`, must have its earlier write actually reverted — not just
/// leave its snapshot released while the write sits in the tree as if
/// committed.
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

/// An aborted write must not resurface after a crash + recovery:
/// `Drop`'s abort path is purely in-memory (see `MVBTSt::abort_write`'s
/// doc) and this transaction never logs a Commit marker for it, so
/// replay's commit-gating skips these writes entirely and ends up at
/// the same "never really happened" result the live abort produced.
#[test]
fn aborted_transaction_write_does_not_resurface_after_recovery() {
    let path = std::env::temp_dir().join(format!(
        "cmvbt_tx_abort_wal_test_{}.log",
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
