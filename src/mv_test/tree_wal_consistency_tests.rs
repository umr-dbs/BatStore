//! Verifies, under real concurrent load, that (a) an `MVBTSt`/`mv_db::Database`
//! tree's live data is exactly what was written to it, and (b) the raw WAL
//! file on disk — decoded independently of `mv_wal::recovery::replay`, not
//! by calling it — contains that same committed data, byte for byte.
//! `reconstruct_from_wal`/`reconstruct_from_table_wal` deliberately duplicate
//! (rather than reuse) the commit-gating + sort-by-`(ts_commit, seq)` logic
//! `replay` already has, so a bug in `replay` itself couldn't silently make
//! these checks agree with it for the wrong reason.

use std::collections::HashMap;
use std::time::Duration;

use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_db::{Database, DbTransaction, TableId};
use crate::mv_record_model::tx_stamp::WorkerId;
use crate::mv_record_model::version_info::Version;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_wal::record::{self, WalEntry};

const FAN: usize = 8;
type TestTree = MVBTSt<FAN, FAN, u64, u64>;
type TestDb = Database<FAN, FAN, u64, u64>;

fn inc(k: u64) -> u64 { k.checked_add(1).unwrap_or(u64::MAX) }
fn dec(k: u64) -> u64 { k.checked_sub(1).unwrap_or(u64::MIN) }

fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("cmvbt_consistency_{name}_{}.log", std::process::id()))
}

/// Reconstructs "key -> final committed payload" purely from a plain
/// (non-table-tagged) WAL file's raw bytes.
fn reconstruct_from_wal(path: &std::path::Path) -> HashMap<u64, u64> {
    let bytes = std::fs::read(path).unwrap();

    let mut writes: Vec<(WorkerId, Version, usize, CRUDOperation<u64, u64>)> = Vec::new();
    let mut commits: HashMap<(WorkerId, Version), Version> = HashMap::new();

    let mut offset = 0usize;
    let mut seq = 0usize;
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        match record::decode_entry::<u64, u64>(body) {
            Some(WalEntry::Write(rec)) => {
                writes.push((rec.stamp.worker_id(), rec.stamp.ts_start(), seq, rec.op));
                seq += 1;
            }
            Some(WalEntry::Commit { stamp, ts_commit }) => {
                commits.insert((stamp.worker_id(), stamp.ts_start()), ts_commit);
            }
            None => break,
        }
        offset += consumed;
    }

    let mut committed: Vec<(Version, usize, CRUDOperation<u64, u64>)> = writes
        .into_iter()
        .filter_map(|(worker, ts_start, seq, op)| {
            commits.get(&(worker, ts_start)).map(|&ts_commit| (ts_commit, seq, op))
        })
        .collect();
    committed.sort_by_key(|(ts_commit, seq, _)| (*ts_commit, *seq));

    let mut state = HashMap::new();
    for (_, _, op) in committed {
        match op {
            CRUDOperation::Insert(k, v) | CRUDOperation::Update(k, v) => { state.insert(k, v); }
            CRUDOperation::Delete(k) => { state.remove(&k); }
            _ => {}
        }
    }
    state
}

/// Same idea, for a `mv_db::Database`'s single shared, table-tagged WAL:
/// reconstructs "(table, key) -> final committed payload" from the raw
/// bytes, demultiplexing by the leading `TableId` on each `Write` entry.
fn reconstruct_from_table_wal(path: &std::path::Path) -> HashMap<(TableId, u64), u64> {
    let bytes = std::fs::read(path).unwrap();

    let mut writes: Vec<(WorkerId, Version, usize, TableId, CRUDOperation<u64, u64>)> = Vec::new();
    let mut commits: HashMap<(WorkerId, Version), Version> = HashMap::new();

    let mut offset = 0usize;
    let mut seq = 0usize;
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        match record::decode_entry_for_table::<u64, u64>(body) {
            Some((table_id, WalEntry::Write(rec))) => {
                writes.push((rec.stamp.worker_id(), rec.stamp.ts_start(), seq, table_id, rec.op));
                seq += 1;
            }
            Some((_, WalEntry::Commit { stamp, ts_commit })) => {
                commits.insert((stamp.worker_id(), stamp.ts_start()), ts_commit);
            }
            None => break,
        }
        offset += consumed;
    }

    let mut committed: Vec<(Version, usize, TableId, CRUDOperation<u64, u64>)> = writes
        .into_iter()
        .filter_map(|(worker, ts_start, seq, table_id, op)| {
            commits.get(&(worker, ts_start)).map(|&ts_commit| (ts_commit, seq, table_id, op))
        })
        .collect();
    committed.sort_by_key(|(ts_commit, seq, _, _)| (*ts_commit, *seq));

    let mut state = HashMap::new();
    for (_, _, table_id, op) in committed {
        match op {
            CRUDOperation::Insert(k, v) | CRUDOperation::Update(k, v) => { state.insert((table_id, k), v); }
            CRUDOperation::Delete(k) => { state.remove(&(table_id, k)); }
            _ => {}
        }
    }
    state
}

/// Many threads insert disjoint keys concurrently through the plain
/// single-op `dispatch_crud` path. Once every write is confirmed durable,
/// both the live tree and an independent reconstruction of the raw WAL
/// bytes must show *exactly* the same key/payload pairs — no more, no
/// fewer, no wrong values.
#[test]
fn concurrent_inserts_are_present_in_tree_and_match_wal_exactly() {
    let path = temp_path("insert");
    let _ = std::fs::remove_file(&path);

    let tree = TestTree::make_standard(RootIndexType::default());
    tree.enable_wal(&path, Duration::from_millis(2)).unwrap();

    const THREADS: u64 = 8;
    const KEYS_PER_THREAD: u64 = 250;

    let max_ts = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let tree = &tree;
                scope.spawn(move || {
                    let mut local_max = 0;
                    for i in 0..KEYS_PER_THREAD {
                        let key = t * KEYS_PER_THREAD + i;
                        let payload = key * 31 + 7;
                        match tree.dispatch_crud(CRUDOperation::Insert(key, payload)) {
                            CRUDOperationResult::Inserted(ts) => local_max = local_max.max(ts),
                            other => panic!("insert of key {key} failed: {other}"),
                        }
                    }
                    local_max
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).max().unwrap()
    });

    tree.wait_wal_hardened(max_ts);

    let version = tree.current_version();
    for t in 0..THREADS {
        for i in 0..KEYS_PER_THREAD {
            let key = t * KEYS_PER_THREAD + i;
            let expected = key * 31 + 7;
            match tree.dispatch_crud(CRUDOperation::Point(key, version)) {
                CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == expected => {}
                other => panic!("tree missing/wrong data for key {key}: {other}"),
            }
        }
    }

    let wal_state = reconstruct_from_wal(&path);
    assert_eq!(
        wal_state.len(),
        (THREADS * KEYS_PER_THREAD) as usize,
        "WAL must contain exactly the committed keys, no more/fewer"
    );
    for t in 0..THREADS {
        for i in 0..KEYS_PER_THREAD {
            let key = t * KEYS_PER_THREAD + i;
            let expected = key * 31 + 7;
            assert_eq!(wal_state.get(&key), Some(&expected), "WAL data for key {key} doesn't match what was inserted");
        }
    }

    drop(tree);
    let _ = std::fs::remove_file(&path);
}

/// Each thread owns a disjoint key range (so outcomes stay deterministic
/// despite real concurrency) and, per key, inserts then updates it, deleting
/// even keys afterwards. Checked three independent ways once everything is
/// durable: the live tree, a from-scratch reconstruction of the raw WAL
/// bytes, and a genuine `open_recovered` replay into a fresh tree — all three
/// must agree exactly on which keys exist and what they hold.
#[test]
fn concurrent_insert_update_delete_matches_wal_and_recovery() {
    let path = temp_path("mixed");
    let _ = std::fs::remove_file(&path);

    const THREADS: u64 = 6;
    const KEYS_PER_THREAD: u64 = 300;

    let final_state: HashMap<u64, u64> = {
        let tree = TestTree::make_standard(RootIndexType::default());
        tree.enable_wal(&path, Duration::from_millis(2)).unwrap();

        let max_ts = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|t| {
                    let tree = &tree;
                    scope.spawn(move || {
                        let mut local_max = 0;
                        for i in 0..KEYS_PER_THREAD {
                            let key = t * KEYS_PER_THREAD + i;

                            match tree.dispatch_crud(CRUDOperation::Insert(key, key * 3 + 1)) {
                                CRUDOperationResult::Inserted(ts) => local_max = local_max.max(ts),
                                other => panic!("insert {key} failed: {other}"),
                            }
                            match tree.dispatch_crud(CRUDOperation::Update(key, key * 3 + 2)) {
                                CRUDOperationResult::Updated(ts) => local_max = local_max.max(ts),
                                other => panic!("update {key} failed: {other}"),
                            }
                            if key % 2 == 0 {
                                match tree.dispatch_crud(CRUDOperation::Delete(key)) {
                                    CRUDOperationResult::Deleted(ts) => local_max = local_max.max(ts),
                                    other => panic!("delete {key} failed: {other}"),
                                }
                            }
                        }
                        local_max
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).max().unwrap()
        });

        tree.wait_wal_hardened(max_ts);

        let mut expected = HashMap::new();
        let version = tree.current_version();
        for t in 0..THREADS {
            for i in 0..KEYS_PER_THREAD {
                let key = t * KEYS_PER_THREAD + i;
                if key % 2 == 0 {
                    match tree.dispatch_crud(CRUDOperation::Point(key, version)) {
                        CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
                        other => panic!("key {key} should be deleted, got {other}"),
                    }
                } else {
                    let expected_payload = key * 3 + 2;
                    match tree.dispatch_crud(CRUDOperation::Point(key, version)) {
                        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == expected_payload => {}
                        other => panic!("key {key} wrong/missing, got {other}"),
                    }
                    expected.insert(key, expected_payload);
                }
            }
        }
        expected
    }; // tree drops here.

    // Cross-check 1: independent reconstruction of the raw WAL bytes.
    let wal_state = reconstruct_from_wal(&path);
    assert_eq!(wal_state, final_state, "WAL-reconstructed state must exactly match the live tree's final state");

    // Cross-check 2: the real recovery path (open_recovered) agrees too.
    let recovered = TestTree::open_recovered(RootIndexType::default(), &path, Duration::from_millis(2)).unwrap();
    let recovered_version = recovered.current_version();
    for (&key, &expected_payload) in &final_state {
        match recovered.dispatch_crud(CRUDOperation::Point(key, recovered_version)) {
            CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == expected_payload => {}
            other => panic!("recovered tree wrong/missing for key {key}: {other}"),
        }
    }
    for t in 0..THREADS {
        for i in 0..KEYS_PER_THREAD {
            let key = t * KEYS_PER_THREAD + i;
            if key % 2 == 0 {
                match recovered.dispatch_crud(CRUDOperation::Point(key, recovered_version)) {
                    CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
                    other => panic!("recovered tree should not have deleted key {key}, got {other}"),
                }
            }
        }
    }

    drop(recovered);
    let _ = std::fs::remove_file(&path);
}

/// The `mv_db::Database` counterpart: many threads run concurrent
/// `DbTransaction`s, each writing its own key to three different tables at
/// once, against one shared WAL. Once durable, every table's live data and
/// the shared WAL's table-demultiplexed reconstruction must agree exactly.
#[test]
fn concurrent_db_transactions_across_tables_match_shared_wal_exactly() {
    let path = temp_path("db_multi_table");
    let meta_path = format!("{}.meta", path.display());
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);

    let db: TestDb = Database::new(RootIndexType::default(), inc, dec, u64::MIN, u64::MAX);
    let t_a = db.create_table("a").table_id().unwrap();
    let t_b = db.create_table("b").table_id().unwrap();
    let t_c = db.create_table("c").table_id().unwrap();
    db.enable_wal(&path, Duration::from_millis(2)).unwrap();

    const THREADS: u64 = 6;
    const TXNS_PER_THREAD: u64 = 100;

    let max_ts = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let db_ref = &db;
                scope.spawn(move || {
                    let mut local_max = 0;
                    for i in 0..TXNS_PER_THREAD {
                        let key = t * TXNS_PER_THREAD + i;
                        let tx = DbTransaction::begin(db_ref);
                        let ts_start = tx.ts_start();

                        assert!(matches!(tx.insert(t_a, key, key * 2 + 1), CRUDOperationResult::Inserted(_)));
                        assert!(matches!(tx.insert(t_b, key, key * 3 + 1), CRUDOperationResult::Inserted(_)));
                        assert!(matches!(tx.insert(t_c, key, key * 5 + 1), CRUDOperationResult::Inserted(_)));
                        tx.commit();

                        local_max = local_max.max(ts_start);
                    }
                    local_max
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).max().unwrap()
    });

    // `wait_wal_hardened` tracks the highest flushed *ts_start*, not
    // ts_commit — see the note on this in the mv_db integration tests.
    db.table(t_a).unwrap().wait_wal_hardened(max_ts);

    let version = db.current_version();
    for t in 0..THREADS {
        for i in 0..TXNS_PER_THREAD {
            let key = t * TXNS_PER_THREAD + i;
            match db.table(t_a).unwrap().dispatch_crud(CRUDOperation::Point(key, version)) {
                CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == key * 2 + 1 => {}
                other => panic!("table a missing/wrong for key {key}: {other}"),
            }
            match db.table(t_b).unwrap().dispatch_crud(CRUDOperation::Point(key, version)) {
                CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == key * 3 + 1 => {}
                other => panic!("table b missing/wrong for key {key}: {other}"),
            }
            match db.table(t_c).unwrap().dispatch_crud(CRUDOperation::Point(key, version)) {
                CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == key * 5 + 1 => {}
                other => panic!("table c missing/wrong for key {key}: {other}"),
            }
        }
    }

    let wal_state = reconstruct_from_table_wal(&path);
    assert_eq!(wal_state.len(), (THREADS * TXNS_PER_THREAD * 3) as usize);
    for t in 0..THREADS {
        for i in 0..TXNS_PER_THREAD {
            let key = t * TXNS_PER_THREAD + i;
            assert_eq!(wal_state.get(&(t_a, key)), Some(&(key * 2 + 1)), "table a WAL mismatch for key {key}");
            assert_eq!(wal_state.get(&(t_b, key)), Some(&(key * 3 + 1)), "table b WAL mismatch for key {key}");
            assert_eq!(wal_state.get(&(t_c, key)), Some(&(key * 5 + 1)), "table c WAL mismatch for key {key}");
        }
    }

    drop(db);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);
}

/// The adversarial case: many threads hammer the *same* small set of keys
/// through full multi-op `DbTransaction`s, so most attempts lose a
/// first-writer-wins race and abort. Regardless of which thread's write
/// actually wins each key (nondeterministic — depends on scheduling), the
/// live table's final state and an independent reconstruction of the raw WAL
/// bytes must land on *exactly* the same values: proof that a losing/aborted
/// attempt never leaks into the WAL's committed history, and that the WAL
/// never disagrees with whichever write really did win.
#[test]
fn contended_concurrent_transactions_tree_and_wal_agree_despite_conflicts() {
    let path = temp_path("contended");
    let meta_path = format!("{}.meta", path.display());
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);

    let db: TestDb = Database::new(RootIndexType::default(), inc, dec, u64::MIN, u64::MAX);
    let t = db.create_table("t").table_id().unwrap();
    db.enable_wal(&path, Duration::from_millis(2)).unwrap();

    const KEYS: u64 = 20;
    for k in 0..KEYS {
        assert!(matches!(db.dispatch_crud(t, CRUDOperation::Insert(k, 0)), CRUDOperationResult::Inserted(_)));
    }

    const THREADS: u64 = 12;
    const ATTEMPTS_PER_THREAD: u64 = 200;

    let (max_ts, total_committed) = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|thread_id| {
                let db_ref = &db;
                scope.spawn(move || {
                    let mut local_max = 0;
                    let mut committed = 0u64;
                    for i in 0..ATTEMPTS_PER_THREAD {
                        let key = (thread_id * ATTEMPTS_PER_THREAD + i) % KEYS;
                        let payload = thread_id * 1_000_000 + i; // uniquely identifies (thread, attempt)

                        let tx = DbTransaction::begin(db_ref);
                        // Extract an owned result first: `tx.update(..)`
                        // returns a `CRUDOperationResult<'static, ..>`, but
                        // matching on it while `tx` is still borrowed would
                        // otherwise keep it alive across the `commit()`/
                        // `drop` below.
                        let update_ts = match tx.update(t, key, payload) {
                            CRUDOperationResult::Updated(ts) => Some(ts),
                            CRUDOperationResult::Conflict => None,
                            other => panic!("unexpected update result: {other}"),
                        };
                        match update_ts {
                            Some(ts) => {
                                // `wait_wal_hardened` tracks the highest
                                // flushed *ts_start*, not ts_commit — fold
                                // only `ts` in here, or this spins forever
                                // (see the note in the mv_db tests).
                                tx.commit();
                                local_max = local_max.max(ts);
                                committed += 1;
                            }
                            None => drop(tx),
                        }
                    }
                    (local_max, committed)
                })
            })
            .collect();
        let results: Vec<(Version, u64)> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        (
            results.iter().map(|r| r.0).max().unwrap(),
            results.iter().map(|r| r.1).sum::<u64>(),
        )
    });
    assert!(total_committed > 0, "expected at least some updates to actually commit under contention");

    db.table(t).unwrap().wait_wal_hardened(max_ts);

    let version = db.current_version();
    let mut tree_state = HashMap::new();
    for k in 0..KEYS {
        match db.table(t).unwrap().dispatch_crud(CRUDOperation::Point(k, version)) {
            CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => { tree_state.insert((t, k), r[0].payload); }
            other => panic!("key {k} missing from tree: {other}"),
        }
    }

    let wal_state = reconstruct_from_table_wal(&path);
    assert_eq!(
        wal_state, tree_state,
        "WAL-reconstructed final state must match the live tree exactly, even under heavy contention/aborts"
    );

    drop(db);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);
}
