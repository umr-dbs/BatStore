//! Verifies, under real concurrent load, that (a) an `MVBTSt`/`bat_db::Database`
//! tree's live data is exactly what was written to it, and (b) the raw WAL
//! file on disk — decoded independently of `bat_wal::recovery::replay`, not
//! by calling it — contains that same committed data, byte for byte.
//! `reconstruct_from_wal`/`reconstruct_from_table_wal` deliberately duplicate
//! (rather than reuse) the commit-gating + sort-by-`(ts_commit, seq)` logic
//! `replay` already has, so a bug in `replay` itself couldn't silently make
//! these checks agree with it for the wrong reason.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_db::{Database, DbTransaction, TableId};
use crate::bat_record_model::tx_stamp::WorkerId;
use crate::bat_record_model::version_info::Version;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_tree::mvbt::MVBTSt;
use crate::bat_wal::record::{self, WalEntry};

const FAN: usize = 8;
type TestTree = MVBTSt<FAN, FAN, u64, u64>;
type TestDb = Database<FAN, FAN, u64, u64>;

fn inc(k: u64) -> u64 {
    k.checked_add(1).unwrap_or(u64::MAX)
}
fn dec(k: u64) -> u64 {
    k.checked_sub(1).unwrap_or(u64::MIN)
}

fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "batstore_consistency_{name}_{}.log",
        std::process::id()
    ))
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
            commits
                .get(&(worker, ts_start))
                .map(|&ts_commit| (ts_commit, seq, op))
        })
        .collect();
    committed.sort_by_key(|(ts_commit, seq, _)| (*ts_commit, *seq));

    let mut state = HashMap::new();
    for (_, _, op) in committed {
        match op {
            CRUDOperation::Insert(k, v) | CRUDOperation::Update(k, v) => {
                state.insert(k, v);
            }
            CRUDOperation::Delete(k) => {
                state.remove(&k);
            }
            _ => {}
        }
    }
    state
}

/// Same idea, for a `bat_db::Database`'s single shared, table-tagged WAL:
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
                writes.push((
                    rec.stamp.worker_id(),
                    rec.stamp.ts_start(),
                    seq,
                    table_id,
                    rec.op,
                ));
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
            commits
                .get(&(worker, ts_start))
                .map(|&ts_commit| (ts_commit, seq, table_id, op))
        })
        .collect();
    committed.sort_by_key(|(ts_commit, seq, _, _)| (*ts_commit, *seq));

    let mut state = HashMap::new();
    for (_, _, table_id, op) in committed {
        match op {
            CRUDOperation::Insert(k, v) | CRUDOperation::Update(k, v) => {
                state.insert((table_id, k), v);
            }
            CRUDOperation::Delete(k) => {
                state.remove(&(table_id, k));
            }
            _ => {}
        }
    }
    state
}

#[test]
fn concurrent_inserts_are_present_in_tree_and_match_wal_exactly() {
    let path = temp_path("insert");
    let _ = std::fs::remove_file(&path);

    let tree = TestTree::make_standard(RootIndexType::default())
        .with_wal(&path, Duration::from_millis(2))
        .unwrap();

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
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .max()
            .unwrap()
    });

    tree.wait_wal_hardened(max_ts);

    let version = tree.current_version();
    for t in 0..THREADS {
        for i in 0..KEYS_PER_THREAD {
            let key = t * KEYS_PER_THREAD + i;
            let expected = key * 31 + 7;
            match tree.dispatch_crud(CRUDOperation::Point(key, version)) {
                CRUDOperationResult::MatchedRecords(r)
                    if r.len() == 1 && r[0].payload == expected => {}
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
            assert_eq!(
                wal_state.get(&key),
                Some(&expected),
                "WAL data for key {key} doesn't match what was inserted"
            );
        }
    }

    drop(tree);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn concurrent_insert_update_delete_matches_wal_and_recovery() {
    let path = temp_path("mixed");
    let _ = std::fs::remove_file(&path);

    const THREADS: u64 = 6;
    const KEYS_PER_THREAD: u64 = 300;

    let final_state: HashMap<u64, u64> = {
        let tree = TestTree::make_standard(RootIndexType::default())
            .with_wal(&path, Duration::from_millis(2))
            .unwrap();

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
                                    CRUDOperationResult::Deleted(ts) => {
                                        local_max = local_max.max(ts)
                                    }
                                    other => panic!("delete {key} failed: {other}"),
                                }
                            }
                        }
                        local_max
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .max()
                .unwrap()
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
                        CRUDOperationResult::MatchedRecords(r)
                            if r.len() == 1 && r[0].payload == expected_payload => {}
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
    assert_eq!(
        wal_state, final_state,
        "WAL-reconstructed state must exactly match the live tree's final state"
    );

    // Cross-check 2: the real recovery path (open_recovered) agrees too.
    let recovered =
        TestTree::open_recovered(RootIndexType::default(), &path, Duration::from_millis(2))
            .unwrap();
    let recovered_version = recovered.current_version();
    for (&key, &expected_payload) in &final_state {
        match recovered.dispatch_crud(CRUDOperation::Point(key, recovered_version)) {
            CRUDOperationResult::MatchedRecords(r)
                if r.len() == 1 && r[0].payload == expected_payload => {}
            other => panic!("recovered tree wrong/missing for key {key}: {other}"),
        }
    }
    for t in 0..THREADS {
        for i in 0..KEYS_PER_THREAD {
            let key = t * KEYS_PER_THREAD + i;
            if key % 2 == 0 {
                match recovered.dispatch_crud(CRUDOperation::Point(key, recovered_version)) {
                    CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
                    other => {
                        panic!("recovered tree should not have deleted key {key}, got {other}")
                    }
                }
            }
        }
    }

    drop(recovered);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn concurrent_db_transactions_across_tables_match_shared_wal_exactly() {
    let path = temp_path("db_multi_table");
    let meta_path = format!("{}.meta", path.display());
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);

    let db: TestDb = Database::new_with_wal(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
        &path,
        Duration::from_millis(2),
    )
    .unwrap();
    let t_a = db.create_table("a").table_id().unwrap();
    let t_b = db.create_table("b").table_id().unwrap();
    let t_c = db.create_table("c").table_id().unwrap();

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
                        let mut tx = DbTransaction::begin(db_ref);
                        let ts_start = tx.ts_start();

                        assert!(matches!(
                            tx.insert(t_a, key, key * 2 + 1),
                            CRUDOperationResult::Inserted(_)
                        ));
                        assert!(matches!(
                            tx.insert(t_b, key, key * 3 + 1),
                            CRUDOperationResult::Inserted(_)
                        ));
                        assert!(matches!(
                            tx.insert(t_c, key, key * 5 + 1),
                            CRUDOperationResult::Inserted(_)
                        ));
                        tx.commit();

                        local_max = local_max.max(ts_start);
                    }
                    local_max
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .max()
            .unwrap()
    });

    // `wait_wal_hardened` tracks the highest flushed *ts_start*, not
    // ts_commit — see the note on this in the bat_db integration tests.
    db.table(t_a).unwrap().wait_wal_hardened(max_ts);

    let version = db.current_version();
    for t in 0..THREADS {
        for i in 0..TXNS_PER_THREAD {
            let key = t * TXNS_PER_THREAD + i;
            match db
                .table(t_a)
                .unwrap()
                .dispatch_crud(CRUDOperation::Point(key, version))
            {
                CRUDOperationResult::MatchedRecords(r)
                    if r.len() == 1 && r[0].payload == key * 2 + 1 => {}
                other => panic!("table a missing/wrong for key {key}: {other}"),
            }
            match db
                .table(t_b)
                .unwrap()
                .dispatch_crud(CRUDOperation::Point(key, version))
            {
                CRUDOperationResult::MatchedRecords(r)
                    if r.len() == 1 && r[0].payload == key * 3 + 1 => {}
                other => panic!("table b missing/wrong for key {key}: {other}"),
            }
            match db
                .table(t_c)
                .unwrap()
                .dispatch_crud(CRUDOperation::Point(key, version))
            {
                CRUDOperationResult::MatchedRecords(r)
                    if r.len() == 1 && r[0].payload == key * 5 + 1 => {}
                other => panic!("table c missing/wrong for key {key}: {other}"),
            }
        }
    }

    let wal_state = reconstruct_from_table_wal(&path);
    assert_eq!(wal_state.len(), (THREADS * TXNS_PER_THREAD * 3) as usize);
    for t in 0..THREADS {
        for i in 0..TXNS_PER_THREAD {
            let key = t * TXNS_PER_THREAD + i;
            assert_eq!(
                wal_state.get(&(t_a, key)),
                Some(&(key * 2 + 1)),
                "table a WAL mismatch for key {key}"
            );
            assert_eq!(
                wal_state.get(&(t_b, key)),
                Some(&(key * 3 + 1)),
                "table b WAL mismatch for key {key}"
            );
            assert_eq!(
                wal_state.get(&(t_c, key)),
                Some(&(key * 5 + 1)),
                "table c WAL mismatch for key {key}"
            );
        }
    }

    drop(db);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);
}

#[test]
fn contended_concurrent_transactions_tree_and_wal_agree_despite_conflicts() {
    let path = temp_path("contended");
    let meta_path = format!("{}.meta", path.display());
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&meta_path);

    let db: TestDb = Database::new_with_wal(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
        &path,
        Duration::from_millis(2),
    )
    .unwrap();
    let t = db.create_table("t").table_id().unwrap();

    const KEYS: u64 = 20;
    for k in 0..KEYS {
        assert!(matches!(
            db.dispatch_crud(t, CRUDOperation::Insert(k, 0)),
            CRUDOperationResult::Inserted(_)
        ));
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

                        let mut tx = DbTransaction::begin(db_ref);
                        let update_ts = match tx.update(t, key, payload) {
                            CRUDOperationResult::Updated(ts) => Some(ts),
                            CRUDOperationResult::Conflict => None,
                            other => panic!("unexpected update result: {other}"),
                        };
                        match update_ts {
                            Some(ts) => {
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
    assert!(
        total_committed > 0,
        "expected at least some updates to actually commit under contention"
    );

    db.table(t).unwrap().wait_wal_hardened(max_ts);

    let version = db.current_version();
    let mut tree_state = HashMap::new();
    for k in 0..KEYS {
        match db
            .table(t)
            .unwrap()
            .dispatch_crud(CRUDOperation::Point(k, version))
        {
            CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => {
                tree_state.insert((t, k), *r[0].payload);
            }
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

const REPRO_THREADS: u64 = 6;
const REPRO_KEYS_PER_THREAD: u64 = 300;
const REPRO_ITERATIONS: usize = 20;

fn dump_tree(
    tree: &TestTree,
    version: crate::bat_record_model::version_info::Version,
    target_key: u64,
) {
    use crate::bat_page_model::BlockRef;
    use crate::bat_page_model::node::PageType;
    use crate::bat_page_model::time_matcher::TimeMatcher;
    use std::fmt::Write as _;

    fn walk(node: &BlockRef<8, 8, u64, u64>, depth: usize, target_key: u64, out: &mut String) {
        let indent = "  ".repeat(depth);
        match node.as_page_ref() {
            PageType::IndexRef(internal_page) => {
                let (keys, versions) = internal_page.keys_versions();
                let entries: Vec<_> = keys
                    .iter()
                    .zip(versions.iter())
                    .map(|(range, ver)| (*range, ver.is_active()))
                    .collect();
                let _ = writeln!(
                    out,
                    "[K{target_key}]{indent}[internal] page={:p} sum_len={} active_len={} entries:",
                    internal_page as *const _,
                    internal_page.sum_len(),
                    internal_page.active_len()
                );
                for (pos, (range, active)) in entries.iter().enumerate() {
                    let covers = range.contains(target_key);
                    let _ = writeln!(
                        out,
                        "[K{target_key}]{indent}  #{pos} range=[{},{}] active={} covers_target={}",
                        range.lower, range.upper, active, covers
                    );
                }
                for (pos, (range, active)) in entries.iter().enumerate() {
                    if *active {
                        let child = internal_page.get_pointer(pos);
                        let _ = writeln!(
                            out,
                            "[K{target_key}]{indent}  -> descending into #{pos} range=[{},{}]",
                            range.lower, range.upper
                        );
                        walk(&child, depth + 1, target_key, out);
                    }
                }
            }
            PageType::LeafRef(leaf_page) => {
                let records = leaf_page.as_records();
                let live_keys: Vec<u64> = records
                    .iter()
                    .filter(|r| r.version().is_live())
                    .map(|r| r.key)
                    .collect();
                let has_target = live_keys.contains(&target_key);
                let _ = writeln!(
                    out,
                    "[K{target_key}]{indent}[leaf] len={} live_keys={:?} HAS_TARGET={}",
                    records.len(),
                    live_keys,
                    has_target
                );
                for (pos, r) in records.iter().enumerate() {
                    if r.key == target_key {
                        let _ = writeln!(
                            out,
                            "[K{target_key}]{indent}  #{pos} MATCH key={} is_live={} is_deleted={} insert_invalid={} worker={} ts_start={}",
                            r.key,
                            r.version().is_live(),
                            r.version().is_deleted(),
                            r.version().insertion_stamp().is_invalid(),
                            r.version().insertion_stamp().worker_id(),
                            r.version().insertion_stamp().ts_start()
                        );
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    let mut out = String::new();
    let _ = writeln!(
        out,
        "=== DUMP for target_key={target_key} at version={version} ==="
    );
    let root = tree.retrieve_root_for(version);
    walk(&root, 0, target_key, &mut out);
    let _ = writeln!(out, "=== END DUMP for target_key={target_key} ===");
    eprint!("{out}");
}

fn repro_run_range(tree: &TestTree, t: u64) {
    for i in 0..REPRO_KEYS_PER_THREAD {
        let key = t * REPRO_KEYS_PER_THREAD + i;
        match tree.dispatch_crud(CRUDOperation::Insert(key, key * 3 + 1)) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert {key} failed: {other}"),
        }
        match tree.dispatch_crud(CRUDOperation::Update(key, key * 3 + 2)) {
            CRUDOperationResult::Updated(_) => {}
            other => {
                let v = tree.current_version();
                let point = tree.dispatch_crud(CRUDOperation::Point(key, v));
                let retry = tree.dispatch_crud(CRUDOperation::Update(key, key * 3 + 2));
                dump_tree(tree, v, key);
                if crate::bat_tree::smo::TRACE_KEY_DEBUG {
                    let log = crate::bat_tree::smo::drain_trace_log().join("\n");
                    let _ = std::fs::write(
                        "/tmp/claude-1000/-home-amir-RustroverProjects-BatStore/f3d9fdfb-aec9-4964-9963-71106541c3dd/scratchpad/trace_log_dump.txt",
                        &log,
                    );
                }
                panic!(
                    "update {key} failed: {other}; immediate Point({key}, {v})={point}; immediate retry Update={retry}"
                );
            }
        }
        if key % 2 == 0 {
            match tree.dispatch_crud(CRUDOperation::Delete(key)) {
                CRUDOperationResult::Deleted(_) => {}
                other => {
                    let v = tree.current_version();
                    let point = tree.dispatch_crud(CRUDOperation::Point(key, v));
                    let retry = tree.dispatch_crud(CRUDOperation::Delete(key));
                    panic!(
                        "delete {key} failed: {other}; immediate Point({key}, {v})={point}; immediate retry Delete={retry}"
                    );
                }
            }
        }
    }
}

#[test]
fn repro_sequential_insert_update_delete() {
    for _ in 0..REPRO_ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        for t in 0..REPRO_THREADS {
            repro_run_range(&tree, t);
        }
    }
}

#[test]
fn repro_concurrent_insert_update_delete() {
    for _ in 0..REPRO_ITERATIONS {
        if crate::bat_tree::smo::TRACE_KEY_DEBUG {
            let _ = crate::bat_tree::smo::drain_trace_log();
        }
        let tree = TestTree::make_standard(RootIndexType::default());
        std::thread::scope(|scope| {
            for t in 0..REPRO_THREADS {
                let tree = &tree;
                scope.spawn(move || repro_run_range(tree, t));
            }
        });
    }
}

#[test]
fn repro_concurrent_insert_update_delete_with_gc() {
    for _ in 0..REPRO_ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        tree.enable_gc(false);
        std::thread::scope(|scope| {
            for t in 0..REPRO_THREADS {
                let tree = &tree;
                scope.spawn(move || repro_run_range(tree, t));
            }
        });
    }
}

fn repro_run_shuffled(tree: &TestTree, t: u64) {
    use rand::prelude::SliceRandom;
    let mut keys: Vec<u64> = (0..REPRO_KEYS_PER_THREAD)
        .map(|i| t * REPRO_KEYS_PER_THREAD + i)
        .collect();
    keys.shuffle(&mut rand::rng());
    for key in keys {
        match tree.dispatch_crud(CRUDOperation::Insert(key, key * 3 + 1)) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert {key} failed: {other}"),
        }
        match tree.dispatch_crud(CRUDOperation::Update(key, key * 3 + 2)) {
            CRUDOperationResult::Updated(_) => {}
            other => panic!("update {key} failed: {other}"),
        }
        if key % 2 == 0 {
            match tree.dispatch_crud(CRUDOperation::Delete(key)) {
                CRUDOperationResult::Deleted(_) => {}
                other => panic!("delete {key} failed: {other}"),
            }
        }
    }
}

#[test]
fn repro_concurrent_insert_update_delete_shuffled_keys() {
    for _ in 0..REPRO_ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        std::thread::scope(|scope| {
            for t in 0..REPRO_THREADS {
                let tree = &tree;
                scope.spawn(move || repro_run_shuffled(tree, t));
            }
        });
    }
}

const RACE_KEY_COUNT: u64 = 4000;
const RACE_ITERATIONS: usize = 30;

#[test]
fn concurrent_inserts_into_splitting_leaf_are_not_lost() {
    for iter in 0..RACE_ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        std::thread::scope(|scope| {
            let t1 = &tree;
            scope.spawn(move || {
                for key in (0..RACE_KEY_COUNT).step_by(2) {
                    match t1.dispatch_crud(CRUDOperation::Insert(key, key)) {
                        CRUDOperationResult::Inserted(_) => {}
                        other => panic!("insert {key} failed: {other}"),
                    }
                }
            });
            let t2 = &tree;
            scope.spawn(move || {
                for key in (1..RACE_KEY_COUNT).step_by(2) {
                    match t2.dispatch_crud(CRUDOperation::Insert(key, key)) {
                        CRUDOperationResult::Inserted(_) => {}
                        other => panic!("insert {key} failed: {other}"),
                    }
                }
            });
        });

        let v = tree.current_version();
        let mut missing = Vec::new();
        for key in 0..RACE_KEY_COUNT {
            match tree.dispatch_crud(CRUDOperation::Point(key, v)) {
                CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == key => {}
                other => missing.push((key, format!("{other}"))),
            }
        }
        assert!(
            missing.is_empty(),
            "iteration {iter}: {} of {} keys inserted successfully but not found afterward: {:?}",
            missing.len(),
            RACE_KEY_COUNT,
            &missing[..missing.len().min(20)]
        );
    }
}

fn repro_run_range_serialized(tree: &TestTree, t: u64, lock: &Mutex<()>) {
    for i in 0..REPRO_KEYS_PER_THREAD {
        let key = t * REPRO_KEYS_PER_THREAD + i;
        {
            let _guard = lock.lock().unwrap();
            match tree.dispatch_crud(CRUDOperation::Insert(key, key * 3 + 1)) {
                CRUDOperationResult::Inserted(_) => {}
                other => panic!("insert {key} failed: {other}"),
            }
        }
        {
            let _guard = lock.lock().unwrap();
            match tree.dispatch_crud(CRUDOperation::Update(key, key * 3 + 2)) {
                CRUDOperationResult::Updated(_) => {}
                other => {
                    let v = tree.current_version();
                    let point = tree.dispatch_crud(CRUDOperation::Point(key, v));
                    dump_tree(tree, v, key);
                    panic!("update {key} failed: {other}; immediate Point({key}, {v})={point}");
                }
            }
        }
        if key % 2 == 0 {
            let _guard = lock.lock().unwrap();
            match tree.dispatch_crud(CRUDOperation::Delete(key)) {
                CRUDOperationResult::Deleted(_) => {}
                other => panic!("delete {key} failed: {other}"),
            }
        }
    }
}

#[test]
fn repro_serialized_concurrent_insert_update_delete() {
    for _ in 0..REPRO_ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        let lock = Mutex::new(());
        std::thread::scope(|scope| {
            for t in 0..REPRO_THREADS {
                let tree = &tree;
                let lock = &lock;
                scope.spawn(move || repro_run_range_serialized(tree, t, lock));
            }
        });
    }
}

fn repro_run_point_after_insert(tree: &TestTree, t: u64) {
    for i in 0..REPRO_KEYS_PER_THREAD {
        let key = t * REPRO_KEYS_PER_THREAD + i;
        match tree.dispatch_crud(CRUDOperation::Insert(key, key * 3 + 1)) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert {key} failed: {other}"),
        }

        let v = tree.current_version();
        match tree.dispatch_crud(CRUDOperation::Point(key, v)) {
            CRUDOperationResult::MatchedRecords(r)
                if r.len() == 1 && r[0].payload == key * 3 + 1 => {}
            other => {
                dump_tree(tree, v, key);
                panic!(
                    "Point({key}, {v}) immediately after a successful insert returned {other}, \
                     expected the just-inserted payload {}",
                    key * 3 + 1
                );
            }
        }
    }
}

#[test]
fn repro_concurrent_point_immediately_after_insert() {
    for _ in 0..REPRO_ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        std::thread::scope(|scope| {
            for t in 0..REPRO_THREADS {
                let tree = &tree;
                scope.spawn(move || repro_run_point_after_insert(tree, t));
            }
        });
    }
}

fn repro_run_range_no_delete(tree: &TestTree, t: u64, keys_per_thread: u64) {
    for i in 0..keys_per_thread {
        let key = t * keys_per_thread + i;
        match tree.dispatch_crud(CRUDOperation::Insert(key, key * 3 + 1)) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert {key} failed: {other}"),
        }
        match tree.dispatch_crud(CRUDOperation::Update(key, key * 3 + 2)) {
            CRUDOperationResult::Updated(_) => {}
            other => {
                let v = tree.current_version();
                let point = tree.dispatch_crud(CRUDOperation::Point(key, v));
                dump_tree(tree, v, key);
                panic!("update {key} failed: {other}; immediate Point({key}, {v})={point}");
            }
        }
    }
}

#[test]
fn repro_two_threads_insert_update_no_delete() {
    for _ in 0..REPRO_ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        std::thread::scope(|scope| {
            for t in 0..2 {
                let tree = &tree;
                scope.spawn(move || repro_run_range_no_delete(tree, t, REPRO_KEYS_PER_THREAD));
            }
        });
    }
}

fn repro_run_range_collect(tree: &TestTree, t: u64, failures: &Mutex<Vec<(u64, String)>>) {
    for i in 0..REPRO_KEYS_PER_THREAD {
        let key = t * REPRO_KEYS_PER_THREAD + i;
        match tree.dispatch_crud(CRUDOperation::Insert(key, key * 3 + 1)) {
            CRUDOperationResult::Inserted(_) => {}
            other => {
                failures
                    .lock()
                    .unwrap()
                    .push((key, format!("insert: {other}")));
                continue;
            }
        }
        match tree.dispatch_crud(CRUDOperation::Update(key, key * 3 + 2)) {
            CRUDOperationResult::Updated(_) => {}
            other => failures
                .lock()
                .unwrap()
                .push((key, format!("update: {other}"))),
        }
        if key % 2 == 0 {
            match tree.dispatch_crud(CRUDOperation::Delete(key)) {
                CRUDOperationResult::Deleted(_) => {}
                other => failures
                    .lock()
                    .unwrap()
                    .push((key, format!("delete: {other}"))),
            }
        }
    }
}

#[test]
fn repro_concurrent_insert_update_delete_reports_all_failures() {
    let mut per_iteration = Vec::new();
    for iter in 0..REPRO_ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        let failures: Mutex<Vec<(u64, String)>> = Mutex::new(Vec::new());
        std::thread::scope(|scope| {
            for t in 0..REPRO_THREADS {
                let tree = &tree;
                let failures = &failures;
                scope.spawn(move || repro_run_range_collect(tree, t, failures));
            }
        });
        let failures = failures.into_inner().unwrap();
        if !failures.is_empty() {
            per_iteration.push((
                iter,
                failures.len(),
                failures.into_iter().take(5).collect::<Vec<_>>(),
            ));
        }
    }
    assert!(
        per_iteration.is_empty(),
        "{} of {} iterations had lost writes (iteration, count, first few): {:?}",
        per_iteration.len(),
        REPRO_ITERATIONS,
        per_iteration
    );
}

/// Regression test for the specific mechanism confirmed via `dump_tree`
/// during this investigation: `traversal_write_internal_olc`'s ordinary
/// "which child covers this key" index lookup reads a node's
/// `key_interval_region`/`version_region`/`pointer_region` via a bare,
/// never-upgraded `Reader` whenever that node itself isn't the one being
/// corrected — nothing excludes a *different* thread that's concurrently
/// write-locked the exact same node as `mufasa` for one of its *other*
/// children. More threads and a smaller fan-out-relative keyspace than the
/// standard repro pushes far more overflow/underflow corrections through
/// the same handful of internal pages per run, maximizing the chance some
/// other thread's ordinary traversal is mid-index-lookup on that same page
/// at the same time — exactly the window `is_reader`/`is_write_locked`/
/// `live_version` (`src/bat_query/olc_query.rs`) now brackets.
const HIGH_CONTENTION_THREADS: u64 = 16;
const HIGH_CONTENTION_KEYS_PER_THREAD: u64 = 120;
const HIGH_CONTENTION_ITERATIONS: usize = 10;

#[test]
fn repro_high_thread_count_insert_update_delete_reports_all_failures() {
    let mut per_iteration = Vec::new();
    for iter in 0..HIGH_CONTENTION_ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        let failures: Mutex<Vec<(u64, String)>> = Mutex::new(Vec::new());
        std::thread::scope(|scope| {
            for t in 0..HIGH_CONTENTION_THREADS {
                let tree = &tree;
                let failures = &failures;
                scope.spawn(move || {
                    for i in 0..HIGH_CONTENTION_KEYS_PER_THREAD {
                        let key = t * HIGH_CONTENTION_KEYS_PER_THREAD + i;
                        match tree.dispatch_crud(CRUDOperation::Insert(key, key * 3 + 1)) {
                            CRUDOperationResult::Inserted(_) => {}
                            other => {
                                failures
                                    .lock()
                                    .unwrap()
                                    .push((key, format!("insert: {other}")));
                                continue;
                            }
                        }
                        match tree.dispatch_crud(CRUDOperation::Update(key, key * 3 + 2)) {
                            CRUDOperationResult::Updated(_) => {}
                            other => {
                                let v = tree.current_version();
                                let point = tree.dispatch_crud(CRUDOperation::Point(key, v));
                                dump_tree(tree, v, key);
                                failures.lock().unwrap().push((
                                    key,
                                    format!("update: {other}; immediate Point({key}, {v})={point}"),
                                ));
                            }
                        }
                        if key % 2 == 0 {
                            match tree.dispatch_crud(CRUDOperation::Delete(key)) {
                                CRUDOperationResult::Deleted(_) => {}
                                other => {
                                    let v = tree.current_version();
                                    let point = tree.dispatch_crud(CRUDOperation::Point(key, v));
                                    dump_tree(tree, v, key);
                                    failures.lock().unwrap().push((
                                        key,
                                        format!(
                                            "delete: {other}; immediate Point({key}, {v})={point}"
                                        ),
                                    ));
                                }
                            }
                        }
                    }
                });
            }
        });
        let failures = failures.into_inner().unwrap();
        if !failures.is_empty() {
            per_iteration.push((
                iter,
                failures.len(),
                failures.into_iter().take(5).collect::<Vec<_>>(),
            ));
        }
    }
    assert!(
        per_iteration.is_empty(),
        "{} of {} high-contention iterations had lost writes (iteration, count, first few): {:?}",
        per_iteration.len(),
        HIGH_CONTENTION_ITERATIONS,
        per_iteration
    );
}
