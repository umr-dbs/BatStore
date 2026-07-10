use std::collections::HashMap;
use std::fs;
use std::time::Duration;
use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_record_model::version_info::Version;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_tree::mvbt::MVBTSt;

const FAN: usize = 8;
type TestTree = MVBTSt<FAN, FAN, u64, u64>;

fn temp_log_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("cmvbt_wal_{name}_{}.log", std::process::id()))
}

/// Per-worker WAL sharding (see `mv_tree::mvbt::wal_shard_path`) means the
/// base `path` itself is never a real file — only `path.0000`, `path.0001`,
/// ... are. Removes a generous range of shard indices so tests don't leave
/// stray files behind regardless of how many workers they actually used.
fn remove_shards(path: &std::path::Path) {
    for worker_id in 0..32 {
        let _ = fs::remove_file(crate::mv_tree::mvbt::wal_shard_path(path, worker_id));
    }
}

fn point(tree: &TestTree, key: u64, version: Version) -> Option<u64> {
    match tree.dispatch_crud(CRUDOperation::Point(key, version)) {
        CRUDOperationResult::MatchedRecords(records) if !records.is_empty() => Some(records[0].payload),
        _ => None,
    }
}

/// Exercises inserts, updates, and deletes (with small FAN_OUT so splits
/// actually fire) through the live WAL-enabled write path, drops the tree,
/// then recovers it via `open_recovered` and diffs the final state against
/// an in-memory oracle. This is the key correctness test for the whole
/// design: it validates that structural repairs (never logged) are
/// faithfully re-derived by replaying only the logged CRUD ops in causal
/// order, reconstructing a logically equivalent tree.
///
/// Deliberately *not* checked: historical snapshots at specific version
/// numbers from before the crash. Recovery mints fresh version numbers for
/// everything it replays rather than preserving the originals (see
/// `mv_wal::recovery`'s doc comment for why) — only the *final* logical
/// state is a guaranteed match, not "the same version number means the same
/// thing it did before the crash."
#[test]
fn crash_recovery_round_trip() {
    let path = temp_log_path("round_trip");
    remove_shards(&path);

    let tree = TestTree::make_standard(RootIndexType::default());
    tree.enable_wal(&path, Duration::from_millis(2)).unwrap();

    let mut oracle: HashMap<u64, u64> = HashMap::new();

    for i in 0..600u64 {
        let key = i % 64;

        match i % 5 {
            0 | 1 if !oracle.contains_key(&key) => {
                let payload = i * 7 + 1;
                if let CRUDOperationResult::Inserted(_) = tree.dispatch_crud(CRUDOperation::Insert(key, payload)) {
                    oracle.insert(key, payload);
                }
            }
            2 if oracle.contains_key(&key) => {
                let payload = i * 13 + 2;
                if let CRUDOperationResult::Updated(_) = tree.dispatch_crud(CRUDOperation::Update(key, payload)) {
                    oracle.insert(key, payload);
                }
            }
            3 if oracle.contains_key(&key) => {
                if let CRUDOperationResult::Deleted(_) = tree.dispatch_crud(CRUDOperation::Delete(key)) {
                    oracle.remove(&key);
                }
            }
            _ => {
                let payload = i * 3 + 5;
                if let CRUDOperationResult::Inserted(_) = tree.dispatch_crud(CRUDOperation::Insert(key, payload)) {
                    oracle.insert(key, payload);
                }
            }
        }
    }

    let final_oracle = oracle.clone();

    // Sanity check against the *live* tree first, to isolate a test/oracle
    // bug from an actual replay bug.
    let live_version = tree.current_version();
    for key in 0..64u64 {
        let expected = final_oracle.get(&key).copied();
        assert_eq!(point(&tree, key, live_version), expected, "LIVE mismatch for key {key}");
    }

    drop(tree);

    let recovered = TestTree::open_recovered(RootIndexType::default(), &path, Duration::from_millis(2)).unwrap();
    let recovered_version = recovered.current_version();

    for key in 0..64u64 {
        let expected = final_oracle.get(&key).copied();
        assert_eq!(point(&recovered, key, recovered_version), expected, "mismatch for key {key} after recovery");
    }

    drop(recovered);
    remove_shards(&path);
}

/// Drives the WAL from many concurrent threads at once — the scenario the
/// lock-free rewrite exists for. Minting a version and appending its record
/// are two independent, uncoordinated steps per thread, so records can (and,
/// with enough threads/keys, reliably do) land in the log file in a
/// different order than their version numbers — this specifically exercises
/// replay's "sort by version before applying" handling of that, not just the
/// single-threaded, naturally-in-order case `crash_recovery_round_trip`
/// covers. It also now exercises per-worker WAL sharding for real: each of
/// the `THREADS` writer threads gets its own `WorkerId` and thus its own
/// shard file.
#[test]
fn concurrent_writers_crash_recovery_round_trip() {
    let path = temp_log_path("concurrent_round_trip");
    remove_shards(&path);

    let tree = TestTree::make_standard(RootIndexType::default());
    tree.enable_wal(&path, Duration::from_millis(2)).unwrap();

    const THREADS: u64 = 8;
    const KEYS_PER_THREAD: u64 = 200;

    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let tree = &tree;
            scope.spawn(move || {
                for i in 0..KEYS_PER_THREAD {
                    let key = t * KEYS_PER_THREAD + i;
                    let result = tree.dispatch_crud(CRUDOperation::Insert(key, key * 10 + 1));
                    assert!(matches!(result, CRUDOperationResult::Inserted(_)));
                }
            });
        }
    });

    drop(tree);

    let recovered = TestTree::open_recovered(RootIndexType::default(), &path, Duration::from_millis(2)).unwrap();

    // Query the *recovered* tree's own clock, not a version captured from
    // the dropped original tree: they're independent `GlobalClock`s, and
    // structural repairs (splits) consume a non-deterministic number of
    // extra ticks on each side (concurrent execution vs. sequential
    // replay), so there's no guaranteed numeric correspondence between the
    // two — recovery deliberately mints fresh version numbers for
    // everything it replays (see this module's top-level doc comment).
    let recovered_version = recovered.current_version();

    for key in 0..THREADS * KEYS_PER_THREAD {
        assert_eq!(
            point(&recovered, key, recovered_version),
            Some(key * 10 + 1),
            "key {key} missing or wrong after concurrent-write recovery"
        );
    }

    drop(recovered);
    remove_shards(&path);
}

/// Simulates a crash mid-fsync by truncating a few bytes off the tail of an
/// otherwise-valid log, and confirms recovery stops cleanly at the last
/// valid record (no panic, no corrupted data) rather than either erroring
/// out or misinterpreting the torn tail as valid.
#[test]
fn torn_write_stops_cleanly() {
    let path = temp_log_path("torn");
    let _ = fs::remove_file(&path);
    // A single test thread means a single WorkerId (0), so all writes land
    // in worker 0's shard — the only shard file that actually has content.
    let shard_path = crate::mv_tree::mvbt::wal_shard_path(&path, 0);
    let _ = fs::remove_file(&shard_path);

    {
        let tree = TestTree::make_standard(RootIndexType::default());
        tree.enable_wal(&path, Duration::from_millis(2)).unwrap();
        for k in 0..20u64 {
            tree.dispatch_crud(CRUDOperation::Insert(k, k * 10));
        }
    } // tree drops here, flushing everything cleanly first.

    let full_len = fs::metadata(&shard_path).unwrap().len();
    assert!(full_len > 0);

    let torn_len = full_len - 3;
    let file = fs::OpenOptions::new().write(true).open(&shard_path).unwrap();
    file.set_len(torn_len).unwrap();
    drop(file);

    let recovered = TestTree::open_recovered(RootIndexType::default(), &path, Duration::from_millis(2)).unwrap();

    let survived = (0..20u64)
        .filter(|&k| point(&recovered, k, recovered.current_version()) == Some(k * 10))
        .count();
    assert!(survived >= 19, "expected at least 19/20 keys to survive a 3-byte tail truncation, got {survived}");

    let truncated_file_len = fs::metadata(&shard_path).unwrap().len();
    assert!(truncated_file_len < full_len, "recovery must truncate away the torn tail");

    drop(recovered);
    let _ = fs::remove_file(&shard_path);
}

/// Confirms the WAL-disabled path (never calling `enable_wal`) behaves
/// exactly as before the WAL work: plain inserts/updates/deletes, including
/// the update-in-place fast path (GC + update-in-place enabled, no
/// registered readers), which is only skipped when a WAL is attached (see
/// `dispatch.rs`'s `update_in_place_disabled_while_wal_attached` test).
#[test]
fn wal_disabled_path_unaffected() {
    let tree = TestTree::make_standard(RootIndexType::default());
    tree.enable_gc(true);

    assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(1, 100)), CRUDOperationResult::Inserted(_)));
    assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(1, 200)), CRUDOperationResult::ZeroAffected(_)));

    assert!(matches!(tree.dispatch_crud(CRUDOperation::Update(1, 300)), CRUDOperationResult::Updated(_)));
    assert_eq!(point(&tree, 1, tree.current_version()), Some(300));

    assert!(matches!(tree.dispatch_crud(CRUDOperation::Delete(1)), CRUDOperationResult::Deleted(_)));
    assert_eq!(point(&tree, 1, tree.current_version()), None);

    assert!(matches!(tree.dispatch_crud(CRUDOperation::Delete(1)), CRUDOperationResult::ZeroAffected(_)));
}
