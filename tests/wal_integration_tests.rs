use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_record_model::tx_stamp::TxStamp;
use crate::bat_record_model::version_info::Version;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_tree::mvbt::MVBTSt;
use crate::bat_wal::record::{self, WalEntry, WalRecord};
use std::collections::HashMap;
use std::fs;
use std::time::Duration;

const FAN: usize = 8;
type TestTree = MVBTSt<FAN, FAN, u64, u64>;

fn temp_log_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("batstore_wal_{name}_{}.log", std::process::id()))
}

fn point(tree: &TestTree, key: u64, version: Version) -> Option<u64> {
    match tree.dispatch_crud(CRUDOperation::Point(key, version)) {
        CRUDOperationResult::MatchedRecords(records) if !records.is_empty() => {
            Some(*records[0].payload)
        }
        _ => None,
    }
}

#[test]
fn crash_recovery_round_trip() {
    let path = temp_log_path("round_trip");
    let _ = fs::remove_file(&path);

    let tree = TestTree::make_standard(RootIndexType::default())
        .with_wal(&path, Duration::from_millis(2))
        .unwrap();

    let mut oracle: HashMap<u64, u64> = HashMap::new();

    for i in 0..600u64 {
        let key = i % 64;

        match i % 5 {
            0 | 1 if !oracle.contains_key(&key) => {
                let payload = i * 7 + 1;
                if let CRUDOperationResult::Inserted(_) =
                    tree.dispatch_crud(CRUDOperation::Insert(key, payload))
                {
                    oracle.insert(key, payload);
                }
            }
            2 if oracle.contains_key(&key) => {
                let payload = i * 13 + 2;
                if let CRUDOperationResult::Updated(_) =
                    tree.dispatch_crud(CRUDOperation::Update(key, payload))
                {
                    oracle.insert(key, payload);
                }
            }
            3 if oracle.contains_key(&key) => {
                if let CRUDOperationResult::Deleted(_) =
                    tree.dispatch_crud(CRUDOperation::Delete(key))
                {
                    oracle.remove(&key);
                }
            }
            _ => {
                let payload = i * 3 + 5;
                if let CRUDOperationResult::Inserted(_) =
                    tree.dispatch_crud(CRUDOperation::Insert(key, payload))
                {
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
        assert_eq!(
            point(&tree, key, live_version),
            expected,
            "LIVE mismatch for key {key}"
        );
    }

    drop(tree);

    let recovered =
        TestTree::open_recovered(RootIndexType::default(), &path, Duration::from_millis(2))
            .unwrap();
    let recovered_version = recovered.current_version();

    for key in 0..64u64 {
        let expected = final_oracle.get(&key).copied();
        assert_eq!(
            point(&recovered, key, recovered_version),
            expected,
            "mismatch for key {key} after recovery"
        );
    }

    drop(recovered);
    let _ = fs::remove_file(&path);
}

#[test]
fn concurrent_writers_crash_recovery_round_trip() {
    let path = temp_log_path("concurrent_round_trip");
    let _ = fs::remove_file(&path);

    let tree = TestTree::make_standard(RootIndexType::default())
        .with_wal(&path, Duration::from_millis(2))
        .unwrap();

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

    let recovered =
        TestTree::open_recovered(RootIndexType::default(), &path, Duration::from_millis(2))
            .unwrap();

    let recovered_version = recovered.current_version();

    for key in 0..THREADS * KEYS_PER_THREAD {
        assert_eq!(
            point(&recovered, key, recovered_version),
            Some(key * 10 + 1),
            "key {key} missing or wrong after concurrent-write recovery"
        );
    }

    drop(recovered);
    let _ = fs::remove_file(&path);
}

#[test]
fn torn_write_stops_cleanly() {
    let path = temp_log_path("torn");
    let _ = fs::remove_file(&path);

    {
        let tree = TestTree::make_standard(RootIndexType::default())
            .with_wal(&path, Duration::from_millis(2))
            .unwrap();
        for k in 0..20u64 {
            tree.dispatch_crud(CRUDOperation::Insert(k, k * 10));
        }
    } // tree drops here, flushing everything cleanly first.

    let full_len = fs::metadata(&path).unwrap().len();
    assert!(full_len > 0);

    let torn_len = full_len - 3;
    let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(torn_len).unwrap();
    drop(file);

    let recovered =
        TestTree::open_recovered(RootIndexType::default(), &path, Duration::from_millis(2))
            .unwrap();

    let survived = (0..20u64)
        .filter(|&k| point(&recovered, k, recovered.current_version()) == Some(k * 10))
        .count();
    assert!(
        survived >= 19,
        "expected at least 19/20 keys to survive a 3-byte tail truncation, got {survived}"
    );

    let truncated_file_len = fs::metadata(&path).unwrap().len();
    assert!(
        truncated_file_len < full_len,
        "recovery must truncate away the torn tail"
    );

    drop(recovered);
    let _ = fs::remove_file(&path);
}

#[test]
fn wait_wal_hardened_reflects_real_on_disk_durability() {
    let path = temp_log_path("async_hardened");
    let _ = fs::remove_file(&path);

    let tree = TestTree::make_standard(RootIndexType::default())
        .with_wal(&path, Duration::from_millis(2))
        .unwrap();

    let mut last_ts = 0;
    for k in 0..500u64 {
        let CRUDOperationResult::Inserted(ts_start) =
            tree.dispatch_crud(CRUDOperation::Insert(k, k * 10))
        else {
            panic!("expected Inserted for key {k}");
        };
        last_ts = ts_start;
    }

    tree.wait_wal_hardened(last_ts);
    assert!(
        tree.wal_hardened_version() >= last_ts,
        "wait_wal_hardened must not return early"
    );

    // Read the file directly instead of going through the tree, to check
    // durability independently of the in-memory structure
    // `wait_wal_hardened` itself doesn't touch.
    let bytes = fs::read(&path).unwrap();
    let mut offset = 0;
    let mut count = 0;
    while let Some((_, consumed)) = crate::bat_wal::record::read_frame(&bytes[offset..]) {
        count += 1;
        offset += consumed;
    }
    // Each insert logs two entries — its Write and, once committed, its
    // Commit marker (see `WalEntry`'s doc) — so 500 inserts is 1000 frames.
    assert_eq!(
        count, 1000,
        "every insert's write and commit marker must be on disk once wait_wal_hardened returns"
    );

    drop(tree);
    let _ = fs::remove_file(&path);
}

/// `wal_hardened_version` reports `0` ("nothing guaranteed durable") for a
/// tree with no WAL attached at all, rather than some value that could be
/// mistaken for a real watermark.
#[test]
fn wal_hardened_version_zero_when_wal_disabled() {
    let tree = TestTree::make_standard(RootIndexType::default());
    assert_eq!(tree.wal_hardened_version(), 0);

    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Insert(1, 100)),
        CRUDOperationResult::Inserted(_)
    ));
    assert_eq!(tree.wal_hardened_version(), 0);
}

#[test]
fn wal_disabled_path_unaffected() {
    let tree = TestTree::make_standard(RootIndexType::default());
    tree.enable_gc(true);

    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Insert(1, 100)),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Insert(1, 200)),
        CRUDOperationResult::ZeroAffected(_)
    ));

    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Update(1, 300)),
        CRUDOperationResult::Updated(_)
    ));
    assert_eq!(point(&tree, 1, tree.current_version()), Some(300));

    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Delete(1)),
        CRUDOperationResult::Deleted(_)
    ));
    assert_eq!(point(&tree, 1, tree.current_version()), None);

    assert!(matches!(
        tree.dispatch_crud(CRUDOperation::Delete(1)),
        CRUDOperationResult::ZeroAffected(_)
    ));
}

#[test]
fn logged_but_never_committed_write_does_not_resurface_after_recovery() {
    let path = temp_log_path("never_committed");
    let _ = fs::remove_file(&path);

    {
        let tree = TestTree::make_standard(RootIndexType::default())
            .with_wal(&path, Duration::from_millis(2))
            .unwrap();

        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Insert(1, 100)),
            CRUDOperationResult::Inserted(_)
        ));

        // Both fail after their op is already logged — neither ever reaches
        // `commit_tx`, so neither ever gets a Commit marker.
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Update(2, 999)),
            CRUDOperationResult::ZeroAffected(_)
        ));
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Delete(2)),
            CRUDOperationResult::ZeroAffected(_)
        ));
    } // tree drops here, flushing everything cleanly first.

    let recovered =
        TestTree::open_recovered(RootIndexType::default(), &path, Duration::from_millis(2))
            .unwrap();
    let version = recovered.current_version();

    assert_eq!(
        point(&recovered, 1, version),
        Some(100),
        "the real, committed insert must survive"
    );
    assert_eq!(
        point(&recovered, 2, version),
        None,
        "the never-committed Update/Delete attempts must not resurface key 2"
    );

    drop(recovered);
    let _ = fs::remove_file(&path);
}

#[test]
fn replay_recovers_records_after_an_interior_hole() {
    let path = temp_log_path("interior_hole");
    let _ = fs::remove_file(&path);

    let mut bytes = Vec::new();

    let stamp1 = TxStamp::new(0, 1);
    record::encode_entry_framed::<u64, u64>(
        &WalEntry::Write(WalRecord {
            stamp: stamp1,
            op: CRUDOperation::Insert(1u64, 111u64),
        }),
        &mut bytes,
    );
    record::encode_entry_framed::<u64, u64>(
        &WalEntry::Commit {
            stamp: stamp1,
            ts_commit: 2,
        },
        &mut bytes,
    );

    // Deliberately not a multiple of 4/8/this frame's own header size - see
    // `resync_next_skips_an_interior_hole_of_unaligned_length` for why that
    // matters (a naive fixed-stride skip would misalign past it).
    bytes.extend(std::iter::repeat(0u8).take(17));

    let stamp2 = TxStamp::new(0, 3);
    record::encode_entry_framed::<u64, u64>(
        &WalEntry::Write(WalRecord {
            stamp: stamp2,
            op: CRUDOperation::Insert(2u64, 222u64),
        }),
        &mut bytes,
    );
    record::encode_entry_framed::<u64, u64>(
        &WalEntry::Commit {
            stamp: stamp2,
            ts_commit: 4,
        },
        &mut bytes,
    );

    fs::write(&path, &bytes).unwrap();

    let tree = TestTree::make_standard(RootIndexType::default())
        .with_wal_lockfree(&path, Duration::from_millis(500), 3)
        .unwrap();
    let valid_len = crate::bat_wal::recovery::replay(&tree, &path).unwrap();
    assert_eq!(
        valid_len,
        bytes.len() as u64,
        "the whole file, hole included, is the valid prefix here - nothing trailing to truncate"
    );

    let version = tree.current_version();
    assert_eq!(
        point(&tree, 1, version),
        Some(111),
        "the record before the hole must survive"
    );
    assert_eq!(
        point(&tree, 2, version),
        Some(222),
        "the record after the hole must ALSO survive"
    );

    let _ = fs::remove_file(&path);
}

#[test]
fn concurrent_writers_lockfree_batched_crash_recovery_round_trip() {
    let path = temp_log_path("lockfree_batched_round_trip");
    let _ = fs::remove_file(&path);

    let tree = TestTree::make_standard(RootIndexType::default())
        .with_wal_lockfree(&path, Duration::from_millis(500), 3)
        .unwrap();

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

    let recovered = TestTree::open_recovered_lockfree(
        RootIndexType::default(),
        &path,
        Duration::from_millis(2),
        3,
    )
    .unwrap();
    let recovered_version = recovered.current_version();

    for key in 0..THREADS * KEYS_PER_THREAD {
        assert_eq!(
            point(&recovered, key, recovered_version),
            Some(key * 10 + 1),
            "key {key} missing or wrong after concurrent lock-free-batched-write recovery"
        );
    }

    drop(recovered);
    let _ = fs::remove_file(&path);
}
