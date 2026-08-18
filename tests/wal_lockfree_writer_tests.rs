use std::collections::HashSet;
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_sync::clock::GlobalClock;
use crate::bat_wal::lockfree_writer::{LocalBatch, LockFreeWalWriter};
use crate::bat_wal::record::{self, WalEntry};

#[test]
fn group_fsync_flushes_and_wait_unblocks() {
    let path = std::env::temp_dir().join(format!(
        "batstore_wal_lockfree_test_{}.log",
        std::process::id()
    ));
    let _ = fs::remove_file(&path);

    let writer: LockFreeWalWriter<u64, u64> =
        LockFreeWalWriter::open(&path, Duration::from_millis(5)).unwrap();
    let clock = GlobalClock::new();

    let s1 = writer.start_commit_logged(&clock, 0, |_v| CRUDOperation::Insert(1, 100));
    let s2 = writer.start_commit_logged(&clock, 0, |_v| CRUDOperation::Delete(2));
    assert!(s2.ts_start() > s1.ts_start());

    writer.wait_flushed(s1.ts_start());
    writer.wait_flushed(s2.ts_start());

    drop(writer);

    let bytes = fs::read(&path).unwrap();
    assert!(!bytes.is_empty());

    let mut offset = 0;
    let mut seen = Vec::new();
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        let record: record::WalRecord<u64, u64> = record::decode(body).unwrap();
        seen.push(record);
        offset += consumed;
    }
    assert_eq!(offset, bytes.len());
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].stamp.ts_start(), s1.ts_start());
    assert_eq!(seen[1].stamp.ts_start(), s2.ts_start());

    let _ = fs::remove_file(&path);
}

/// The whole point of this writer: many threads calling `log_with_stamp`
/// concurrently, each doing its own `pwrite` with no lock and no channel
/// hand-off. Every record must land intact, exactly once, at a distinct
/// offset — this is what would break first if `tail.fetch_add`'s disjoint
/// ranges ever overlapped or a partial `pwrite` were left unretried.
#[test]
fn concurrent_writers_each_land_intact_and_distinct() {
    let path = std::env::temp_dir().join(format!(
        "batstore_wal_lockfree_concurrent_{}.log",
        std::process::id()
    ));
    let _ = fs::remove_file(&path);

    let writer: Arc<LockFreeWalWriter<u64, u64>> =
        Arc::new(LockFreeWalWriter::open(&path, Duration::from_micros(200)).unwrap());
    let clock = Arc::new(GlobalClock::new());

    const THREADS: usize = 16;
    const PER_THREAD: usize = 500;

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let writer = writer.clone();
            let clock = clock.clone();
            thread::spawn(move || {
                let mut stamps = Vec::with_capacity(PER_THREAD);
                for i in 0..PER_THREAD {
                    let key = (t * PER_THREAD + i) as u64;
                    let stamp = writer.start_commit_logged(&clock, t as u16, move |_v| {
                        CRUDOperation::Insert(key, key * 7)
                    });
                    stamps.push((stamp.ts_start(), key));
                }
                stamps
            })
        })
        .collect();

    let mut expected: HashSet<(u64, u64)> = HashSet::new();
    for h in handles {
        for (ts_start, key) in h.join().unwrap() {
            assert!(
                expected.insert((ts_start, key)),
                "duplicate ts_start minted: {ts_start}"
            );
        }
    }

    // Every `start_commit_logged` call above already returned (the thread
    // that made it was `join`ed), and `enqueue` doesn't return until its
    // own `pwrite` has: every record's bytes are already in the page cache
    // and visible to a plain `read()` below, fsync/quiescence notwithstanding.
    drop(writer);

    let bytes = fs::read(&path).unwrap();
    let mut offset = 0;
    let mut seen: HashSet<(u64, u64)> = HashSet::new();
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        let record: record::WalRecord<u64, u64> =
            record::decode(body).expect("every completed record must decode cleanly");
        match record.op {
            CRUDOperation::Insert(key, payload) => {
                assert_eq!(payload, key * 7);
                assert!(
                    seen.insert((record.stamp.ts_start(), key)),
                    "record seen twice in file"
                );
            }
            other => panic!("unexpected op in WAL: {other}"),
        }
        offset += consumed;
    }
    assert_eq!(
        offset,
        bytes.len(),
        "no torn/unparseable trailing bytes expected once all writers joined"
    );
    assert_eq!(
        seen, expected,
        "every minted write must appear exactly once, verbatim, in the file"
    );

    let _ = fs::remove_file(&path);
}

/// `LocalBatch`: several threads each grouping their own records into
/// fixed-size local batches (flushed with one `pwrite` each via
/// `flush_batch`) concurrently against the same writer/file. Every batch's
/// bytes must land intact and at a distinct offset, same as the unbatched
/// concurrent test above — `enqueue_bytes` doesn't care whether it's
/// writing one record or several concatenated ones.
#[test]
fn concurrent_local_batches_each_land_intact_and_distinct() {
    let path = std::env::temp_dir().join(format!(
        "batstore_wal_lockfree_batch_{}.log",
        std::process::id()
    ));
    let _ = fs::remove_file(&path);

    let writer: Arc<LockFreeWalWriter<u64, u64>> =
        Arc::new(LockFreeWalWriter::open(&path, Duration::from_micros(200)).unwrap());
    let clock = Arc::new(GlobalClock::new());

    const THREADS: usize = 8;
    const PER_THREAD: usize = 777; // deliberately not a multiple of BATCH_SIZE
    const BATCH_SIZE: usize = 16;

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let writer = writer.clone();
            let clock = clock.clone();
            thread::spawn(move || {
                let mut batch: LocalBatch<u64, u64> = LocalBatch::new();
                let mut stamps = Vec::with_capacity(PER_THREAD);
                for i in 0..PER_THREAD {
                    let key = (t * PER_THREAD + i) as u64;
                    let stamp = crate::bat_record_model::tx_stamp::TxStamp::new(
                        t as u16,
                        clock.next_timestamp(),
                    );
                    batch.push_write(stamp, move |_v| CRUDOperation::Insert(key, key * 3));
                    stamps.push((stamp.ts_start(), key));
                    if batch.len() >= BATCH_SIZE {
                        writer.flush_batch(&mut batch);
                    }
                }
                writer.flush_batch(&mut batch); // flush the remainder
                assert!(batch.is_empty());
                stamps
            })
        })
        .collect();

    let mut expected: HashSet<(u64, u64)> = HashSet::new();
    for h in handles {
        for (ts_start, key) in h.join().unwrap() {
            assert!(
                expected.insert((ts_start, key)),
                "duplicate ts_start minted: {ts_start}"
            );
        }
    }

    drop(writer);

    let bytes = fs::read(&path).unwrap();
    let mut offset = 0;
    let mut seen: HashSet<(u64, u64)> = HashSet::new();
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        let record: record::WalRecord<u64, u64> =
            record::decode(body).expect("every completed record must decode cleanly");
        match record.op {
            CRUDOperation::Insert(key, payload) => {
                assert_eq!(payload, key * 3);
                assert!(
                    seen.insert((record.stamp.ts_start(), key)),
                    "record seen twice in file"
                );
            }
            other => panic!("unexpected op in WAL: {other}"),
        }
        offset += consumed;
    }
    assert_eq!(
        offset,
        bytes.len(),
        "no torn/unparseable trailing bytes expected once all writers joined"
    );
    assert_eq!(
        seen, expected,
        "every batched write must appear exactly once, verbatim, in the file"
    );

    let _ = fs::remove_file(&path);
}

#[test]
fn hardened_version_starts_unset_and_only_advances_on_commit() {
    let path = std::env::temp_dir().join(format!(
        "batstore_wal_lockfree_hardened_{}.log",
        std::process::id()
    ));
    let _ = fs::remove_file(&path);

    let writer: LockFreeWalWriter<u64, u64> =
        LockFreeWalWriter::open(&path, Duration::from_millis(2)).unwrap();
    let clock = GlobalClock::new();

    assert_eq!(writer.hardened_version(), u64::MAX);

    let stamp = writer.start_commit_logged(&clock, 0, |_v| CRUDOperation::Insert(1, 100));
    // A bare Write (no Commit yet) must not advance hardened past "pending".
    thread::sleep(Duration::from_millis(20));
    assert!(
        writer.hardened_version() == 0,
        "a Write with no Commit must not be credited as hardened"
    );

    let ts_commit = clock.next_timestamp();
    writer.log_commit(stamp, ts_commit);
    // Not `writer.wait_flushed(stamp.ts_start())`: the Write logged above
    // shares this same `ts_start`, so `flushed_any` (see that field's doc)
    // was already satisfied by the Write alone, before the Commit was even
    // issued — waiting on it here would return immediately rather than
    // actually waiting for the Commit's own fsync cycle. Poll
    // `hardened_version` (Commit-gated) directly instead, which is the
    // thing this test actually wants to observe.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while writer.hardened_version() != stamp.ts_start() {
        assert!(
            std::time::Instant::now() < deadline,
            "hardened_version never advanced to the Commit's ts_start"
        );
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(writer.hardened_version(), stamp.ts_start());

    drop(writer);
    let _ = fs::remove_file(&path);
}
