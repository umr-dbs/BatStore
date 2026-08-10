use std::fs;
use std::time::Duration;

use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_sync::clock::GlobalClock;
use crate::mv_wal::record::{self, WalEntry, WalRecord};
use crate::mv_wal::writer::WalWriter;

#[test]
fn group_commit_flushes_and_wait_unblocks() {
    let path = std::env::temp_dir().join(format!("cmvbt_wal_test_{}.log", std::process::id()));
    let _ = fs::remove_file(&path);

    let writer: WalWriter<u64, u64> =
        WalWriter::open(&path, Duration::from_millis(5)).unwrap();
    let clock = GlobalClock::new();

    let (s1, t1)
        = writer.start_commit_logged_with_ticket(&clock, 0, |_v| CRUDOperation::Insert(1, 100));

    let (s2, t2)
        = writer.start_commit_logged_with_ticket(&clock, 0, |_v| CRUDOperation::Delete(2));
    assert!(s2.ts_start() > s1.ts_start());

    writer.wait_flushed(t1);
    writer.wait_flushed(t2);

    drop(writer);

    let bytes = fs::read(&path).unwrap();
    assert!(!bytes.is_empty());

    let mut offset = 0;
    let mut seen = Vec::new();
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        let record: WalRecord<u64, u64> = record::decode(body).unwrap();
        seen.push(record);
        offset += consumed;
    }
    assert_eq!(offset, bytes.len());
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].stamp.ts_start(), s1.ts_start());
    assert_eq!(seen[1].stamp.ts_start(), s2.ts_start());

    let _ = fs::remove_file(&path);
}

/// The `Database`-shared-writer path: two records logged under two
/// different table ids via `start_commit_logged_for_table` must still
/// group-commit together (one file, `flush_loop` unchanged) and each
/// decoded entry's `table_id` must match what was logged for it.
#[test]
fn group_commit_table_tagged_flushes_and_round_trips() {
    let path = std::env::temp_dir().join(format!("cmvbt_wal_table_test_{}.log", std::process::id()));
    let _ = fs::remove_file(&path);

    let writer: WalWriter<u64, u64> =
        WalWriter::open(&path, Duration::from_millis(5)).unwrap();
    let clock = GlobalClock::new();

    let (s1, t1) = writer.start_commit_logged_for_table_with_ticket(11, &clock, 0, |_v| CRUDOperation::Insert(1, 100));
    let (s2, t2) = writer.start_commit_logged_for_table_with_ticket(22, &clock, 0, |_v| CRUDOperation::Delete(2));
    assert!(s2.ts_start() > s1.ts_start());

    writer.wait_flushed(t1);
    writer.wait_flushed(t2);

    drop(writer);

    let bytes = fs::read(&path).unwrap();
    let mut offset = 0;
    let mut seen = Vec::new();
    while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
        let (table_id, entry) = record::decode_entry_for_table::<u64, u64>(body).unwrap();
        seen.push((table_id, entry));
        offset += consumed;
    }
    assert_eq!(offset, bytes.len());
    assert_eq!(seen.len(), 2);

    match &seen[0] {
        (11, WalEntry::Write(r)) => assert_eq!(r.stamp.ts_start(), s1.ts_start()),
        other => panic!("expected (11, Write) first, got {:?}", other.0),
    }
    match &seen[1] {
        (22, WalEntry::Write(r)) => assert_eq!(r.stamp.ts_start(), s2.ts_start()),
        other => panic!("expected (22, Write) second, got {:?}", other.0),
    }

    let _ = fs::remove_file(&path);
}
