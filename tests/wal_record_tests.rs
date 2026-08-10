use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_record_model::tx_stamp::TxStamp;
use crate::mv_record_model::version_info::Version;
use crate::mv_wal::record::{
    decode, decode_entry, decode_entry_for_table, encode, encode_entry, encode_entry_for_table_framed,
    encode_entry_framed, frame, read_frame, resync_next, WalEntry, WalRecord, TABLE_ID_COMMIT_SENTINEL,
};

fn assert_ops_eq(a: &CRUDOperation<u64, u64>, b: &CRUDOperation<u64, u64>) {
    match (a, b) {
        (CRUDOperation::Insert(k1, p1), CRUDOperation::Insert(k2, p2))
        | (CRUDOperation::Update(k1, p1), CRUDOperation::Update(k2, p2)) => {
            assert_eq!(k1, k2);
            assert_eq!(p1, p2);
        }
        (CRUDOperation::Delete(k1), CRUDOperation::Delete(k2)) => assert_eq!(k1, k2),
        _ => panic!("operation kind mismatch: {a} vs {b}"),
    }
}

#[test]
fn round_trips_all_variants() {
    let ops: Vec<CRUDOperation<u64, u64>> = vec![
        CRUDOperation::Insert(1, 100),
        CRUDOperation::Update(2, 200),
        CRUDOperation::Delete(3),
    ];

    for (i, op) in ops.into_iter().enumerate() {
        let record = WalRecord { stamp: TxStamp::new(7, i as Version + 1), op };
        let mut body = Vec::new();
        encode(&record, &mut body);

        let mut framed = Vec::new();
        frame(&body, &mut framed);

        let (read_body, consumed) = read_frame(&framed).expect("valid frame");
        assert_eq!(consumed, framed.len());

        let decoded: WalRecord<u64, u64> = decode(read_body).expect("valid record");
        assert_eq!(decoded.stamp.ts_start(), record.stamp.ts_start());
        assert_eq!(decoded.stamp.worker_id(), record.stamp.worker_id());
        assert_ops_eq(&decoded.op, &record.op);
    }
}

#[test]
fn detects_truncated_frame() {
    let record = WalRecord { stamp: TxStamp::new(1, 42), op: CRUDOperation::Insert(1u64, 2u64) };
    let mut body = Vec::new();
    encode(&record, &mut body);
    let mut framed = Vec::new();
    frame(&body, &mut framed);

    for cut in 1..framed.len() {
        assert!(read_frame(&framed[..cut]).is_none(), "cut at {cut} should be incomplete");
    }
}

/// `encode_entry_framed`'s single-buffer path must produce byte-for-byte
/// the same frame as the two-step `encode` + `frame` it replaces in
/// `WalWriter::log_with_stamp` — otherwise recovery (which only knows
/// the two-step format's invariants) could silently start reading a
/// different wire format.
#[test]
fn encode_entry_framed_matches_two_step_encode_and_frame() {
    let record = WalRecord { stamp: TxStamp::new(3, 99), op: CRUDOperation::Update(5u64, 6u64) };

    let mut body = Vec::new();
    encode(&record, &mut body);
    let mut expected = Vec::new();
    frame(&body, &mut expected);

    let mut actual = Vec::new();
    encode_entry_framed(&WalEntry::Write(record.clone()), &mut actual);

    assert_eq!(actual, expected);

    let (read_body, consumed) = read_frame(&actual).expect("valid frame");
    assert_eq!(consumed, actual.len());
    let decoded: WalRecord<u64, u64> = decode(read_body).expect("valid record");
    assert_eq!(decoded.stamp.ts_start(), record.stamp.ts_start());
    assert_ops_eq(&decoded.op, &record.op);
}

/// Same byte-for-byte equivalence, but writing into a buffer that
/// already has unrelated bytes at the front — guards against the
/// placeholder-patch math in `encode_entry_framed` assuming `out` starts
/// empty (it only ever gets called that way today, but the offset
/// arithmetic must stay correct if that changes).
#[test]
fn encode_entry_framed_patches_length_correctly_with_a_nonempty_prefix() {
    let record: WalRecord<u64, u64> = WalRecord { stamp: TxStamp::new(1, 1), op: CRUDOperation::Delete(7u64) };

    let mut body = Vec::new();
    encode(&record, &mut body);
    let mut expected_frame = Vec::new();
    frame(&body, &mut expected_frame);

    let mut actual = vec![0xAAu8; 5];
    encode_entry_framed(&WalEntry::Write(record), &mut actual);

    assert_eq!(&actual[5..], &expected_frame[..]);
    assert_eq!(&actual[..5], &[0xAA; 5]);
}

/// A `Commit` marker must round-trip through frame/read_frame/decode_entry
/// distinctly from a `Write` entry, carrying the exact `(worker_id,
/// ts_start, ts_commit)` it was built with.
#[test]
fn commit_entry_round_trips() {
    let entry: WalEntry<u64, u64> = WalEntry::Commit { stamp: TxStamp::new(9, 123), ts_commit: 456 };

    let mut framed = Vec::new();
    encode_entry_framed(&entry, &mut framed);

    let (body, consumed) = read_frame(&framed).expect("valid frame");
    assert_eq!(consumed, framed.len());

    match decode_entry::<u64, u64>(body).expect("valid entry") {
        WalEntry::Commit { stamp, ts_commit } => {
            assert_eq!(stamp.worker_id(), 9);
            assert_eq!(stamp.ts_start(), 123);
            assert_eq!(ts_commit, 456);
        }
        WalEntry::Write(_) => panic!("expected a Commit entry"),
    }
}

/// A `Write` entry decodes back as `WalEntry::Write`, not `Commit` —
/// `decode_entry` must actually distinguish the two by tag, not just
/// happen to succeed on either.
#[test]
fn write_entry_decodes_as_write_not_commit() {
    let record = WalRecord { stamp: TxStamp::new(2, 10), op: CRUDOperation::Insert(1u64, 2u64) };
    let mut framed = Vec::new();
    encode_entry_framed(&WalEntry::Write(record), &mut framed);

    let (body, _) = read_frame(&framed).expect("valid frame");
    match decode_entry::<u64, u64>(body).expect("valid entry") {
        WalEntry::Write(r) => assert_eq!(r.stamp.ts_start(), 10),
        WalEntry::Commit { .. } => panic!("expected a Write entry"),
    }
}

/// A table-tagged `Write` entry round-trips its `table_id` alongside the
/// same record data `decode_entry` already covers.
#[test]
fn table_tagged_entry_round_trips() {
    let record = WalRecord { stamp: TxStamp::new(4, 55), op: CRUDOperation::Insert(9u64, 10u64) };
    let mut framed = Vec::new();
    encode_entry_for_table_framed(42, &WalEntry::Write(record), &mut framed);

    let (body, consumed) = read_frame(&framed).expect("valid frame");
    assert_eq!(consumed, framed.len());

    let (table_id, entry) = decode_entry_for_table::<u64, u64>(body).expect("valid entry");
    assert_eq!(table_id, 42);
    match entry {
        WalEntry::Write(r) => {
            assert_eq!(r.stamp.ts_start(), 55);
            assert_ops_eq(&r.op, &CRUDOperation::Insert(9u64, 10u64));
        }
        WalEntry::Commit { .. } => panic!("expected a Write entry"),
    }
}

/// A table-tagged `Commit` entry carries `TABLE_ID_COMMIT_SENTINEL`
/// through the round-trip unchanged — replay ignores this value, but the
/// encode/decode pair itself must still preserve whatever was written.
#[test]
fn table_tagged_commit_round_trips_with_sentinel() {
    let entry: WalEntry<u64, u64> = WalEntry::Commit { stamp: TxStamp::new(1, 7), ts_commit: 8 };
    let mut framed = Vec::new();
    encode_entry_for_table_framed(TABLE_ID_COMMIT_SENTINEL, &entry, &mut framed);

    let (body, consumed) = read_frame(&framed).expect("valid frame");
    assert_eq!(consumed, framed.len());

    let (table_id, decoded) = decode_entry_for_table::<u64, u64>(body).expect("valid entry");
    assert_eq!(table_id, TABLE_ID_COMMIT_SENTINEL);
    match decoded {
        WalEntry::Commit { stamp, ts_commit } => {
            assert_eq!(stamp.worker_id(), 1);
            assert_eq!(stamp.ts_start(), 7);
            assert_eq!(ts_commit, 8);
        }
        WalEntry::Write(_) => panic!("expected a Commit entry"),
    }
}

/// `encode_entry_for_table_framed`'s single-buffer path must produce
/// byte-for-byte the same frame as manually prepending the table id
/// before `encode_entry` and then framing — mirrors
/// `encode_entry_framed_matches_two_step_encode_and_frame`'s check for
/// the non-table-tagged path.
#[test]
fn table_tagged_framed_matches_manual_prefix_plus_frame() {
    let record: WalRecord<u64, u64> = WalRecord { stamp: TxStamp::new(2, 3), op: CRUDOperation::Delete(5u64) };
    let entry = WalEntry::Write(record);

    let mut expected_body = Vec::new();
    expected_body.extend_from_slice(&99u32.to_le_bytes());
    encode_entry(&entry, &mut expected_body);
    let mut expected = Vec::new();
    frame(&expected_body, &mut expected);

    let mut actual = Vec::new();
    encode_entry_for_table_framed(99, &entry, &mut actual);

    assert_eq!(actual, expected);
}

/// Same truncation-detection guarantee as `detects_truncated_frame`, for
/// the table-tagged framing.
#[test]
fn detects_truncated_table_tagged_frame() {
    let record = WalRecord { stamp: TxStamp::new(1, 1), op: CRUDOperation::Insert(1u64, 2u64) };
    let mut framed = Vec::new();
    encode_entry_for_table_framed(7, &WalEntry::Write(record), &mut framed);

    for cut in 1..framed.len() {
        assert!(read_frame(&framed[..cut]).is_none(), "cut at {cut} should be incomplete");
    }
}

#[test]
fn detects_corrupted_body() {
    let record = WalRecord { stamp: TxStamp::new(1, 42), op: CRUDOperation::Insert(1u64, 2u64) };
    let mut body = Vec::new();
    encode(&record, &mut body);
    let mut framed = Vec::new();
    frame(&body, &mut framed);

    let corrupt_idx = 4; // first body byte (tag)
    framed[corrupt_idx] ^= 0xFF;

    assert!(read_frame(&framed).is_none());
}

/// `resync_next` must skip clean over an **interior** hole (the scenario
/// `read_frame`'s plain "stop at the first bad frame" can't handle — see
/// that function's doc for why `LockFreeWalWriter` can produce one) and
/// still find the record on the far side, at a hole length that is
/// deliberately *not* a multiple of the frame header size, so a naive
/// "skip by 8 bytes and retry" fix (which would misalign past the hole's
/// true end) would fail this.
#[test]
fn resync_next_skips_an_interior_hole_of_unaligned_length() {
    let first = WalRecord { stamp: TxStamp::new(1, 1), op: CRUDOperation::Insert(1u64, 100u64) };
    let second = WalRecord { stamp: TxStamp::new(1, 2), op: CRUDOperation::Insert(2u64, 200u64) };

    let mut bytes = Vec::new();
    encode_entry_framed(&WalEntry::Write(first), &mut bytes);
    let first_len = bytes.len();
    let hole_len = 13; // not a multiple of 4, 8, or this frame's own header size
    bytes.extend(std::iter::repeat(0u8).take(hole_len));
    let second_offset = bytes.len();
    encode_entry_framed(&WalEntry::Write(second), &mut bytes);
    let second_len = bytes.len() - second_offset;

    // `offset` tracks where each `resync_next` call *starts* scanning from,
    // not where it actually found its entry (that's `offset` plus however
    // much of `consumed` was spent skipping) — recovery only ever needs
    // "advance by `consumed`", never the found position itself, so that's
    // all this loop tracks too.
    let mut offset = 0;
    let mut found = Vec::new();
    while let Some((entry, consumed)) = resync_next(&bytes[offset..], decode_entry::<u64, u64>) {
        found.push(entry);
        offset += consumed;
    }

    assert_eq!(offset, bytes.len(), "must consume every byte, including the hole, once fully resynced");
    assert_eq!(found.len(), 2, "both records must survive, despite the hole between them");
    match &found[0] {
        WalEntry::Write(r) => assert_eq!(r.stamp.ts_start(), 1),
        _ => panic!("expected first Write"),
    }
    match &found[1] {
        WalEntry::Write(r) => assert_eq!(r.stamp.ts_start(), 2),
        _ => panic!("expected second Write"),
    }
    // The hole's exact position/length round-trips correctly too, not just
    // the two records either side of it.
    assert_eq!(first_len + hole_len, second_offset);
    assert_eq!(second_offset + second_len, bytes.len());
}

/// A hole reaching all the way to true EOF (nothing follows it) must be
/// treated as a torn tail, same as `read_frame` already does — `resync_next`
/// generalizes "stop at the first bad frame" to "stop once nothing further
/// is found", not "keep looking forever".
#[test]
fn resync_next_returns_none_when_the_hole_reaches_eof() {
    let first = WalRecord { stamp: TxStamp::new(1, 1), op: CRUDOperation::Insert(1u64, 100u64) };
    let mut bytes = Vec::new();
    encode_entry_framed(&WalEntry::Write(first), &mut bytes);
    bytes.extend(std::iter::repeat(0u8).take(20));

    let (entry, consumed) = resync_next(&bytes, decode_entry::<u64, u64>).expect("first record still found");
    match entry {
        WalEntry::Write(r) => assert_eq!(r.stamp.ts_start(), 1),
        _ => panic!("expected a Write"),
    }
    assert!(resync_next(&bytes[consumed..], decode_entry::<u64, u64>).is_none(), "trailing hole-to-EOF must not fabricate a record");
}
