//! Bit-layout tests for `TxStamp`:
//!
//! ```text
//!  63 | 62 ............ 48 | 47 .................... 0
//!  I  |  worker_id (15 b)  |      ts_start (48 b)
//! ```
//!
//! Bit 63 is the invalidation flag on insertion stamps and the
//! delete-presence flag on `VersionInfo`'s deletion stamp.

use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::{Version, VersionInfo};
use crate::bat_sync::visibility::{SnapshotCache, is_visible};
use crate::bat_sync::worker::{READ_ONLY_SCAN_WORKER_ID, WorkerRegistry};
use crate::bat_wal::record::{WalEntry, WalRecord, decode, decode_entry, encode, encode_entry};

const BIT_63: u64 = 1 << 63;
const WORKER_MASK: u64 = 0x7FFF << 48;
const TS_MASK: u64 = (1 << 48) - 1;

/// Edge-case `ts_start` values: zero, small, every single bit position,
/// all-ones prefixes, and the 48-bit maximum.
fn ts_edge_values() -> Vec<Version> {
    let mut v = vec![0, 1, 2, 42, TS_MASK - 1, TS_MASK];
    for bit in 0..48 {
        v.push(1 << bit);
        v.push((1u64 << bit) - 1);
        v.push(TS_MASK ^ (1 << bit));
    }
    v
}

fn worker_edge_values() -> Vec<WorkerId> {
    let mut v = vec![0, 1, 2, 7, 0x3FFF, 0x4000, 0x7FFE, 0x7FFF];
    for bit in 0..15 {
        v.push(1 << bit);
        v.push(0x7FFF ^ (1 << bit));
    }
    v
}

/// Tiny deterministic xorshift so the randomized tests are reproducible.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

#[test]
fn constants_match_layout() {
    assert_eq!(TxStamp::TS_START_BITS, 48);
    assert_eq!(TxStamp::MAX_WORKER_ID, 0x7FFF);
    assert_eq!(TxStamp::MAX_WORKER_ID as u32, (1 << 15) - 1);
    assert_eq!(TxStamp::MAX_TS_START, (1u64 << 48) - 1);
    assert_eq!(std::mem::size_of::<TxStamp>(), 8);
    assert_eq!(std::mem::size_of::<VersionInfo>(), 16);
}

#[test]
fn exact_raw_bit_positions() {
    assert_eq!(TxStamp::new(0, 0).raw(), 0);
    assert_eq!(TxStamp::new(0, 1).raw(), 1);
    assert_eq!(TxStamp::new(1, 0).raw(), 1 << 48);
    assert_eq!(TxStamp::new(0x7FFF, 0).raw(), 0x7FFF << 48);
    assert_eq!(TxStamp::new(0, TS_MASK).raw(), TS_MASK);
    assert_eq!(TxStamp::new(0x7FFF, TS_MASK).raw(), u64::MAX >> 1);
    assert_eq!(TxStamp::new(0x1234, 0xABCD_EF01_2345).raw(), 0x1234_ABCD_EF01_2345);
    assert_eq!(TxStamp::new(0, 0).mark_invalid().raw(), BIT_63);
    assert_eq!(TxStamp::new(0x7FFF, TS_MASK).mark_invalid().raw(), u64::MAX);
}

#[test]
fn round_trips_every_worker_id_against_edge_timestamps() {
    let ts_values = ts_edge_values();
    for worker in 0..=TxStamp::MAX_WORKER_ID {
        for &ts in &ts_values {
            let s = TxStamp::new(worker, ts);
            assert_eq!(s.worker_id(), worker, "worker {worker} ts {ts:#x}");
            assert_eq!(s.ts_start(), ts, "worker {worker} ts {ts:#x}");
            assert!(!s.is_invalid(), "fresh stamp invalid: worker {worker} ts {ts:#x}");
            assert_eq!(s.raw() & BIT_63, 0);
            assert_eq!(s.raw(), ((worker as u64) << 48) | ts);
        }
    }
}

#[test]
fn fields_do_not_bleed_into_each_other() {
    for &w in &worker_edge_values() {
        for &ts in &ts_edge_values() {
            let s = TxStamp::new(w, ts);
            assert_eq!(s.raw() & TS_MASK, ts);
            assert_eq!((s.raw() & WORKER_MASK) >> 48, w as u64);
            // Changing only one field never changes the other.
            assert_eq!(TxStamp::new(w, 0).worker_id(), w);
            assert_eq!(TxStamp::new(0, ts).ts_start(), ts);
            assert_eq!(TxStamp::new(0x7FFF, ts).ts_start(), ts);
            assert_eq!(TxStamp::new(w, TS_MASK).worker_id(), w);
        }
    }
}

#[test]
fn mark_invalid_only_sets_bit_63_and_preserves_fields() {
    for &w in &worker_edge_values() {
        for &ts in &ts_edge_values() {
            let s = TxStamp::new(w, ts);
            let inv = s.mark_invalid();
            assert!(inv.is_invalid());
            assert_eq!(inv.worker_id(), w);
            assert_eq!(inv.ts_start(), ts);
            assert_eq!(inv.raw(), s.raw() | BIT_63);
            assert_eq!(inv.raw() ^ s.raw(), BIT_63);
            // Idempotent.
            assert_eq!(inv.mark_invalid(), inv);
            // Ordering ignores validity; equality does not.
            assert_eq!(inv.cmp(&s), std::cmp::Ordering::Equal);
            assert_ne!(inv, s);
        }
    }
}

#[test]
fn randomized_round_trip() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for _ in 0..1_000_000 {
        let r = rng.next();
        let w = (r >> 48) as WorkerId & TxStamp::MAX_WORKER_ID;
        let ts = rng.next() & TS_MASK;
        let s = TxStamp::new(w, ts);
        assert_eq!((s.worker_id(), s.ts_start(), s.is_invalid()), (w, ts, false));
        let inv = s.mark_invalid();
        assert_eq!((inv.worker_id(), inv.ts_start(), inv.is_invalid()), (w, ts, true));
        assert_eq!(TxStamp::from_raw(s.raw()), s);
        assert_eq!(TxStamp::from_raw(inv.raw()), inv);
    }
}

#[test]
fn from_raw_decodes_arbitrary_bits() {
    let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
    for _ in 0..100_000 {
        let raw = rng.next();
        let s = TxStamp::from_raw(raw);
        assert_eq!(s.ts_start(), raw & TS_MASK);
        assert_eq!(s.worker_id() as u64, (raw >> 48) & 0x7FFF);
        assert_eq!(s.is_invalid(), raw & BIT_63 != 0);
        assert!(s.worker_id() <= TxStamp::MAX_WORKER_ID);
        assert!(s.ts_start() <= TxStamp::MAX_TS_START);
    }
}

#[test]
fn ordering_is_by_ts_start_only() {
    let a = TxStamp::new(0x7FFF, 10);
    let b = TxStamp::new(0, 11);
    assert!(a < b);
    assert!(TxStamp::new(0, TS_MASK - 1) < TxStamp::new(0x7FFF, TS_MASK) );
    assert_eq!(TxStamp::new(3, 5).cmp(&TxStamp::new(9, 5)), std::cmp::Ordering::Equal);
    // An invalid (bit 63) stamp must not sort as "newer".
    assert!(TxStamp::new(0, 5).mark_invalid() < TxStamp::new(0, 6));
}

#[test]
fn try_new_accepts_in_range_and_rejects_overflow() {
    assert_eq!(TxStamp::try_new(0, 0), Some(TxStamp::new(0, 0)));
    assert_eq!(
        TxStamp::try_new(TxStamp::MAX_WORKER_ID, TxStamp::MAX_TS_START),
        Some(TxStamp::new(TxStamp::MAX_WORKER_ID, TxStamp::MAX_TS_START))
    );
    assert_eq!(TxStamp::try_new(TxStamp::MAX_WORKER_ID + 1, 0), None);
    assert_eq!(TxStamp::try_new(WorkerId::MAX, 0), None);
    assert_eq!(TxStamp::try_new(0, TxStamp::MAX_TS_START + 1), None);
    assert_eq!(TxStamp::try_new(0, Version::MAX), None);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "worker_id exceeds 15 bits")]
fn new_rejects_16_bit_worker_in_debug() {
    let _ = TxStamp::new(0x8000, 0);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "ts_start exceeds 48 bits")]
fn new_rejects_49_bit_ts_in_debug() {
    let _ = TxStamp::new(0, 1 << 48);
}

/// Release builds skip `new`'s debug_asserts; an oversized worker id must
/// still be masked to 15 bits and never spill into the invalid flag, and an
/// oversized ts_start must never spill into the worker field.
#[test]
#[cfg(not(debug_assertions))]
fn new_masks_oversized_fields_in_release() {
    for &w in &worker_edge_values() {
        let s = TxStamp::new(w | 0x8000, 7);
        assert!(!s.is_invalid());
        assert_eq!(s.worker_id(), w);
        assert_eq!(s.ts_start(), 7);
    }
    let s = TxStamp::new(5, u64::MAX);
    assert!(!s.is_invalid());
    assert_eq!(s.worker_id(), 5);
    assert_eq!(s.ts_start(), TS_MASK);
}

#[test]
fn display_shows_decoded_fields() {
    assert_eq!(TxStamp::new(0x7FFF, TS_MASK).to_string(), format!("(w32767@{TS_MASK})"));
    assert_eq!(TxStamp::new(3, 9).mark_invalid().to_string(), "(w3@9)");
}

/// Deletion stamps share the layout; bit 63 is their presence flag. A
/// max-worker/max-ts delete stamp must not be confused with "absent", and
/// must decode back exactly (without the presence bit leaking in as
/// "invalid").
#[test]
fn version_info_delete_stamp_round_trips_at_extremes() {
    for &w in &worker_edge_values() {
        for &ts in &[0, 1, 1 << 47, TS_MASK] {
            let ins = TxStamp::new(0, 0);
            let del = TxStamp::new(w, ts);

            let v = VersionInfo::new(ins);
            assert!(!v.is_deleted());
            assert!(v.is_live());
            assert_eq!(v.deletion_stamp(), None);

            assert!(v.delete(del));
            assert!(v.is_deleted());
            assert!(!v.is_live());
            let got = v.deletion_stamp().expect("delete stamp present");
            assert_eq!(got, del);
            assert_eq!((got.worker_id(), got.ts_start()), (w, ts));
            assert!(!got.is_invalid());
            assert_eq!(v.insertion_stamp(), ins);

            // Deleting an already-deleted version is refused.
            assert!(!v.delete(TxStamp::new(1, 1)));
            assert_eq!(v.deletion_stamp(), Some(del));

            v.undelete();
            assert!(!v.is_deleted());
            assert!(v.is_live());
            assert_eq!(v.deletion_stamp(), None);

            let both = VersionInfo::from(ins, del);
            assert_eq!(both.deletion_stamp(), Some(del));
            assert_eq!(both.insertion_stamp(), ins);
        }
    }
}

/// The all-zero delete word (worker 0, ts 0) must still read as "absent",
/// while a real delete stamp of worker 0 at ts 0 reads as present.
#[test]
fn zero_delete_stamp_is_distinguishable_from_absent() {
    let v = VersionInfo::new(TxStamp::new(1, 1));
    assert_eq!(v.deletion_stamp(), None);
    assert!(v.delete(TxStamp::new(0, 0)));
    assert_eq!(v.deletion_stamp(), Some(TxStamp::new(0, 0)));
}

#[test]
fn version_info_invalidate_preserves_insert_fields() {
    for &w in &worker_edge_values() {
        for &ts in &[0, 1, 1 << 47, TS_MASK] {
            let stamp = TxStamp::new(w, ts);
            let v = VersionInfo::new(stamp);
            assert!(v.is_live());
            v.invalidate();
            let got = v.insertion_stamp();
            assert!(got.is_invalid());
            assert_eq!((got.worker_id(), got.ts_start()), (w, ts));
            assert!(!v.is_live());
            assert!(!v.is_deleted());
            // Invalidated records cannot be deleted.
            assert!(!v.delete(TxStamp::new(1, 1)));
            // Idempotent.
            v.invalidate();
            assert_eq!(v.insertion_stamp(), got);
            // Clone carries the invalid bit.
            assert_eq!(v.clone().insertion_stamp(), got);
        }
    }
}

/// Same-worker fast path of `is_visible` exercised at the top of the
/// 15-bit worker range — it never touches commit logs / snapshot cache.
#[test]
fn visibility_same_worker_and_invalid_at_max_worker() {
    let mut cache = SnapshotCache::new(0);
    let w = TxStamp::MAX_WORKER_ID;
    for &(stamp_ts, reader_ts, expect) in &[
        (5, 5, true),
        (5, 6, true),
        (6, 5, false),
        (TS_MASK, TS_MASK, true),
        (0, TS_MASK, true),
    ] {
        let s = TxStamp::new(w, stamp_ts);
        assert_eq!(is_visible(&[], &mut cache, w, reader_ts, s), expect);
        // An invalidated stamp is never visible — even to its own worker.
        assert!(!is_visible(&[], &mut cache, w, reader_ts, s.mark_invalid()));
    }
}

/// `READ_ONLY_SCAN_WORKER_ID` lies outside the 15-bit worker field, so no
/// encodable stamp can ever claim it (which would wrongly take the
/// same-worker fast path).
#[test]
fn read_only_scan_worker_id_is_unencodable() {
    assert!(READ_ONLY_SCAN_WORKER_ID > TxStamp::MAX_WORKER_ID);
    assert_eq!(TxStamp::try_new(READ_ONLY_SCAN_WORKER_ID, 0), None);
    let mut rng = Rng(7);
    for _ in 0..100_000 {
        assert_ne!(TxStamp::from_raw(rng.next()).worker_id(), READ_ONLY_SCAN_WORKER_ID);
    }
}

#[test]
fn worker_registry_accepts_full_15_bit_capacity() {
    let r = WorkerRegistry::new(TxStamp::MAX_WORKER_ID as usize + 1);
    assert_eq!(r.max_workers(), 1 << 15);
}

#[test]
#[should_panic(expected = "exceeds TxStamp worker capacity")]
fn worker_registry_rejects_capacity_past_15_bits() {
    let _ = WorkerRegistry::new(TxStamp::MAX_WORKER_ID as usize + 2);
}

#[test]
fn wal_record_round_trips_extreme_stamps() {
    for &w in &[0, 1, 0x4000, TxStamp::MAX_WORKER_ID] {
        for &ts in &[0, 1, 1 << 47, TS_MASK] {
            let record = WalRecord {
                stamp: TxStamp::new(w, ts),
                op: CRUDOperation::Insert(11u64, 22u64),
            };
            let mut body = Vec::new();
            encode(&record, &mut body);
            let got: WalRecord<u64, u64> = decode(&body).expect("valid record");
            assert_eq!(got.stamp, record.stamp);

            let entry: WalEntry<u64, u64> = WalEntry::Commit {
                stamp: TxStamp::new(w, ts),
                ts_commit: ts + 1,
            };
            let mut body = Vec::new();
            encode_entry(&entry, &mut body);
            match decode_entry::<u64, u64>(&body).expect("valid commit") {
                WalEntry::Commit { stamp, ts_commit } => {
                    assert_eq!(stamp, TxStamp::new(w, ts));
                    assert_eq!(ts_commit, ts + 1);
                }
                WalEntry::Write(_) => panic!("expected Commit"),
            }
        }
    }
}

/// A WAL record whose on-disk worker id / ts_start don't fit the stamp
/// layout is rejected rather than silently masked into another stamp.
#[test]
fn wal_decode_rejects_out_of_range_stamp_fields() {
    let record = WalRecord {
        stamp: TxStamp::new(1, 1),
        op: CRUDOperation::Insert(11u64, 22u64),
    };
    let mut body = Vec::new();
    encode(&record, &mut body);
    // Header: tag(1) | ts_start(8, LE) | worker_id(2, LE).
    assert!(decode::<u64, u64>(&body).is_some());

    let mut bad_worker = body.clone();
    bad_worker[9..11].copy_from_slice(&0x8000u16.to_le_bytes());
    assert!(decode::<u64, u64>(&bad_worker).is_none());

    let mut bad_ts = body.clone();
    bad_ts[1..9].copy_from_slice(&(1u64 << 48).to_le_bytes());
    assert!(decode::<u64, u64>(&bad_ts).is_none());

    let entry: WalEntry<u64, u64> = WalEntry::Commit {
        stamp: TxStamp::new(1, 1),
        ts_commit: 2,
    };
    let mut body = Vec::new();
    encode_entry(&entry, &mut body);
    let mut bad_worker = body.clone();
    bad_worker[9..11].copy_from_slice(&0xFFFFu16.to_le_bytes());
    assert!(decode_entry::<u64, u64>(&bad_worker).is_none());
    let mut bad_ts = body.clone();
    bad_ts[1..9].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(decode_entry::<u64, u64>(&bad_ts).is_none());
}
