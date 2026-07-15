use std::cmp::Ord;
use std::fmt::Display;
use std::hash::Hash;
use std::mem::size_of;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::Version;

const TAG_INSERT: u8 = 0;
const TAG_UPDATE: u8 = 1;
const TAG_DELETE: u8 = 2;
const TAG_INVALIDATE: u8 = 3;
// const TAG_UNDELETE: u8 = 4;

/// How a `Payload` is (de)serialized to/from the WAL. `Key` is not covered by
/// this trait — every real instantiation in this project uses `Key = u64`,
/// so its wire encoding stays the fixed-size raw copy `write_raw`/`read_raw`
/// always did.
///
/// The base system's actual payload (`u64`) is POD with no owned heap data,
/// so its impl below is a trivial fixed-size byte copy. A payload with
/// heap-owned fields (e.g. `TpccRow`'s boxed rows containing `String`s) MUST
/// NOT rely on `write_raw`/`read_raw` (raw pointer reinterpretation is
/// unsound the moment heap ownership is involved — see this trait's `impl`
/// site for `TpccRow` in `mv_bench::tpcc_wal_codec`) and instead provide a
/// real, self-describing (variable-length) encoding here.
///
/// `wal_decode` receives *exactly* this record's payload bytes — the WAL's
/// outer per-record framing (`frame`/`read_frame`) already delimits the
/// whole record by length, so there's no separate length prefix to manage
/// here; a variable-length impl can simply consume as many bytes as it needs
/// and return `None` on any malformed/truncated input (treated the same as
/// any other decode failure: a torn write, not a panic).
pub trait WalPayload: Sized {
    fn wal_encode(&self, out: &mut Vec<u8>);
    fn wal_decode(bytes: &[u8]) -> Option<Self>;
}

impl WalPayload for u64 {
    #[inline]
    fn wal_encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_le_bytes());
    }

    #[inline]
    fn wal_decode(bytes: &[u8]) -> Option<Self> {
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }
}

/// One logged, auto-committed write: a single `CRUDOperation` (Insert,
/// Update, or Delete — the only variants ever passed to `encode`) tagged
/// with the OSIC stamp (worker + ts_start) it was written at. `CRUDOperation`
/// doubles as both the live dispatch request type and the WAL's record
/// payload, since today a dispatched `CRUDOperation` already *is* a
/// one-operation, auto-committed transaction.
#[derive(Copy, Clone)]
pub struct WalRecord<Key: Ord + Copy + Hash + Display, Payload: Clone> {
    pub stamp: TxStamp,
    pub op: CRUDOperation<Key, Payload>,
}

/// Reinterprets `value`'s bytes directly — only ever used for `Key` now
/// (`Payload` goes through `WalPayload` instead). Deliberately not bounded by
/// `Copy`: `T: Sized` is all raw byte reinterpretation actually needs, and
/// requiring `Copy` here would force every generic caller up the call chain
/// to also require `Key: Copy`, even though only this module cares. The real
/// constraint — `T` must be a plain fixed-size value with no owned heap data
/// (no `Vec`/`String`/`Box`) — is a documented discipline, not something the
/// type system enforces; it holds for this project's actual `Key = u64`.
#[inline]
unsafe fn write_raw<T>(out: &mut Vec<u8>, value: &T) {
    let bytes = unsafe {
        std::slice::from_raw_parts(value as *const T as *const u8, size_of::<T>())
    };
    out.extend_from_slice(bytes);
}

#[inline]
unsafe fn read_raw<T>(bytes: &[u8]) -> T {
    debug_assert!(bytes.len() >= size_of::<T>());
    unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const T) }
}

/// Encodes a single record's body (no length prefix, no checksum) as:
/// `[u8 tag][u64 ts_start][u16 worker_id][Key bytes][Payload bytes]?`
/// `Key` is always raw fixed-size bytes (every real instantiation uses
/// `Key = u64`, see `write_raw`); `Payload` uses its `WalPayload` impl, which
/// may be fixed-size raw bytes (POD payloads like `u64`) or a real
/// variable-length encoding (payloads with owned heap data, like `TpccRow`).
///
/// Panics if `record.op` isn't `Insert`/`Update`/`Delete`/`Invalidate`/
/// `Undelete` — nothing else is ever handed to this function: the dispatch
/// layer only calls `wal_start_commit` for `Insert`/`Update`/`Delete`,
/// `MVBTSt::abort_write` only calls `wal_log_write` for `Invalidate`/
/// `Undelete` (see `mv_page_model::leaf_page::AbortOutcome`), and read/
/// `*Rand` operations are never logged at all.
pub fn encode<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
    record: &WalRecord<Key, Payload>,
    out: &mut Vec<u8>,
) {
    let (tag, key, payload): (u8, &Key, Option<&Payload>) = match &record.op {
        CRUDOperation::Insert(key, payload) => (TAG_INSERT, key, Some(payload)),
        CRUDOperation::Update(key, payload) => (TAG_UPDATE, key, Some(payload)),
        CRUDOperation::Delete(key) => (TAG_DELETE, key, None),
        CRUDOperation::Invalidate(key) => (TAG_INVALIDATE, key, None),
        // CRUDOperation::Undelete(key) => (TAG_UNDELETE, key, None),
        other => unreachable!("WAL only ever logs Insert/Update/Delete/Invalidate/Undelete, got: {other}"),
    };

    out.push(tag);
    out.extend_from_slice(&record.stamp.ts_start().to_le_bytes());
    out.extend_from_slice(&record.stamp.worker_id().to_le_bytes());
    unsafe { write_raw(out, key) };
    if let Some(payload) = payload {
        payload.wal_encode(out);
    }
}

/// Decodes a record body produced by [`encode`]. Returns `None` if `bytes`
/// is too short, the tag is unrecognized, or `Payload::wal_decode` rejects
/// its bytes — all treated as corruption by callers. Since the WAL's outer
/// per-record framing (`frame`/`read_frame`) already delimits the exact
/// bytes belonging to this record, whatever remains after the key is handed
/// to `Payload::wal_decode` in full — no separate payload length to compute.
pub fn decode<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
    bytes: &[u8],
) -> Option<WalRecord<Key, Payload>> {
    let key_sz = size_of::<Key>();
    let header_sz = 1 + 8 + 2;

    if bytes.len() < header_sz + key_sz {
        return None;
    }

    let tag = bytes[0];
    let ts_start = Version::from_le_bytes(bytes[1..9].try_into().ok()?);
    let worker_id = WorkerId::from_le_bytes(bytes[9..11].try_into().ok()?);
    let stamp = TxStamp::new(worker_id, ts_start);

    let key_bytes = &bytes[header_sz..header_sz + key_sz];
    let key: Key = unsafe { read_raw(key_bytes) };

    let op = match tag {
        TAG_INSERT | TAG_UPDATE => {
            let payload_bytes = &bytes[header_sz + key_sz..];
            let payload: Payload = Payload::wal_decode(payload_bytes)?;

            if tag == TAG_INSERT {
                CRUDOperation::Insert(key, payload)
            } else {
                CRUDOperation::Update(key, payload)
            }
        }
        TAG_DELETE => CRUDOperation::Delete(key),
        TAG_INVALIDATE => CRUDOperation::Invalidate(key),
        // TAG_UNDELETE => CRUDOperation::Undelete(key),
        _ => return None,
    };

    Some(WalRecord { stamp, op })
}

/// Standard (IEEE) CRC-32, implemented by hand to avoid pulling in a
/// dependency and to stay stable across toolchains/versions (unlike e.g.
/// `DefaultHasher`, whose algorithm is explicitly not guaranteed stable).
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in bytes {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Wraps an encoded record body with a length prefix and trailing checksum,
/// as physically written to / read from the log file:
/// `[u32 len][body (len bytes)][u32 crc32(body)]`.
pub fn frame(body: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(&crc32(body).to_le_bytes());
}

/// Same wire format as `encode` followed by `frame`
/// (`[u32 len][body][u32 crc32(body)]`), but encodes the body straight into
/// `out` instead of into a separate buffer first — one allocation instead
/// of two, and no copy of the body into a second buffer. The length prefix
/// is written as a placeholder, then patched once the body's actual length
/// is known. Used by `WalWriter::log_with_stamp`, which frames exactly one
/// record per call and has no other reason to keep the body separate.
pub fn encode_framed<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
    record: &WalRecord<Key, Payload>,
    out: &mut Vec<u8>,
) {
    let start = out.len();
    out.extend_from_slice(&0u32.to_le_bytes());
    encode(record, out);

    let body_len = (out.len() - start - 4) as u32;
    out[start..start + 4].copy_from_slice(&body_len.to_le_bytes());

    let crc = crc32(&out[start + 4..]);
    out.extend_from_slice(&crc.to_le_bytes());
}

/// Attempts to read one framed record starting at `bytes[0]`. Returns the
/// body slice and the total number of bytes consumed (frame overhead + body),
/// or `None` if the frame is incomplete or fails its checksum — both cases
/// are treated identically by recovery as "log ends here" (a torn write from
/// a crash mid-fsync is expected, not an error).
pub fn read_frame(bytes: &[u8]) -> Option<(&[u8], usize)> {
    if bytes.len() < 4 {
        return None;
    }
    let len = u32::from_le_bytes(bytes[0..4].try_into().ok()?) as usize;
    let total = 4 + len + 4;
    if bytes.len() < total {
        return None;
    }
    let body = &bytes[4..4 + len];
    let stored_crc = u32::from_le_bytes(bytes[4 + len..total].try_into().ok()?);
    if crc32(body) != stored_crc {
        return None;
    }
    Some((body, total))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_ops_eq(a: &CRUDOperation<u64, u64>, b: &CRUDOperation<u64, u64>) {
        match (a, b) {
            (CRUDOperation::Insert(k1, p1), CRUDOperation::Insert(k2, p2))
            | (CRUDOperation::Update(k1, p1), CRUDOperation::Update(k2, p2)) => {
                assert_eq!(k1, k2);
                assert_eq!(p1, p2);
            }
            (CRUDOperation::Delete(k1), CRUDOperation::Delete(k2))
            | (CRUDOperation::Invalidate(k1), CRUDOperation::Invalidate(k2))
            // | (CRUDOperation::Undelete(k1), CRUDOperation::Undelete(k2))
            => assert_eq!(k1, k2),
            _ => panic!("operation kind mismatch: {a} vs {b}"),
        }
    }

    #[test]
    fn round_trips_all_variants() {
        let ops: Vec<CRUDOperation<u64, u64>> = vec![
            CRUDOperation::Insert(1, 100),
            CRUDOperation::Update(2, 200),
            CRUDOperation::Delete(3),
            CRUDOperation::Invalidate(4),
            // CRUDOperation:: Undelete(5),
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

    /// `encode_framed`'s single-buffer path must produce byte-for-byte the
    /// same frame as the two-step `encode` + `frame` it replaces in
    /// `WalWriter::log_with_stamp` — otherwise recovery (which only knows
    /// the two-step format's invariants) could silently start reading a
    /// different wire format.
    #[test]
    fn encode_framed_matches_two_step_encode_and_frame() {
        let record = WalRecord { stamp: TxStamp::new(3, 99), op: CRUDOperation::Update(5u64, 6u64) };

        let mut body = Vec::new();
        encode(&record, &mut body);
        let mut expected = Vec::new();
        frame(&body, &mut expected);

        let mut actual = Vec::new();
        encode_framed(&record, &mut actual);

        assert_eq!(actual, expected);

        let (read_body, consumed) = read_frame(&actual).expect("valid frame");
        assert_eq!(consumed, actual.len());
        let decoded: WalRecord<u64, u64> = decode(read_body).expect("valid record");
        assert_eq!(decoded.stamp.ts_start(), record.stamp.ts_start());
        assert_ops_eq(&decoded.op, &record.op);
    }

    /// Same byte-for-byte equivalence, but writing into a buffer that
    /// already has unrelated bytes at the front — guards against the
    /// placeholder-patch math in `encode_framed` assuming `out` starts
    /// empty (it only ever gets called that way today, but the offset
    /// arithmetic must stay correct if that changes).
    #[test]
    fn encode_framed_patches_length_correctly_with_a_nonempty_prefix() {
        let record: WalRecord<u64, u64> = WalRecord { stamp: TxStamp::new(1, 1), op: CRUDOperation::Delete(7u64) };

        let mut body = Vec::new();
        encode(&record, &mut body);
        let mut expected_frame = Vec::new();
        frame(&body, &mut expected_frame);

        let mut actual = vec![0xAAu8; 5];
        encode_framed(&record, &mut actual);

        assert_eq!(&actual[5..], &expected_frame[..]);
        assert_eq!(&actual[..5], &[0xAA; 5]);
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
}
