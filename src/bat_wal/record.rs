use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::Version;
use std::cmp::Ord;
use std::fmt::Display;
use std::hash::Hash;
use std::mem::size_of;

const TAG_INSERT: u8 = 0;
const TAG_UPDATE: u8 = 1;
const TAG_DELETE: u8 = 2;
/// Marks that the transaction identified by `(worker_id, ts_start)` in the
/// header actually committed, carrying the `ts_commit` it committed at —
/// see `WalEntry::Commit`'s doc for why this is a separate entry kind
/// rather than a field on the original write record.
const TAG_COMMIT: u8 = 3;

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
/// site for `TpccRow` in `bat_bench::tpcc_wal_codec`) and instead provide a
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

    #[inline]
    fn wal_encode_size_hint(&self) -> usize {
        8
    }
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

#[inline]
unsafe fn write_raw<T>(out: &mut Vec<u8>, value: &T) {
    let bytes =
        unsafe { std::slice::from_raw_parts(value as *const T as *const u8, size_of::<T>()) };
    out.extend_from_slice(bytes);
}

#[inline]
unsafe fn read_raw<T>(bytes: &[u8]) -> T {
    debug_assert!(bytes.len() >= size_of::<T>());
    unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const T) }
}

pub fn encode<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
    record: &WalRecord<Key, Payload>,
    out: &mut Vec<u8>,
) {
    let (tag, key, payload): (u8, &Key, Option<&Payload>) = match &record.op {
        CRUDOperation::Insert(key, payload) => (TAG_INSERT, key, Some(payload)),
        CRUDOperation::Update(key, payload) => (TAG_UPDATE, key, Some(payload)),
        CRUDOperation::Delete(key) => (TAG_DELETE, key, None),
        other => unreachable!("WAL only ever logs Insert/Update/Delete, got: {other}"),
    };

    out.push(tag);
    out.extend_from_slice(&record.stamp.ts_start().to_le_bytes());
    out.extend_from_slice(&record.stamp.worker_id().to_le_bytes());
    unsafe { write_raw(out, key) };
    if let Some(payload) = payload {
        payload.wal_encode(out);
    }
}

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
        _ => return None,
    };

    Some(WalRecord { stamp, op })
}

/// One physical entry appended to the WAL: either a logged write (`Write`,
/// today's `WalRecord` unchanged) or a **Commit marker** (`Commit`)
/// confirming that the transaction identified by `stamp` actually committed
/// at `ts_commit`.
///
/// A write's `Write` entry is appended optimistically, before its
/// transaction is known to commit (see
/// `bat_sync::version_handle::wal_start_commit`/`wal_log_write`'s docs) — an
/// append-only file has no way to retroactively attach `ts_commit` to bytes
/// already flushed, so a small separate marker is appended instead, once
/// `commit_tx` actually succeeds. Replay (`bat_wal::recovery::replay`) is
/// commit-gated: a `Write` is only ever replayed once a matching `Commit`
/// (same `worker_id`/`ts_start`) is found for it — a transaction that
/// aborts, or that a crash catches before it commits, simply never gets
/// one, so its writes are silently skipped. No separate abort/invalidate
/// record is needed for that at all.
pub enum WalEntry<Key: Ord + Copy + Hash + Display, Payload: Clone> {
    Write(WalRecord<Key, Payload>),
    Commit { stamp: TxStamp, ts_commit: Version },
}

pub fn entry_size_hint<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
    entry: &WalEntry<Key, Payload>,
) -> usize {
    const HEADER_AND_FRAME: usize = 1 + 8 + 2 + 4 + 4;
    let payload_hint = match entry {
        WalEntry::Write(WalRecord {
            op: CRUDOperation::Insert(_, p) | CRUDOperation::Update(_, p),
            ..
        }) => p.wal_encode_size_hint(),
        _ => 0,
    };
    HEADER_AND_FRAME + size_of::<Key>() + payload_hint
}

pub fn encode_entry<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
    entry: &WalEntry<Key, Payload>,
    out: &mut Vec<u8>,
) {
    match entry {
        WalEntry::Write(record) => encode(record, out),
        WalEntry::Commit { stamp, ts_commit } => {
            out.push(TAG_COMMIT);
            out.extend_from_slice(&stamp.ts_start().to_le_bytes());
            out.extend_from_slice(&stamp.worker_id().to_le_bytes());
            out.extend_from_slice(&ts_commit.to_le_bytes());
        }
    }
}

/// Decodes an entry body produced by [`encode_entry`]. `None` on the same
/// conditions as `decode` (too short / unrecognized tag / bad payload).
pub fn decode_entry<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
    bytes: &[u8],
) -> Option<WalEntry<Key, Payload>> {
    let header_sz = 1 + 8 + 2;

    if bytes.first().copied() == Some(TAG_COMMIT) {
        if bytes.len() < header_sz + 8 {
            return None;
        }
        let ts_start = Version::from_le_bytes(bytes[1..9].try_into().ok()?);
        let worker_id = WorkerId::from_le_bytes(bytes[9..11].try_into().ok()?);
        let ts_commit = Version::from_le_bytes(bytes[header_sz..header_sz + 8].try_into().ok()?);
        return Some(WalEntry::Commit {
            stamp: TxStamp::new(worker_id, ts_start),
            ts_commit,
        });
    }

    decode(bytes).map(WalEntry::Write)
}

/// Identifies which table a WAL entry belongs to in a `Database`'s single
/// shared log file (see `bat_db::database::table_id`) — a stable hash of the
/// table's name, not an insertion-order counter, so it's self-describing
/// across process restarts with no separate persisted catalog needed.
pub type TableId = u32;

/// Reserved `TableId` carried by a table-tagged `Commit` entry. A commit
/// marker is transaction-scoped, not table-scoped — one shared file needs
/// exactly one marker per transaction to gate replay of every table it
/// touched (see `WalEntry::Commit`'s doc) — but the id slot stays physically
/// present in the frame so every entry in the file parses with the same
/// uniform shape; `bat_wal::recovery::replay_database` ignores this value.
/// `bat_db::database::table_id` guarantees it never returns this sentinel for
/// a real table name.
pub const TABLE_ID_COMMIT_SENTINEL: TableId = TableId::MAX;

/// Table-tagged counterpart to `encode_entry`, for a `Database`'s single
/// shared log file: `[u32 table_id][entry body, same as encode_entry]` — no
/// length prefix, no checksum (added by `frame`, same as `encode_entry`).
pub fn encode_entry_for_table<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
    table_id: TableId,
    entry: &WalEntry<Key, Payload>,
    out: &mut Vec<u8>,
) {
    out.extend_from_slice(&table_id.to_le_bytes());
    encode_entry(entry, out);
}

/// Inverse of `encode_entry_for_table`. `None` on the same conditions as
/// `decode_entry` (too short / unrecognized tag / bad payload), plus if
/// fewer than 4 bytes are present for the table id itself.
pub fn decode_entry_for_table<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
    bytes: &[u8],
) -> Option<(TableId, WalEntry<Key, Payload>)> {
    if bytes.len() < 4 {
        return None;
    }
    let table_id = TableId::from_le_bytes(bytes[0..4].try_into().ok()?);
    let entry = decode_entry(&bytes[4..])?;
    Some((table_id, entry))
}

pub fn encode_entry_for_table_framed<
    Key: Ord + Copy + Hash + Display,
    Payload: Clone + WalPayload,
>(
    table_id: TableId,
    entry: &WalEntry<Key, Payload>,
    out: &mut Vec<u8>,
) {
    let start = out.len();
    out.extend_from_slice(&0u32.to_le_bytes());
    encode_entry_for_table(table_id, entry, out);

    let body_len = (out.len() - start - 4) as u32;
    out[start..start + 4].copy_from_slice(&body_len.to_le_bytes());

    let crc = crc32(&out[start + 4..]);
    out.extend_from_slice(&crc.to_le_bytes());
}

/// Every possible byte's contribution to the running CRC, precomputed once
/// at compile time (same reflected IEEE-802.3 polynomial, `0xEDB8_8320`, the
/// bit-loop below used to apply 8 shift-xor steps at a time per byte) — see
/// `crc32`'s doc for why this table exists instead of just that loop.
const CRC32_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut byte = 0usize;
    while byte < 256 {
        let mut crc = byte as u32;
        let mut _bit = 0;
        while _bit < 8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            _bit += 1;
        }
        table[byte] = crc;
        byte += 1;
    }
    table
};

pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in bytes {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[idx];
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

pub fn encode_entry_framed<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
    entry: &WalEntry<Key, Payload>,
    out: &mut Vec<u8>,
) {
    let start = out.len();
    out.extend_from_slice(&0u32.to_le_bytes());
    encode_entry(entry, out);

    let body_len = (out.len() - start - 4) as u32;
    out[start..start + 4].copy_from_slice(&body_len.to_le_bytes());

    let crc = crc32(&out[start + 4..]);
    out.extend_from_slice(&crc.to_le_bytes());
}

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

pub fn resync_next<T>(bytes: &[u8], decode: impl Fn(&[u8]) -> Option<T>) -> Option<(T, usize)> {
    let mut skip = 0usize;
    while skip < bytes.len() {
        if let Some((body, consumed)) = read_frame(&bytes[skip..]) {
            if !body.is_empty() {
                if let Some(parsed) = decode(body) {
                    return Some((parsed, skip + consumed));
                }
            }
        }
        skip += 1;
    }
    None
}

#[cfg(test)]
mod crc32_tests {
    use super::crc32;

    #[test]
    fn matches_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn empty_input() {
        assert_eq!(crc32(b""), 0x0000_0000);
    }
}
