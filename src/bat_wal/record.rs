use std::cmp::Ord;
use std::fmt::Display;
use std::hash::Hash;
use std::mem::size_of;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::Version;

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

    /// Upper-bound-ish estimate of how many bytes `wal_encode` is about to
    /// write, used only to pre-size the framing `Vec` (see
    /// `entry_size_hint`/`WalWriter::log_with_stamp`) so encoding a real
    /// payload doesn't pay for repeated grow-and-copy reallocations along
    /// the way (`Vec::extend_from_slice` on an undersized buffer). Getting
    /// this wrong costs at worst one extra reallocation, never correctness
    /// — `wal_encode` remains the sole source of truth for what's actually
    /// written. Default of `8` matches the base `u64` payload exactly;
    /// override for anything bigger (see `TpccRow`/`YcsbRow`'s impls).
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
/// Panics if `record.op` isn't `Insert`/`Update`/`Delete` — nothing else is
/// ever handed to this function: the dispatch layer only calls
/// `wal_start_commit`/`wal_log_write` for those three, and read/`*Rand`
/// operations are never logged at all.
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

/// Estimated total framed size of `entry` — `tag(1) + ts_start(8) +
/// worker_id(2) + Key(size_of::<Key>()) + frame overhead(4 len + 4 crc)`,
/// plus the payload's own `wal_encode_size_hint` for a `Write` entry backed
/// by `Insert`/`Update` (an entry with no payload — `Delete`/`Commit` —
/// adds none). Callers that pre-size their encoding buffer with
/// `Vec::with_capacity(entry_size_hint(entry))` avoid the repeated
/// grow-and-copy `encode_entry_framed`/`encode_entry_for_table_framed`
/// would otherwise pay for a payload much bigger than a fixed small guess
/// (real payloads like `TpccRow`/`YcsbRow` routinely run into the hundreds
/// of bytes, not the ~20-30 bytes a `u64`-payload record needs).
pub fn entry_size_hint<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
    entry: &WalEntry<Key, Payload>,
) -> usize {
    const HEADER_AND_FRAME: usize = 1 + 8 + 2 + 4 + 4;
    let payload_hint = match entry {
        WalEntry::Write(WalRecord { op: CRUDOperation::Insert(_, p) | CRUDOperation::Update(_, p), .. }) => {
            p.wal_encode_size_hint()
        }
        _ => 0,
    };
    HEADER_AND_FRAME + size_of::<Key>() + payload_hint
}

/// Encodes one [`WalEntry`]'s body (no length prefix, no checksum). A
/// `Write` entry is exactly `encode`'s layout; a `Commit` entry is
/// `[u8 tag=TAG_COMMIT][u64 ts_start][u16 worker_id][u64 ts_commit]` — the
/// same header as a `Write` entry, with `ts_commit` in place of a
/// key/payload.
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
        return Some(WalEntry::Commit { stamp: TxStamp::new(worker_id, ts_start), ts_commit });
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

/// Table-tagged counterpart to `encode_entry_framed`: same single-buffer,
/// placeholder-patched framing (`[u32 len][table_id][entry body][u32 crc32]`),
/// just with `table_id` folded into the framed body. Used by
/// `WalWriter::log_with_stamp_for_table`/`log_commit_for_table`.
pub fn encode_entry_for_table_framed<Key: Ord + Copy + Hash + Display, Payload: Clone + WalPayload>(
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

/// Standard (IEEE) CRC-32, implemented by hand (table-driven — see
/// `CRC32_TABLE`) to avoid pulling in a dependency and to stay stable across
/// toolchains/versions (unlike e.g. `DefaultHasher`, whose algorithm is
/// explicitly not guaranteed stable). Same output as the straightforward
/// byte-at-a-time bit-loop this replaced: profiling a write-heavy workload
/// found that loop (8 branchy shift-xor steps per byte, inlined into every
/// call site via `encode_entry_framed`) costing over 20% of total CPU time,
/// since it runs on every single WAL record's body. One table lookup per
/// byte instead of 8 shift-xor steps is the standard fix.
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

/// Same wire format as `encode_entry` followed by `frame`
/// (`[u32 len][body][u32 crc32(body)]`), but encodes the body straight into
/// `out` instead of into a separate buffer first — one allocation instead
/// of two, and no copy of the body into a second buffer. The length prefix
/// is written as a placeholder, then patched once the body's actual length
/// is known. Used by `WalWriter::log_with_stamp`/`log_commit`, each of
/// which frames exactly one entry per call and has no other reason to keep
/// the body separate.
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

/// `read_frame` + `decode`, tolerant of an **interior** hole rather than
/// only a torn *tail*: scans forward from `bytes[0]` a byte at a time until
/// it finds a position where a frame parses *and* `decode` accepts its
/// body, returning the decoded value and the total bytes consumed from
/// `bytes[0]` (including whatever was skipped to get there). `None` once
/// the scan runs off the end of `bytes` with nothing found — a genuine
/// torn tail, same as `read_frame` returning `None` right away used to
/// mean for the strictly-sequential writer this module was originally
/// written for.
///
/// # Why this exists
/// `bat_wal::writer::WalWriter` appends strictly in the order its one
/// background thread drains its channel, so there `read_frame` returning
/// `None` can *only* mean "this is where a crash cut off the tail" —
/// stopping the scan right there (what every `recovery::replay*` used to
/// do) is exactly correct. `bat_wal::lockfree_writer::LockFreeWalWriter`
/// breaks that assumption: concurrent writers reserve disjoint byte ranges
/// via `fetch_add` but can *complete* out of order, so a thread that
/// reserved a low offset and then died (the whole process crashing, not
/// just that thread stalling) before its `pwrite` landed leaves a hole of
/// unwritten (zero) bytes with valid, durable records on *both* sides of
/// it — see that type's doc for the full argument. A plain "stop at the
/// first bad frame" scan would silently discard every record after such a
/// hole, even though they really did reach disk.
///
/// # Why a byte-at-a-time scan is safe here
/// A real record's body is never empty (every real `encode`/`encode_entry`
/// output is at least 19 bytes — `1` tag `+ 8` ts_start `+ 2` worker_id
/// `+` at least a `Key`/`ts_commit`'s worth more), so `len == 0` can only
/// come from a hole's zero bytes, never genuine data. Since a hole is
/// always *some enqueue call's entire reserved range* (`tail.fetch_add`
/// hands out one call's whole framed length atomically — never a partial
/// record from two different calls), the byte immediately after a hole is
/// always the true, aligned start of the next real frame. So advancing one
/// byte at a time through anything that doesn't parse-and-decode is
/// guaranteed to land exactly there, however long the hole is and whatever
/// it's misaligned against (no assumption that a hole's length is a
/// multiple of anything). The one residual risk — some misaligned window
/// *inside* a hole coincidentally produces a length that fits the
/// remaining bytes *and* whose CRC32 happens to match *and* whose decoded
/// body looks superficially valid — is the same class of (astronomically
/// unlikely, ~1-in-4-billion-per-candidate-position) risk `read_frame`'s
/// CRC32 already accepts as "good enough to catch a torn write, not a
/// cryptographic guarantee"; this doesn't introduce a new kind of risk,
/// just more chances (one per skipped byte) to hit the existing one.
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

    /// The standard CRC-32/ISO-HDLC check value for the ASCII string
    /// "123456789" — the reference test vector every implementation of this
    /// polynomial is checked against. Pins the table-driven implementation
    /// to the exact same algorithm the byte-at-a-time bit-loop it replaced
    /// computed.
    #[test]
    fn matches_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn empty_input() {
        assert_eq!(crc32(b""), 0x0000_0000);
    }
}

