//! YCSB-style key-value schema for the cMVBT tree (Cooper et al., "Benchmarking
//! Cloud Serving Systems with YCSB", SoCC 2010). Unlike TPC-C's multi-table
//! schema (`tpcc_schema`), YCSB has exactly one table ("usertable"): a flat
//! key -> N-field row, so no key tagging/encoding scheme is needed — the raw
//! `u64` primary key *is* the tree key.
//!
//! Every op in `ycsb_txn` is a single `CRUDOperation` dispatched straight
//! through `AtomicTxDispatcher::dispatch_crud` (see that module's docs for
//! why a multi-op `mv_db::transaction::DbTransaction`, as used by TPC-C to
//! span several tables atomically, isn't needed here).

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::fmt::{Debug, Display, Formatter};
use std::mem::size_of;
use std::ptr::NonNull;

use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_wal::record::WalPayload;

pub type YcsbKey = u64;

/// Reuses the base tree's page-capacity constants outright, not just as a
/// starting point: `RecordPoint::payload` is a `PayloadSlot<Payload>`, always
/// exactly one `usize` (8B) wide - it inlines `Payload` bitwise only when
/// `Payload` is itself exactly `usize`-sized/aligned, and otherwise
/// heap-boxes it behind that one word. `YcsbRow` is deliberately shaped to
/// take the inline path (see its own doc), same as the base tree's `u64`, so
/// `RecordPoint<YcsbKey, YcsbRow>` is 32B - identical to `RecordPoint<u64,
/// u64>` - and the base tree's `NUM_RECORDS` (125) already fills the leaf's
/// 4KB record-array budget exactly. See the same reasoning spelled out in
/// full in `tpcc_schema::TPCC_NUM_RECORDS`'s doc, which reuses this same
/// value for a related but distinct reason (boxed, not inlined - see there).
pub const YCSB_FAN_OUT: usize = crate::mv_tree::mvbt::FAN_OUT;
pub const YCSB_NUM_RECORDS: usize = crate::mv_tree::mvbt::NUM_RECORDS;

pub type YcsbTree = MVBTSt<YCSB_FAN_OUT, YCSB_NUM_RECORDS, YcsbKey, YcsbRow>;

/// Database scale/shape, mirroring YCSB's `recordcount`/`fieldcount`/
/// `fieldlength` workload properties.
#[derive(Clone, Copy, Debug)]
pub struct YcsbConfig {
    /// Number of rows loaded before the timed run starts (YCSB `recordcount`).
    pub record_count: u64,
    /// Fields per row (YCSB `fieldcount`, default 10).
    pub field_count: usize,
    /// Bytes per field (YCSB `fieldlength`, default 100).
    pub field_length: usize,
}

impl Default for YcsbConfig {
    fn default() -> Self {
        Self { record_count: 1_000_000, field_count: 10, field_length: 100 }
    }
}

/// One "usertable" row: `field_count` fixed-length opaque byte fields (YCSB
/// `field0..fieldN`), flattened into a single contiguous heap allocation.
/// Every field is exactly `field_length` bytes wide (`YcsbConfig`) and never
/// changes after the row is created, so no offset table is needed - field
/// `i` is simply `bytes[i*field_length..(i+1)*field_length]`.
///
/// Unlike a `Box<[u8]>` (a 16-byte fat pointer: data pointer + length),
/// `YcsbRow` owns its buffer through a single *thin* (8-byte) pointer, with
/// the length stored as a `u32` header at the start of the same allocation
/// (`[len: u32][bytes...]`) rather than in pointer metadata. That's the
/// difference that matters to `RecordPoint::payload`'s `PayloadSlot<Payload>`
/// (`record_point.rs`): it only bit-inlines a `Payload` that's itself exactly
/// `usize`-sized/aligned, and boxes anything else behind one more word. A
/// fat-pointer-shaped row would get boxed *again* on top of its own `Box<[u8]>`
/// - two allocations and two pointer-chases per row. This thin-pointer shape
/// takes the inline path instead: one allocation, one pointer-chase, same as
/// the base tree's plain `u64` payload. `Drop`/`Clone` recover the buffer's
/// length from its own header, so no external config is needed to free or
/// duplicate a row.
///
/// Every op replaces/reads the row as a whole (see `ycsb_txn` module docs) —
/// there's no partial-field update at the storage layer, same simplification
/// `tpcc_schema::TpccRow` makes for its own read-modify-write fields.
pub struct YcsbRow {
    /// Points at a `[u32 len][u8; len]` allocation, aligned to `HEADER_LEN`
    /// so the header can be read/written without unaligned-access helpers.
    ptr: NonNull<u8>,
}

// Sound exactly like `Box<[u8]>`: `YcsbRow` uniquely owns its heap
// allocation (no aliasing, see the type's doc), so it's safe to move/share
// a reference across threads the same way `Box`'s own blanket impls are.
unsafe impl Send for YcsbRow {}
unsafe impl Sync for YcsbRow {}

const HEADER_LEN: usize = size_of::<u32>();

impl YcsbRow {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let layout = Self::layout_for(bytes.len());
        unsafe {
            let raw = alloc(layout);
            if raw.is_null() {
                handle_alloc_error(layout);
            }
            raw.cast::<u32>().write(bytes.len() as u32);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), raw.add(HEADER_LEN), bytes.len());
            Self { ptr: NonNull::new_unchecked(raw) }
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().add(HEADER_LEN), self.len()) }
    }

    fn len(&self) -> usize {
        unsafe { self.ptr.as_ptr().cast::<u32>().read() as usize }
    }

    /// Aligned to `HEADER_LEN` (4B) so the `u32` header is never a misaligned
    /// read/write - the allocator hands back a suitably-aligned pointer for
    /// whatever `Layout` we ask for.
    fn layout_for(data_len: usize) -> Layout {
        Layout::from_size_align(HEADER_LEN + data_len, HEADER_LEN)
            .expect("YcsbRow: row too large to allocate")
    }
}

// The entire point of this type's shape: if a future edit adds a field and
// pushes `YcsbRow` past one `usize` in size (or past `usize`'s alignment),
// `PayloadSlot<YcsbRow>` (`record_point.rs`) silently falls back to boxing
// it - still correct, but quietly reintroducing the extra allocation this
// type exists to avoid. Fail the build instead of failing silently.
const _: () = assert!(
    size_of::<YcsbRow>() == size_of::<usize>()
        && std::mem::align_of::<YcsbRow>() <= std::mem::align_of::<usize>(),
    "YcsbRow must stay usize-sized/aligned so PayloadSlot bit-inlines it instead of boxing it",
);

impl Drop for YcsbRow {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), Self::layout_for(self.len())); }
    }
}

impl Clone for YcsbRow {
    fn clone(&self) -> Self {
        Self::from_bytes(self.as_bytes())
    }
}

impl Default for YcsbRow {
    fn default() -> Self {
        Self::from_bytes(&[])
    }
}

impl PartialEq for YcsbRow {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Debug for YcsbRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "YcsbRow(bytes={})", self.len())
    }
}

impl Display for YcsbRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "YcsbRow(bytes={})", self.len())
    }
}

/// Length-prefixed raw bytes: `YcsbRow` is already stored this way on the
/// heap, so WAL encoding is just a memcpy of the same layout its own buffer
/// already uses - decoding hands the bytes straight to `from_bytes`.
impl WalPayload for YcsbRow {
    fn wal_encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.len() as u32).to_le_bytes());
        out.extend_from_slice(self.as_bytes());
    }

    fn wal_decode(bytes: &[u8]) -> Option<Self> {
        let len = u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?) as usize;
        let data = bytes.get(4..4 + len)?;
        Some(Self::from_bytes(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_bytes() {
        let row = YcsbRow::from_bytes(b"hello world");
        assert_eq!(row.as_bytes(), b"hello world");
    }

    #[test]
    fn empty_row_is_sound() {
        let row = YcsbRow::default();
        assert_eq!(row.as_bytes(), b"");
    }

    #[test]
    fn clone_is_a_deep_copy() {
        let a = YcsbRow::from_bytes(b"field0field1field2");
        let b = a.clone();
        assert_eq!(a, b);
        assert_ne!(a.as_bytes().as_ptr(), b.as_bytes().as_ptr());
    }

    #[test]
    fn wal_round_trip() {
        let row = YcsbRow::from_bytes(&vec![7u8; 1_000]);
        let mut buf = Vec::new();
        row.wal_encode(&mut buf);
        let decoded = YcsbRow::wal_decode(&buf).expect("decode");
        assert_eq!(row, decoded);
    }
}
