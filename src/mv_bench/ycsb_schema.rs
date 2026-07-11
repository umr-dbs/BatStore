//! YCSB-style key-value schema for the cMVBT tree (Cooper et al., "Benchmarking
//! Cloud Serving Systems with YCSB", SoCC 2010). Unlike TPC-C's multi-table
//! schema (`tpcc_schema`), YCSB has exactly one table ("usertable"): a flat
//! key -> N-field row, so no key tagging/encoding scheme is needed — the raw
//! `u64` primary key *is* the tree key.
//!
//! Every op in `ycsb_txn` is a single `CRUDOperation` dispatched straight
//! through `AtomicTxDispatcher::dispatch_crud` (see that module's docs for
//! why a multi-op `mv_query::transaction::Transaction`, as used by TPC-C to
//! span several tables atomically, isn't needed here).

use std::fmt::{Display, Formatter};

use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_wal::record::WalPayload;

pub type YcsbKey = u64;

/// Reuses the base tree's page-capacity constants: `YcsbRow` is a `Vec<String>`
/// (24 bytes inline, same ballpark as a boxed pointer), so there's no reason
/// to retune fan-out/page capacity the way `tpcc_schema` does for its larger
/// inline row payloads.
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

/// One "usertable" row: `field_count` opaque string fields (YCSB `field0..fieldN`).
/// Every op replaces/reads the row as a whole (see `ycsb_txn` module docs) —
/// there's no partial-field update at the storage layer, same simplification
/// `tpcc_schema::TpccRow` makes for its own read-modify-write fields.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct YcsbRow {
    pub fields: Vec<String>,
}

impl Display for YcsbRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "YcsbRow(fields={})", self.fields.len())
    }
}

/// Variable-length encoding (`YcsbRow` owns heap data via `String`), same
/// reasoning as `tpcc_wal_codec`'s impl for `TpccRow`: `write_raw`/`read_raw`
/// (raw memcpy) is unsound the moment heap ownership is involved.
impl WalPayload for YcsbRow {
    fn wal_encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.fields.len() as u32).to_le_bytes());
        for field in &self.fields {
            out.extend_from_slice(&(field.len() as u32).to_le_bytes());
            out.extend_from_slice(field.as_bytes());
        }
    }

    fn wal_decode(bytes: &[u8]) -> Option<Self> {
        fn read_u32(bytes: &[u8], pos: &mut usize) -> Option<u32> {
            let s = bytes.get(*pos..*pos + 4)?;
            *pos += 4;
            Some(u32::from_le_bytes(s.try_into().ok()?))
        }

        let mut pos = 0usize;
        let count = read_u32(bytes, &mut pos)? as usize;
        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            let len = read_u32(bytes, &mut pos)? as usize;
            let s = bytes.get(pos..pos + len)?;
            pos += len;
            fields.push(String::from_utf8(s.to_vec()).ok()?);
        }
        Some(Self { fields })
    }
}
