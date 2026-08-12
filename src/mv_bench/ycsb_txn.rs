//! YCSB's five Core Workload operations (Cooper et al., SoCC 2010, §3):
//! Read, Update, Insert, Scan and Read-Modify-Write. Each is dispatched as
//! one (or, for read-modify-write, two sequential) plain `CRUDOperation`
//! calls through `AtomicTxDispatcher::dispatch_crud` — YCSB ops are single-
//! key (Read-Modify-Write included: real YCSB backends run it as a plain
//! read call followed by a plain write call, timed together as one logical
//! operation, not as one multi-statement DB transaction), so there's no need
//! for the heavier multi-op `mv_db::transaction::DbTransaction` that
//! `tpcc_txn` uses to span several tables atomically in one snapshot.
//!
//! Point/range reads use `CRUDOperation::PointSi`/`RangeSi` ("read the
//! current snapshot") rather than a snapshot held open across several
//! operations — each op is its own atomic unit, matching TPC-C's read-only
//! Order-Status/Stock-Level except without needing multiple reads to share
//! one snapshot. `*Si` draws its version internally, gap-free (see
//! `mv_query::dispatch`'s docs on those variants) — unlike calling
//! `tree.current_version()` here and passing it to `Point`/`Range`, which
//! would leave a window between that read and this module's dispatch call
//! where a concurrent GC decision can't yet see this read and could reclaim
//! a page it needs.

use crate::mv_bench::ycsb_random::{random_field_patch, random_row};
use crate::mv_bench::ycsb_schema::{YcsbConfig, YcsbKey, YcsbTree};
use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_query::interval::Interval;

/// Point read of the freshest committed version. Returns whether the row
/// was found (a miss can only happen for a key beyond the currently-inserted
/// range, e.g. a `Latest`-distribution read racing just ahead of a fresh
/// `Insert`'s counter bump).
pub fn read(tree: &YcsbTree, key: YcsbKey) -> bool { read_with_mode(tree, key, true) }

pub fn read_with_mode(tree: &YcsbTree, key: YcsbKey, read_payload: bool) -> bool {
    if !read_payload {
        return tree.point_exists_si(key);
    }
    match tree.dispatch_crud(CRUDOperation::PointSi(key)) {
        CRUDOperationResult::MatchedRecords(rows) => {
            if let Some(row) = rows.first() {
                // Consume the complete logical value. `black_box` prevents an optimizing
                // build from reducing a YCSB read back to an existence check.
                let checksum = row.payload.as_bytes().iter().fold(0u8, |a, b| a.wrapping_add(*b));
                std::hint::black_box(checksum);
                true
            } else {
                false
            }
        }
        CRUDOperationResult::ZeroAffected(_) => false,
        other => panic!("ycsb read: unexpected result: {other}"),
    }
}

/// Updates a row using YCSB's `writeallfields` policy. When it is `true`, a
/// fresh complete row is generated. The standard/default `false` changes one
/// randomly selected field while preserving all other fields. The latter is
/// applied atomically under the leaf write latch, so concurrent updates of
/// different fields cannot overwrite each other's already-committed bytes.
/// Returns `false` if the key does not currently exist.
pub fn update(tree: &YcsbTree, cfg: &YcsbConfig, key: YcsbKey, write_all_fields: bool) -> bool {
    let replacement = if write_all_fields {
        Some(random_row(cfg))
    } else {
        None
    };
    let result = if let Some(replacement) = replacement {
        tree.dispatch_crud(CRUDOperation::Update(key, replacement))
    } else if cfg.field_count == 0 || cfg.field_length == 0 {
        return tree.point_exists_si(key);
    } else {
        let (field, bytes) = random_field_patch(cfg);
        tree.update_with(key, |old| {
            old.copy_with_field(field, cfg.field_length, &bytes)
        })
    };
    match result {
        CRUDOperationResult::Updated(_) => true,
        CRUDOperationResult::ZeroAffected(_) => false,
        other => panic!("ycsb update: unexpected result: {other}"),
    }
}

/// Inserts a brand-new row at `key` (expected to be beyond every key handed
/// out so far — see the driver's key-minting counter).
pub fn insert(tree: &YcsbTree, cfg: &YcsbConfig, key: YcsbKey) {
    match tree.dispatch_crud(CRUDOperation::Insert(key, random_row(cfg))) {
        CRUDOperationResult::Inserted(_) => {}
        other => panic!("ycsb insert: unexpected result: {other}"),
    }
}

/// Range scan of `len` rows starting at `start_key` (YCSB "scan a range of
/// records"), against the freshest committed version. Returns the number of
/// rows actually found (can be `< len` near the end of the loaded key range).
///
/// Goes via `RangeIterSi` and its zero-copy `count_ref` terminal operation:
/// no result vector, `RecordPointResult`, or payload-handle clone is needed
/// for YCSB's count-only scan result.
pub fn scan(tree: &YcsbTree, start_key: YcsbKey, len: u64) -> usize {
    scan_with_mode(tree, start_key, len, true)
}

pub fn scan_with_mode(tree: &YcsbTree, start_key: YcsbKey, len: u64, read_payload: bool) -> usize {
    let hi = start_key.saturating_add(len.saturating_sub(1));
    match tree.dispatch_crud(CRUDOperation::RangeIterSi(Interval::new(start_key, hi))) {
        CRUDOperationResult::MatchedRecordIter(iter) => if read_payload {
            let (count, checksum) = iter.fold_ref((0usize, 0u8), |(count, checksum), _, payload| {
                let checksum = payload.as_bytes().iter().fold(checksum, |a, b| a.wrapping_add(*b));
                (count + 1, checksum)
            });
            std::hint::black_box(checksum);
            count
        } else {
            iter.count_ref()
        },
        other => panic!("ycsb scan: unexpected result: {other}"),
    }
}

/// Reads then unconditionally rewrites `key` (YCSB "read a record, modify
/// it, write it back") as two sequential ops — see module docs for why this
/// isn't wrapped in a multi-op `Transaction`. Returns whether the write half
/// found the row (the read half's outcome isn't separately observable here,
/// same as real YCSB clients which don't act on the read's content either).
pub fn read_modify_write(
    tree: &YcsbTree,
    cfg: &YcsbConfig,
    key: YcsbKey,
    write_all_fields: bool,
) -> bool {
    read_modify_write_with_mode(tree, cfg, key, write_all_fields, true)
}

pub fn read_modify_write_with_mode(
    tree: &YcsbTree, cfg: &YcsbConfig, key: YcsbKey, write_all_fields: bool, read_payload: bool,
) -> bool {
    let _ = read_with_mode(tree, key, read_payload);
    update(tree, cfg, key, write_all_fields)
}
