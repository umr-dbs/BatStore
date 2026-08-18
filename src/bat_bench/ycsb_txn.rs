//! YCSB's five Core Workload operations (Cooper et al., SoCC 2010, §3):
//! Read, Update, Insert, Scan and Read-Modify-Write. Each is dispatched as
//! one (or, for read-modify-write, two sequential) plain `CRUDOperation`
//! calls through `AtomicTxDispatcher::dispatch_crud` in the default
//! `atomic` mode — YCSB ops are single-
//! key (Read-Modify-Write included: real YCSB backends run it as a plain
//! read call followed by a plain write call, timed together as one logical
//! operation, not as one multi-statement DB transaction), so there's no need
//! for the heavier multi-op `bat_db::transaction::DbTransaction` that
//! `tpcc_txn` uses to span several tables atomically in one snapshot.
//! The configurable `transaction` mode is a controlled comparison using
//! ordinary snapshot registration, visibility/conflict checks, retry, and
//! commit for each operation. Its Read-Modify-Write keeps both halves under
//! one registered snapshot.
//!
//! Point/range reads use `CRUDOperation::PointSi`/`RangeSi` ("read the
//! current snapshot") rather than a snapshot held open across several
//! operations — each op is its own atomic unit, matching TPC-C's read-only
//! Order-Status/Stock-Level except without needing multiple reads to share
//! one snapshot. `*Si` draws its version internally, gap-free (see
//! `bat_query::dispatch`'s docs on those variants) — unlike calling
//! `tree.current_version()` here and passing it to `Point`/`Range`, which
//! would leave a window between that read and this module's dispatch call
//! where a concurrent GC decision can't yet see this read and could reclaim
//! a page it needs.

use crate::bat_bench::ycsb_random::{random_field_patch, random_row};
use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbKey, YcsbTree};
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_db::transaction::{insert_on_tree, point_on_tree, update_on_tree};
use crate::bat_query::interval::Interval;
use crate::bat_record_model::tx_stamp::TxStamp;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum YcsbExecutionMode {
    /// Specialized one-operation path: commit before publishing the leaf.
    Atomic,
    /// Ordinary registered transaction lifecycle, including write-set-style
    /// conflict semantics and commit after the operation has been applied.
    Transaction,
}

fn own_write_result(
    result: CRUDOperationResult<
        '_,
        { crate::bat_bench::ycsb_schema::YCSB_FAN_OUT },
        { crate::bat_bench::ycsb_schema::YCSB_NUM_RECORDS },
        YcsbKey,
        crate::bat_bench::ycsb_schema::YcsbRow,
    >,
) -> CRUDOperationResult<
    'static,
    { crate::bat_bench::ycsb_schema::YCSB_FAN_OUT },
    { crate::bat_bench::ycsb_schema::YCSB_NUM_RECORDS },
    YcsbKey,
    crate::bat_bench::ycsb_schema::YcsbRow,
> {
    match result {
        CRUDOperationResult::Inserted(v) => CRUDOperationResult::Inserted(v),
        CRUDOperationResult::Updated(v) => CRUDOperationResult::Updated(v),
        CRUDOperationResult::Deleted(v) => CRUDOperationResult::Deleted(v),
        CRUDOperationResult::ZeroAffected(reason) => CRUDOperationResult::ZeroAffected(reason),
        CRUDOperationResult::Conflict => CRUDOperationResult::Conflict,
        CRUDOperationResult::Error => CRUDOperationResult::Error,
        _ => panic!("YCSB write path returned a read result"),
    }
}

fn transactional_update(
    tree: &YcsbTree,
    key: YcsbKey,
    payload: crate::bat_bench::ycsb_schema::YcsbRow,
) -> CRUDOperationResult<
    'static,
    { crate::bat_bench::ycsb_schema::YCSB_FAN_OUT },
    { crate::bat_bench::ycsb_schema::YCSB_NUM_RECORDS },
    YcsbKey,
    crate::bat_bench::ycsb_schema::YcsbRow,
> {
    let worker = tree.worker_id();
    let ts_start = tree.begin_snapshot();
    let (result, wrote) = update_on_tree(tree, worker, ts_start, key, payload);
    if wrote {
        let ts_commit = tree.commit_tx(worker);
        tree.wal_log_commit(TxStamp::new(worker, ts_start), ts_commit);
    }
    tree.end_snapshot(ts_start);
    result
}

fn transactional_insert(
    tree: &YcsbTree,
    key: YcsbKey,
    payload: crate::bat_bench::ycsb_schema::YcsbRow,
) -> CRUDOperationResult<
    'static,
    { crate::bat_bench::ycsb_schema::YCSB_FAN_OUT },
    { crate::bat_bench::ycsb_schema::YCSB_NUM_RECORDS },
    YcsbKey,
    crate::bat_bench::ycsb_schema::YcsbRow,
> {
    let worker = tree.worker_id();
    let ts_start = tree.begin_snapshot();
    let (result, wrote) = insert_on_tree(tree, worker, ts_start, key, payload);
    if wrote {
        let ts_commit = tree.commit_tx(worker);
        tree.wal_log_commit(TxStamp::new(worker, ts_start), ts_commit);
    }
    tree.end_snapshot(ts_start);
    result
}

/// Point read of the freshest committed version. Returns whether the row
/// was found (a miss can only happen for a key beyond the currently-inserted
/// range, e.g. a `Latest`-distribution read racing just ahead of a fresh
/// `Insert`'s counter bump).
pub fn read(tree: &YcsbTree, key: YcsbKey) -> bool {
    read_with_mode(tree, key, true)
}

pub fn read_with_mode(tree: &YcsbTree, key: YcsbKey, read_payload: bool) -> bool {
    if !read_payload {
        return tree.point_exists_si(key);
    }
    match tree.dispatch_crud(CRUDOperation::PointSi(key)) {
        CRUDOperationResult::MatchedRecords(rows) => {
            if let Some(row) = rows.first() {
                // Consume the complete logical value. `black_box` prevents an optimizing
                // build from reducing a YCSB read back to an existence check.
                let checksum = row
                    .payload
                    .as_bytes()
                    .iter()
                    .fold(0u8, |a, b| a.wrapping_add(*b));
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
    update_with_execution_mode(tree, cfg, key, write_all_fields, YcsbExecutionMode::Atomic)
}

pub fn update_with_execution_mode(
    tree: &YcsbTree,
    cfg: &YcsbConfig,
    key: YcsbKey,
    write_all_fields: bool,
    mode: YcsbExecutionMode,
) -> bool {
    loop {
        match update_once(tree, cfg, key, write_all_fields, mode) {
            CRUDOperationResult::Updated(_) => return true,
            CRUDOperationResult::ZeroAffected(_) => return false,
            CRUDOperationResult::Conflict if mode == YcsbExecutionMode::Transaction => {
                std::hint::spin_loop();
            }
            other => panic!("ycsb update: unexpected result: {other}"),
        }
    }
}

fn update_once(
    tree: &YcsbTree,
    cfg: &YcsbConfig,
    key: YcsbKey,
    write_all_fields: bool,
    mode: YcsbExecutionMode,
) -> CRUDOperationResult<
    'static,
    { crate::bat_bench::ycsb_schema::YCSB_FAN_OUT },
    { crate::bat_bench::ycsb_schema::YCSB_NUM_RECORDS },
    YcsbKey,
    crate::bat_bench::ycsb_schema::YcsbRow,
> {
    let replacement = if write_all_fields {
        Some(random_row(cfg))
    } else {
        None
    };
    let result = if let Some(replacement) = replacement {
        match mode {
            YcsbExecutionMode::Atomic => {
                own_write_result(tree.dispatch_crud(CRUDOperation::Update(key, replacement)))
            }
            YcsbExecutionMode::Transaction => transactional_update(tree, key, replacement),
        }
    } else if cfg.field_count == 0 || cfg.field_length == 0 {
        return if tree.point_exists_si(key) {
            CRUDOperationResult::Updated(tree.current_version())
        } else {
            CRUDOperationResult::ZeroAffected(
                crate::bat_crud_model::crud_operation_result::CRUDOperationInnerReason::KeyDoesNotExist,
            )
        };
    } else {
        let (field, bytes) = random_field_patch(cfg);
        match mode {
            YcsbExecutionMode::Atomic => own_write_result(tree.update_with(key, |old| {
                old.copy_with_field(field, cfg.field_length, &bytes)
            })),
            YcsbExecutionMode::Transaction => {
                let worker = tree.worker_id();
                let ts_start = tree.begin_snapshot();
                let current = point_on_tree(tree, worker, ts_start, key);
                let result = match current {
                    CRUDOperationResult::MatchedRecords(rows) if !rows.is_empty() => {
                        let payload = rows[0].payload.copy_with_field(field, cfg.field_length, &bytes);
                        let (result, wrote) = update_on_tree(tree, worker, ts_start, key, payload);
                        if wrote {
                            let ts_commit = tree.commit_tx(worker);
                            tree.wal_log_commit(TxStamp::new(worker, ts_start), ts_commit);
                        }
                        result
                    }
                    _ => CRUDOperationResult::ZeroAffected(crate::bat_crud_model::crud_operation_result::CRUDOperationInnerReason::KeyDoesNotExist),
                };
                tree.end_snapshot(ts_start);
                result
            }
        }
    };
    result
}

/// Inserts a brand-new row at `key` (expected to be beyond every key handed
/// out so far — see the driver's key-minting counter).
pub fn insert(tree: &YcsbTree, cfg: &YcsbConfig, key: YcsbKey) {
    insert_with_execution_mode(tree, cfg, key, YcsbExecutionMode::Atomic)
}

pub fn insert_with_execution_mode(
    tree: &YcsbTree,
    cfg: &YcsbConfig,
    key: YcsbKey,
    mode: YcsbExecutionMode,
) {
    let payload = random_row(cfg);
    let result = match mode {
        YcsbExecutionMode::Atomic => tree.dispatch_crud(CRUDOperation::Insert(key, payload)),
        YcsbExecutionMode::Transaction => transactional_insert(tree, key, payload),
    };
    match result {
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
        CRUDOperationResult::MatchedRecordIter(iter) => {
            if read_payload {
                let (count, checksum) =
                    iter.fold_ref((0usize, 0u8), |(count, checksum), _, payload| {
                        let checksum = payload
                            .as_bytes()
                            .iter()
                            .fold(checksum, |a, b| a.wrapping_add(*b));
                        (count + 1, checksum)
                    });
                std::hint::black_box(checksum);
                count
            } else {
                iter.count_ref()
            }
        }
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
    tree: &YcsbTree,
    cfg: &YcsbConfig,
    key: YcsbKey,
    write_all_fields: bool,
    read_payload: bool,
) -> bool {
    let _ = read_with_mode(tree, key, read_payload);
    update(tree, cfg, key, write_all_fields)
}

pub fn read_modify_write_with_execution_mode(
    tree: &YcsbTree,
    cfg: &YcsbConfig,
    key: YcsbKey,
    write_all_fields: bool,
    read_payload: bool,
    mode: YcsbExecutionMode,
) -> bool {
    match mode {
        YcsbExecutionMode::Atomic => {
            read_modify_write_with_mode(tree, cfg, key, write_all_fields, read_payload)
        }
        YcsbExecutionMode::Transaction => loop {
            let worker = tree.worker_id();
            let ts_start = tree.begin_snapshot();
            let current = point_on_tree(tree, worker, ts_start, key);
            let result = match current {
                    CRUDOperationResult::MatchedRecords(rows) if !rows.is_empty() => {
                        if read_payload {
                            let checksum = rows[0]
                                .payload
                                .as_bytes()
                                .iter()
                                .fold(0u8, |a, b| a.wrapping_add(*b));
                            std::hint::black_box(checksum);
                        }
                        let payload = if write_all_fields {
                            random_row(cfg)
                        } else {
                            let (field, bytes) = random_field_patch(cfg);
                            rows[0]
                                .payload
                                .copy_with_field(field, cfg.field_length, &bytes)
                        };
                        let (result, wrote) =
                            update_on_tree(tree, worker, ts_start, key, payload);
                        if wrote {
                            let ts_commit = tree.commit_tx(worker);
                            tree.wal_log_commit(TxStamp::new(worker, ts_start), ts_commit);
                        }
                        result
                    }
                    _ => CRUDOperationResult::ZeroAffected(
                        crate::bat_crud_model::crud_operation_result::CRUDOperationInnerReason::KeyDoesNotExist,
                    ),
                };
            tree.end_snapshot(ts_start);
            match result {
                CRUDOperationResult::Updated(_) => break true,
                CRUDOperationResult::ZeroAffected(_) => break false,
                CRUDOperationResult::Conflict => std::hint::spin_loop(),
                other => panic!("ycsb transactional RMW: unexpected result: {other}"),
            }
        },
    }
}
