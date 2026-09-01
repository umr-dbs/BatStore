//! The "S-YCSB" (streaming HTAP) workload's two write-side operations. Reuses
//! `ycsb_txn`'s existing `read`/`update`/`scan` verbatim (a hot-tail update
//! or an OLAP scan against this workload's YCSB-shaped single table is
//! identical to YCSB's own) - the one genuinely new operation is
//! `arrival_upsert`: YCSB's `insert` assumes the key has never existed, but
//! this workload's near-sorted arrival stream can mint an already-used key
//! (a bounded-lateness "late" event colliding with an earlier one - see
//! `s_ycsb_random::mint_arrival_key`'s doc), which must be handled as an
//! upsert rather than a panic.

use crate::bat_bench::ycsb_random::random_row;
use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbKey, YcsbTree};
use crate::bat_bench::ycsb_txn::{self, YcsbExecutionMode};
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::{CRUDOperationInnerReason, CRUDOperationResult};
use crate::bat_db::transaction::insert_on_tree;
use crate::bat_record_model::tx_stamp::TxStamp;

/// Inserts `key` if it has never been written, or falls back to an ordinary
/// update if a bounded-lateness draw made this arrival collide with an
/// already-materialized row. Returns `true` for a genuinely new row,
/// `false` for a late-arrival upsert of an existing one - the driver uses
/// this to keep separate `arrival`/`late_upsert` counters.
pub fn arrival_upsert(
    tree: &YcsbTree,
    cfg: &YcsbConfig,
    key: YcsbKey,
    write_all_fields: bool,
    mode: YcsbExecutionMode,
) -> bool {
    match mode {
        YcsbExecutionMode::Atomic => atomic_arrival_upsert(tree, cfg, key, write_all_fields),
        YcsbExecutionMode::Transaction => {
            transactional_arrival_upsert(tree, cfg, key, write_all_fields)
        }
    }
}

fn atomic_arrival_upsert(
    tree: &YcsbTree,
    cfg: &YcsbConfig,
    key: YcsbKey,
    write_all_fields: bool,
) -> bool {
    let payload = random_row(cfg);
    match tree.dispatch_crud(CRUDOperation::Insert(key, payload)) {
        CRUDOperationResult::Inserted(_) => true,
        // `KeyAlreadyExists`: this ticket's (possibly jittered) key was
        // already materialized by an earlier arrival — a legitimate late
        // upsert (see this module's doc). `Conflict`: a live version exists
        // but isn't yet visible to this read of `current_version()` — the
        // same live-key situation, just observed mid-commit by a racing
        // writer (jittered keys from different write threads can collide),
        // so it gets the same upsert treatment. Atomic `Update` (unlike
        // `Insert`) doesn't check visibility at all — it always overwrites
        // the latest position unconditionally — so falling back to it here
        // is safe in both cases.
        CRUDOperationResult::ZeroAffected(CRUDOperationInnerReason::KeyAlreadyExists)
        | CRUDOperationResult::Conflict => {
            ycsb_txn::update_with_execution_mode(
                tree,
                cfg,
                key,
                write_all_fields,
                YcsbExecutionMode::Atomic,
            );
            false
        }
        other => panic!("s_ycsb arrival: unexpected atomic insert result: {other}"),
    }
}

fn transactional_arrival_upsert(
    tree: &YcsbTree,
    cfg: &YcsbConfig,
    key: YcsbKey,
    write_all_fields: bool,
) -> bool {
    loop {
        let worker = tree.worker_id();
        let ts_start = tree.begin_snapshot();
        let payload = random_row(cfg);
        let (result, wrote) = insert_on_tree(tree, worker, ts_start, key, payload);
        if wrote {
            let ts_commit = tree.commit_tx(worker);
            tree.wal_log_commit(TxStamp::new(worker, ts_start), ts_commit);
        }
        tree.end_snapshot(ts_start);
        match result {
            CRUDOperationResult::Inserted(_) => return true,
            CRUDOperationResult::ZeroAffected(CRUDOperationInnerReason::KeyAlreadyExists) => {
                ycsb_txn::update_with_execution_mode(
                    tree,
                    cfg,
                    key,
                    write_all_fields,
                    YcsbExecutionMode::Transaction,
                );
                return false;
            }
            CRUDOperationResult::Conflict => std::hint::spin_loop(),
            other => panic!("s_ycsb arrival: unexpected transactional insert result: {other}"),
        }
    }
}
