//! Initial YCSB data-set population (`recordcount` rows), analogous to
//! `tpcc_load`: plain single-op `dispatch_crud` inserts, each already its own
//! auto-committing transaction, run sequentially before any concurrent
//! worker starts.

use crate::bat_bench::ycsb_random::random_row;
use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbKey, YcsbTree};
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;

/// Loads keys `1..=cfg.record_count`.
pub fn populate(tree: &YcsbTree, cfg: &YcsbConfig) {
    for key in 1..=cfg.record_count {
        let row = random_row(cfg);
        match tree.dispatch_crud(CRUDOperation::Insert(key as YcsbKey, row)) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("ycsb load: unexpected insert result for key {key}: {other}"),
        }
    }
}
