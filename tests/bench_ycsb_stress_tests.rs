//! Concurrent stress tests for the YCSB benchmark harness (`bat_bench::
//! ycsb_txn`/`ycsb_load`): unlike `bench_ycsb_correctness_tests.rs`
//! (single-threaded), these tests run several real OS threads issuing a live
//! mix of Read/Update/Insert/Scan/Read-Modify-Write concurrently, with GC
//! enabled, for a second or two each - the same shape of workload
//! `ycsb_driver::run_ycsb` runs in real benchmarks, just at test scale.
//!
//! The invariant checked is simple but strong: every key that was ever
//! either originally loaded (`1..=record_count`) or successfully minted by a
//! concurrent Insert (tracked via the same `current_max_key.fetch_add`
//! pattern `ycsb_driver::worker_thread` uses) must, after every thread stops,
//! be readable with exactly the configured `field_count * field_length`
//! byte shape - and nothing beyond the final max key should exist. A lost
//! insert, a torn concurrent update, or a GC pass reclaiming a page a live
//! reader still needs would all show up here as a missing key or a
//! wrong-shaped row.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::thread;
use std::time::Duration;

use crate::bat_bench::ycsb_load::populate;
use crate::bat_bench::ycsb_random::{
    KeySampler, RequestDistribution, YcsbMix, YcsbOpType, pick_op, random_scan_length,
};
use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbTree};
use crate::bat_bench::ycsb_txn;
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_root::index_root::RootIndexType;

fn stress_cfg() -> YcsbConfig {
    YcsbConfig {
        record_count: 500,
        field_count: 4,
        field_length: 16,
    }
}

fn read_bytes(tree: &YcsbTree, key: u64) -> Option<Vec<u8>> {
    match tree.dispatch_crud(CRUDOperation::PointSi(key)) {
        CRUDOperationResult::MatchedRecords(v) => v.first().map(|r| r.payload.as_bytes().to_vec()),
        other => panic!("ycsb stress test: unexpected point result: {other}"),
    }
}

/// Mirrors `ycsb_driver::worker_thread`'s op dispatch, minus the per-second
/// bookkeeping this test doesn't need.
#[allow(clippy::too_many_arguments)]
fn stress_worker(
    tree: Arc<YcsbTree>,
    cfg: YcsbConfig,
    mix: YcsbMix,
    sampler: Arc<KeySampler>,
    max_scan_length: u64,
    current_max_key: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Relaxed) {
        let record_count = cfg.record_count;
        let max_key_now = current_max_key.load(Relaxed);
        match pick_op(&mix) {
            YcsbOpType::Read => {
                ycsb_txn::read(&tree, sampler.sample(record_count, max_key_now));
            }
            YcsbOpType::Update => {
                ycsb_txn::update(
                    &tree,
                    &cfg,
                    sampler.sample(record_count, max_key_now),
                    false,
                );
            }
            YcsbOpType::Insert => {
                let key = current_max_key.fetch_add(1, Relaxed) + 1;
                ycsb_txn::insert(&tree, &cfg, key);
            }
            YcsbOpType::Scan => {
                let key = sampler.sample(record_count, max_key_now);
                ycsb_txn::scan(&tree, key, random_scan_length(max_scan_length));
            }
            YcsbOpType::ReadModifyWrite => {
                ycsb_txn::read_modify_write(
                    &tree,
                    &cfg,
                    sampler.sample(record_count, max_key_now),
                    false,
                );
            }
        }
    }
}

fn run_stress_and_check_every_row(
    gc_update_in_place: bool,
    mix: YcsbMix,
    distribution: RequestDistribution,
    num_threads: usize,
    duration: Duration,
) {
    let cfg = stress_cfg();
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    tree.enable_gc(gc_update_in_place);
    populate(&tree, &cfg);

    let sampler = Arc::new(KeySampler::new(distribution, cfg.record_count));
    let current_max_key = Arc::new(AtomicU64::new(cfg.record_count));
    let stop = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let tree = tree.clone();
            let sampler = sampler.clone();
            let current_max_key = current_max_key.clone();
            let stop = stop.clone();
            thread::spawn(move || stress_worker(tree, cfg, mix, sampler, 50, current_max_key, stop))
        })
        .collect();

    thread::sleep(duration);
    stop.store(true, Relaxed);
    for h in handles {
        h.join().expect("ycsb stress worker thread must not panic");
    }

    let final_max_key = current_max_key.load(Relaxed);
    assert!(
        final_max_key >= cfg.record_count,
        "current_max_key must never move backwards"
    );

    for key in 1..=final_max_key {
        let bytes = read_bytes(&tree, key).unwrap_or_else(|| {
            panic!(
                "key {key} (loaded or concurrently inserted) must be readable after the stress run"
            )
        });
        assert_eq!(
            bytes.len(),
            cfg.field_count * cfg.field_length,
            "row at key {key} has the wrong byte width after concurrent ops"
        );
    }
    assert!(
        read_bytes(&tree, final_max_key + 1).is_none(),
        "no key beyond the final max key should exist"
    );
}

/// Every op type nonzero, uniform key distribution, copy-on-write GC:
/// concurrent inserts racing on `current_max_key` alongside concurrent
/// updates/scans over the whole loaded+inserted range.
#[test]
fn concurrent_mixed_ops_keep_every_row_readable_and_correctly_shaped() {
    let mix = YcsbMix {
        read: 0.3,
        update: 0.3,
        insert: 0.2,
        scan: 0.1,
        read_modify_write: 0.1,
    };
    run_stress_and_check_every_row(
        false,
        mix,
        RequestDistribution::Uniform,
        8,
        Duration::from_millis(1500),
    );
}

#[test]
fn concurrent_workload_d_read_latest_keeps_inserted_rows_consistent() {
    let mix = YcsbMix::workload("d").expect("workload d must be defined");
    run_stress_and_check_every_row(
        true,
        mix,
        RequestDistribution::Latest { theta: 0.99 },
        8,
        Duration::from_millis(1500),
    );
}
