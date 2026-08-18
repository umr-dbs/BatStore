//! Concurrent stress tests for the "S-HTAP" (streaming HTAP) benchmark
//! harness (`bat_bench::s_htap_random`/`s_htap_txn`): several real OS threads
//! run the workload's actual shape - near-sorted arrivals plus recency-biased
//! hot-tail updates on one side, long OLAP scans straddling the cold/hot
//! boundary on the other - concurrently, with GC enabled, for a second or
//! two, mirroring `s_htap_driver::run_s_htap` at test scale (see
//! `bench_ycsb_stress_tests.rs`, whose structure this follows for the
//! write-side invariant).
//!
//! Two invariants are checked:
//! - Every key that was ever cold-loaded (`1..=record_count`) or actually
//!   touched by a concurrent write op must, after every thread stops, be
//!   readable with exactly the configured row shape, and no key beyond the
//!   final arrival ticket should exist (a lost/torn write or a premature GC
//!   reclaim would show up as a missing or wrong-shaped key). Note this
//!   deliberately does *not* assert that every key in `1..=final_max_key` is
//!   present when `max_lateness > 0`: `mint_arrival_key` maps ticket `t` to
//!   key `t - lateness` for a random `lateness` in `0..=max_lateness`, so a
//!   given key `v > record_count` is only ever reachable from the bounded
//!   set of tickets `v..=v+max_lateness` - once those have all been drawn
//!   without landing exactly on `v`, `v` is permanently unreachable. That's
//!   a real, structural (non-negligible probability, not a rare corner case)
//!   property of this workload's key-minting design, not an engine bug - so
//!   the test tracks the actual keys each write op touched instead of
//!   assuming ticket-contiguous coverage.
//! - Every OLAP scan completed during the run must return at most as many
//!   rows as its own requested span - a direct, always-on regression guard
//!   for the hot/cold double-counting bug fixed in
//!   `RangeQueryIter::walk_cold_chain_for_range` (2026-08-15/16): that bug
//!   let a single key be counted once from the hot leaf and again from its
//!   cold chain, which always manifests as a scan returning more rows than
//!   the span it was given.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::bat_bench::s_htap_random::{
    HotTailSampler, SHtapMix, SHtapWriteOp, mint_arrival_key, olap_scan_bounds, pick_write_op,
};
use crate::bat_bench::s_htap_txn::arrival_upsert;
use crate::bat_bench::ycsb_load::populate;
use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbTree};
use crate::bat_bench::ycsb_txn::{self, YcsbExecutionMode};
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_root::index_root::RootIndexType;

fn stress_cfg() -> YcsbConfig {
    YcsbConfig {
        record_count: 300,
        field_count: 3,
        field_length: 16,
    }
}

fn read_bytes(tree: &YcsbTree, key: u64) -> Option<Vec<u8>> {
    match tree.dispatch_crud(CRUDOperation::PointSi(key)) {
        CRUDOperationResult::MatchedRecords(v) => v.first().map(|r| r.payload.as_bytes().to_vec()),
        other => panic!("s_htap stress test: unexpected point result: {other}"),
    }
}

/// Mirrors `s_htap_driver::write_worker_thread`'s op dispatch, minus the
/// per-second bookkeeping this test doesn't need. Returns every key this
/// thread actually wrote (arrival or hot-update) so the caller can verify
/// exactly those keys - see the module doc for why the ticket range itself
/// isn't a valid stand-in for that set under lateness.
///
/// A hot-tail sample can legitimately miss: `hot_sampler` is anchored to the
/// shared `current_max_key` ticket counter, which another thread's
/// `mint_arrival_key` bumps *before* that same arrival's row is actually
/// materialized (there's a real window between the counter fetch-add and
/// the row write) - so a concurrent hot update can sample a key whose
/// arrival hasn't landed yet and correctly report a miss without writing
/// anything. Only a reported hit is recorded as touched.
fn write_worker(
    tree: Arc<YcsbTree>,
    cfg: YcsbConfig,
    mix: SHtapMix,
    hot_sampler: Arc<HotTailSampler>,
    current_max_key: Arc<AtomicU64>,
    max_lateness: u64,
    execution_mode: YcsbExecutionMode,
    stop: Arc<AtomicBool>,
) -> Vec<u64> {
    let mut touched = Vec::new();
    while !stop.load(Relaxed) {
        match pick_write_op(&mix) {
            SHtapWriteOp::Arrival => {
                let key = mint_arrival_key(&current_max_key, max_lateness);
                arrival_upsert(&tree, &cfg, key, false, execution_mode);
                touched.push(key);
            }
            SHtapWriteOp::HotUpdate => {
                let key = hot_sampler.sample(current_max_key.load(Relaxed));
                if ycsb_txn::update_with_execution_mode(&tree, &cfg, key, false, execution_mode) {
                    touched.push(key);
                }
            }
        }
    }
    touched
}

/// Mirrors `s_htap_driver::olap_worker_thread`, additionally recording any
/// scan whose returned count exceeds its own requested span into `violations`
/// instead of asserting inline (asserting inside a spawned thread would only
/// surface as an opaque `join` panic message with no scan details attached).
fn olap_worker(
    tree: Arc<YcsbTree>,
    current_max_key: Arc<AtomicU64>,
    olap_lag: u64,
    olap_span: u64,
    stop: Arc<AtomicBool>,
    violations: Arc<Mutex<Vec<(u64, u64, usize)>>>,
) {
    while !stop.load(Relaxed) {
        let (lo, len) = olap_scan_bounds(current_max_key.load(Relaxed), olap_lag, olap_span);
        let scanned = ycsb_txn::scan_with_mode(&tree, lo, len, true);
        if scanned as u64 > len {
            violations.lock().unwrap().push((lo, len, scanned));
        }
    }
}

fn run_stress_and_check(
    gc_update_in_place: bool,
    mix: SHtapMix,
    max_lateness: u64,
    hot_window: u64,
    execution_mode: YcsbExecutionMode,
    num_write_threads: usize,
    num_olap_threads: usize,
    duration: Duration,
) {
    let cfg = stress_cfg();
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    tree.enable_gc(gc_update_in_place);
    populate(&tree, &cfg);

    let hot_sampler = Arc::new(HotTailSampler::new(0.99, hot_window));
    let current_max_key = Arc::new(AtomicU64::new(cfg.record_count));
    let stop = Arc::new(AtomicBool::new(false));
    let violations = Arc::new(Mutex::new(Vec::new()));

    let write_handles: Vec<_> = (0..num_write_threads)
        .map(|_| {
            let tree = tree.clone();
            let hot_sampler = hot_sampler.clone();
            let current_max_key = current_max_key.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                write_worker(
                    tree,
                    cfg,
                    mix,
                    hot_sampler,
                    current_max_key,
                    max_lateness,
                    execution_mode,
                    stop,
                )
            })
        })
        .collect();

    let olap_span = hot_window * 3;
    let olap_handles: Vec<_> = (0..num_olap_threads)
        .map(|_| {
            let tree = tree.clone();
            let current_max_key = current_max_key.clone();
            let stop = stop.clone();
            let violations = violations.clone();
            thread::spawn(move || {
                olap_worker(tree, current_max_key, 0, olap_span, stop, violations)
            })
        })
        .collect();

    thread::sleep(duration);
    stop.store(true, Relaxed);
    let mut touched_keys: HashSet<u64> = HashSet::new();
    for h in write_handles {
        touched_keys.extend(h.join().expect("s_htap write worker thread must not panic"));
    }
    for h in olap_handles {
        h.join().expect("s_htap OLAP worker thread must not panic");
    }

    let violations = violations.lock().unwrap();
    assert!(
        violations.is_empty(),
        "OLAP scan(s) over-counted (hot/cold double-count regression): {violations:?}"
    );

    let final_max_key = current_max_key.load(Relaxed);
    assert!(final_max_key >= cfg.record_count);

    for key in (1..=cfg.record_count).chain(touched_keys.iter().copied()) {
        let bytes = read_bytes(&tree, key).unwrap_or_else(|| {
            panic!("key {key} (cold-loaded or actually written by a worker) must be readable after the stress run")
        });
        assert_eq!(
            bytes.len(),
            cfg.field_count * cfg.field_length,
            "row at key {key} has the wrong byte width after concurrent ops"
        );
    }
    assert!(
        touched_keys.iter().all(|&k| k <= final_max_key),
        "no write op should ever touch a key beyond the current arrival ticket count"
    );
    assert!(
        read_bytes(&tree, final_max_key + 1).is_none(),
        "no key beyond the final arrival ticket should exist"
    );
}

/// The workload's default op mix (mostly hot-tail updates, a trickle of new
/// arrivals) with bounded lateness enabled, so late-arrival upserts actually
/// occur, alongside concurrent OLAP scans, under copy-on-write GC.
#[test]
fn concurrent_default_mix_with_lateness_keeps_every_row_readable_and_scans_never_overcount() {
    run_stress_and_check(
        false,
        SHtapMix::default(),
        50,
        30,
        YcsbExecutionMode::Atomic,
        6,
        2,
        Duration::from_millis(1500),
    );
}

/// A narrower hot window and heavier arrival share, under update-in-place
/// GC and the `Transaction` execution mode - a different write concentration,
/// GC path, and execution mode than the test above, deliberately chosen to
/// force more frequent version-chain compaction on a smaller set of leaves.
#[test]
fn concurrent_narrow_hot_window_under_update_in_place_gc_and_transaction_mode() {
    run_stress_and_check(
        true,
        SHtapMix {
            arrival: 0.4,
            hot_update: 0.6,
        },
        0,
        10,
        YcsbExecutionMode::Transaction,
        6,
        2,
        Duration::from_millis(1500),
    );
}
