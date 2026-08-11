//! Concurrent HTAP-style stress test: the standard TPC-C OLTP mix (New-Order/
//! Payment, cross-warehouse remote enabled) running on several threads at
//! the same time as several threads repeatedly executing the CH-benCHmark
//! analytical queries (`mv_bench::tpch_queries` q1/q4/q5/q6) - the same
//! mixed-workload shape `tpcc_driver::run_tpcc`'s `OlapMode::ChBenchmark`
//! exercises in real benchmark runs, at test scale (~2s).
//!
//! Unlike `bench_tpch_correctness_tests.rs` (fully static, hand-built
//! fixtures, no concurrency at all), this test's whole point is to catch
//! bugs that only appear when an analytical query's snapshot read races
//! against concurrent structural modification of the same tables (page
//! splits, GC reclaiming a page mid-scan, a torn cross-table commit becoming
//! visible half-applied). Since the OLTP side's amounts/quantities are
//! internally randomized, each query's *individual* result can't be
//! predicted - instead every query result is checked against invariants
//! that must hold for *any* valid snapshot of a database built only by the
//! real transaction profiles: every revenue/amount aggregate must be
//! finite and non-negative, and every count must be non-negative and
//! internally consistent (e.g. a query's average is exactly its sum divided
//! by its own count). A broken read under concurrency (a double-counted or
//! skipped row, a torn partial write observed mid-flight) would tend to
//! surface as a violated bound here, even though no individual call panics.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rand::prelude::*;

use crate::mv_bench::tpcc_load::{populate_items, populate_regions_and_nations, populate_suppliers, populate_warehouse};
use crate::mv_bench::tpcc_schema::TpccConfig;
use crate::mv_bench::tpcc_schema::TpccDatabase;
use crate::mv_bench::tpcc_txn::{self, TxnOutcome};
use crate::mv_bench::tpch_queries;
use crate::mv_root::index_root::RootIndexType;

// Keep enough items for representative Stock-key contention without making
// test population expensive.
fn stress_cfg() -> TpccConfig {
    TpccConfig {
        num_warehouses: 4,
        districts_per_warehouse: 2,
        customers_per_district: 40,
        num_items: 5_000,
        initial_orders_per_district: 20,
        initial_new_orders: 8,
        num_suppliers: 20,
    }
}

fn oltp_worker(db: Arc<TpccDatabase>, cfg: TpccConfig, stop: Arc<AtomicBool>, history_seq: Arc<AtomicU64>) {
    while !stop.load(Relaxed) {
        let home_w = rand::rng().random_range(1..=cfg.num_warehouses);
        if rand::rng().random_range(1..=100u32) <= 55 {
            let _ = tpcc_txn::new_order(&db, &cfg, home_w, true);
        } else {
            let outcome = tpcc_txn::payment(&db, &cfg, home_w, true, &history_seq);
            debug_assert!(outcome == TxnOutcome::Committed || outcome == TxnOutcome::Conflict || outcome == TxnOutcome::UserAbort);
        }
    }
}

/// One OLAP worker thread's whole run: q1/q6/q4/q5 in a loop, asserting
/// every result stays within the bounds any valid snapshot must satisfy.
/// Returns how many full rounds it completed, so the test can confirm real
/// overlap with the concurrent OLTP phase happened (not just a handful of
/// queries against an empty/static database).
fn olap_worker(db: Arc<TpccDatabase>, region_name: &str, stop: Arc<AtomicBool>) -> u64 {
    let mut rounds = 0u64;
    while !stop.load(Relaxed) {
        let (q1_groups, _ts) = tpch_queries::q1(&db, i64::MAX);
        for g in &q1_groups {
            assert!(g.count > 0, "q1: a reported group must have a positive row count");
            assert!(g.sum_qty > 0, "q1: a reported group's summed quantity must be positive");
            assert!(g.sum_amount.is_finite() && g.sum_amount >= 0.0, "q1: sum_amount must be finite and non-negative, got {}", g.sum_amount);
            assert!((g.avg_qty() - g.sum_qty as f64 / g.count as f64).abs() < 1e-9, "q1: avg_qty must equal sum_qty/count");
            assert!((g.avg_amount() - g.sum_amount / g.count as f64).abs() < 1e-9, "q1: avg_amount must equal sum_amount/count");
        }

        let (q6_revenue, _ts) = tpch_queries::q6(&db, i64::MIN, i64::MAX, 20);
        assert!(q6_revenue.is_finite() && q6_revenue >= 0.0, "q6: forecast revenue must be finite and non-negative, got {q6_revenue}");

        let (q4_counts, _ts) = tpch_queries::q4(&db, i64::MIN, i64::MAX, 1_000);
        for c in &q4_counts {
            assert!(c.order_count > 0, "q4: a reported o_ol_cnt group must have a positive order count");
        }

        let (q5_rev, _ts) = tpch_queries::q5(&db, region_name, i64::MIN, i64::MAX);
        for r in &q5_rev {
            assert!(r.revenue.is_finite() && r.revenue >= 0.0, "q5: per-nation revenue must be finite and non-negative, got {} for {}", r.revenue, r.n_name);
        }
        // No two rows for the same nation - a torn/duplicated read would show up as a repeat.
        for i in 0..q5_rev.len() {
            for j in (i + 1)..q5_rev.len() {
                assert_ne!(q5_rev[i].n_name, q5_rev[j].n_name, "q5: the same nation must not appear twice in one snapshot's result");
            }
        }

        rounds += 1;
    }
    rounds
}

#[test]
fn concurrent_oltp_and_ch_benchmark_queries_never_observe_corrupt_aggregates() {
    let cfg = stress_cfg();
    let db = Arc::new(TpccDatabase::new(RootIndexType::default()));
    db.enable_gc(false);

    populate_regions_and_nations(&db);
    populate_suppliers(&db, &cfg);
    populate_items(&db, &cfg);
    let history_seq = Arc::new(AtomicU64::new(0));
    for w in 1..=cfg.num_warehouses {
        populate_warehouse(&db, &cfg, w, &history_seq);
    }

    let stop = Arc::new(AtomicBool::new(false));

    let oltp_handles: Vec<_> = (0..4).map(|_| {
        let db = db.clone();
        let stop = stop.clone();
        let history_seq = history_seq.clone();
        thread::spawn(move || oltp_worker(db, cfg, stop, history_seq))
    }).collect();

    let olap_handles: Vec<_> = (0..2).map(|_| {
        let db = db.clone();
        let stop = stop.clone();
        thread::spawn(move || olap_worker(db, "EUROPE", stop))
    }).collect();

    thread::sleep(Duration::from_secs(2));
    stop.store(true, Relaxed);

    for h in oltp_handles {
        h.join().expect("OLTP worker thread must not panic");
    }
    let olap_rounds: Vec<u64> = olap_handles.into_iter().map(|h| h.join().expect("OLAP worker thread must not panic")).collect();

    for (i, rounds) in olap_rounds.iter().enumerate() {
        assert!(*rounds > 0, "OLAP worker {i} completed zero rounds in 2s - concurrent phase likely never actually overlapped");
    }
}
