//! Crash-recovery stress test: runs the TPC-C OLTP mix on several concurrent
//! threads against a WAL-logged database (the same `enable_wal` +
//! concurrent-writer combination `tpcc_driver::run_tpcc`'s `wal` option
//! exercises in real runs), then drops the live database (flushing and
//! joining the WAL writer - see `mv_wal::writer::WalWriter`'s `Drop`) and
//! rebuilds a fresh one from the log via `TpccDatabase::open_recovered`.
//!
//! Unlike `wal_recovery_tests.rs` (single-threaded, hand-picked ops), this
//! recovers a log that several racing threads wrote concurrently across 14
//! tables, one shared Commit marker per (possibly multi-table) transaction
//! (see `mv_db::DbTransaction::commit`'s doc). Every aggregate captured from
//! the live database right before it's dropped must come back identical
//! (exactly, for row counts/counters; within float rounding, for ytd/balance
//! sums) after recovery - any mismatch would mean the WAL lost, duplicated,
//! or misordered part of some transaction's cross-table writes under
//! concurrent load.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::thread;
use std::time::Duration;

use rand::prelude::*;

use crate::mv_bench::tpcc_load::{populate_items, populate_warehouse};
use crate::mv_bench::tpcc_schema::{Table, TpccConfig, TpccDatabase, TpccKey, TpccRow};
use crate::mv_bench::tpcc_txn::{self, TpccTxn, many};
use crate::mv_query::interval::Interval;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_root::index_root::RootIndexType;

// Keep enough items for representative Stock-key contention without making
// test population expensive.
fn stress_cfg() -> TpccConfig {
    TpccConfig {
        num_warehouses: 3,
        districts_per_warehouse: 2,
        customers_per_district: 30,
        num_items: 5_000,
        initial_orders_per_district: 15,
        initial_new_orders: 5,
        num_suppliers: 2,
    }
}

fn full_range() -> Interval<TpccKey> {
    Interval::new(TpccKey::MIN, TpccKey::MAX)
}

fn scan_all(db: &TpccDatabase, table: Table) -> Vec<RecordPointResult<TpccKey, TpccRow>> {
    let mut tx = TpccTxn::begin(db);
    let rows = many(tx.range(table, full_range(), true));
    tx.commit();
    rows
}

/// Every table's row count plus every float ytd/balance aggregate - enough
/// to catch a lost, duplicated, or partially-replayed transaction across any
/// of the 14 tables after recovery.
struct FullSnapshot {
    counts: [usize; 14],
    w_ytd_sum: f64,
    d_ytd_sum: f64,
    c_ytd_sum: f64,
    c_balance_sum: f64,
    s_ytd_sum: f64,
    ol_qty_sum: u64,
}

fn full_snapshot(db: &TpccDatabase) -> FullSnapshot {
    let mut counts = [0usize; 14];
    for (i, &t) in Table::ALL.iter().enumerate() {
        counts[i] = scan_all(db, t).len();
    }
    let customers = scan_all(db, Table::Customer);
    let order_lines = scan_all(db, Table::OrderLine);
    FullSnapshot {
        counts,
        w_ytd_sum: scan_all(db, Table::Warehouse)
            .iter()
            .map(|r| r.payload.as_warehouse().w_ytd)
            .sum(),
        d_ytd_sum: scan_all(db, Table::District)
            .iter()
            .map(|r| r.payload.as_district().d_ytd)
            .sum(),
        c_ytd_sum: customers
            .iter()
            .map(|r| r.payload.as_customer().c_ytd_payment)
            .sum(),
        c_balance_sum: customers
            .iter()
            .map(|r| r.payload.as_customer().c_balance)
            .sum(),
        s_ytd_sum: scan_all(db, Table::Stock)
            .iter()
            .map(|r| r.payload.as_stock().s_ytd)
            .sum(),
        ol_qty_sum: order_lines
            .iter()
            .map(|r| r.payload.as_order_line().ol_quantity as u64)
            .sum(),
    }
}

fn assert_snapshots_match(before: &FullSnapshot, after: &FullSnapshot) {
    for (i, &t) in Table::ALL.iter().enumerate() {
        assert_eq!(
            before.counts[i], after.counts[i],
            "row count for table {t:?} must match after recovery"
        );
    }
    let eps = 1e-6 * (before.counts.iter().sum::<usize>().max(1) as f64);
    assert!(
        (before.w_ytd_sum - after.w_ytd_sum).abs() < eps,
        "warehouse ytd sum must match after recovery: {} vs {}",
        before.w_ytd_sum,
        after.w_ytd_sum
    );
    assert!(
        (before.d_ytd_sum - after.d_ytd_sum).abs() < eps,
        "district ytd sum must match after recovery: {} vs {}",
        before.d_ytd_sum,
        after.d_ytd_sum
    );
    assert!(
        (before.c_ytd_sum - after.c_ytd_sum).abs() < eps,
        "customer ytd_payment sum must match after recovery: {} vs {}",
        before.c_ytd_sum,
        after.c_ytd_sum
    );
    assert!(
        (before.c_balance_sum - after.c_balance_sum).abs() < eps,
        "customer balance sum must match after recovery: {} vs {}",
        before.c_balance_sum,
        after.c_balance_sum
    );
    assert!(
        (before.s_ytd_sum - after.s_ytd_sum).abs() < eps,
        "stock s_ytd sum must match after recovery: {} vs {}",
        before.s_ytd_sum,
        after.s_ytd_sum
    );
    assert_eq!(
        before.ol_qty_sum, after.ol_qty_sum,
        "total order-line quantity must match after recovery"
    );
}

fn stress_worker(
    db: Arc<TpccDatabase>,
    cfg: TpccConfig,
    stop: Arc<AtomicBool>,
    history_seq: Arc<AtomicU64>,
) {
    while !stop.load(Relaxed) {
        let home_w = rand::rng().random_range(1..=cfg.num_warehouses);
        if rand::rng().random_range(1..=100u32) <= 50 {
            let _ = tpcc_txn::new_order(&db, &cfg, home_w, true);
        } else {
            let _ = tpcc_txn::payment(&db, &cfg, home_w, true, &history_seq);
        }
    }
}

/// Unique per test-process invocation, so parallel `cargo test` runs (and
/// repeated local runs) never collide on the same path.
fn unique_wal_path() -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Relaxed);
    std::env::temp_dir().join(format!(
        "cmvbt_wal_recovery_stress_{}_{n}.log",
        std::process::id()
    ))
}

#[test]
fn wal_logged_concurrent_workload_recovers_to_a_consistent_snapshot() {
    let wal_path = unique_wal_path();
    let _ = std::fs::remove_file(&wal_path);
    let flush_interval = Duration::from_millis(5);

    let cfg = stress_cfg();
    let before = {
        let db = Arc::new(
            TpccDatabase::new_with_wal(RootIndexType::default(), &wal_path, flush_interval)
                .expect("WAL construction must succeed on a fresh path"),
        );
        db.enable_gc(false);

        // Population itself goes through the WAL too, matching
        // `tpcc_driver::run_tpcc`'s ordering (WAL attached before load).
        populate_items(&db, &cfg);
        let history_seq = Arc::new(AtomicU64::new(0));
        for w in 1..=cfg.num_warehouses {
            populate_warehouse(&db, &cfg, w, &history_seq);
        }

        let stop = Arc::new(AtomicBool::new(false));
        let handles: Vec<_> = (0..6)
            .map(|_| {
                let db = db.clone();
                let stop = stop.clone();
                let history_seq = history_seq.clone();
                thread::spawn(move || stress_worker(db, cfg, stop, history_seq))
            })
            .collect();

        thread::sleep(Duration::from_millis(1500));
        stop.store(true, Relaxed);
        for h in handles {
            h.join().expect("stress worker thread must not panic");
        }

        let snap = full_snapshot(&db);
        assert!(
            snap.counts.iter().sum::<usize>() > 0,
            "sanity: the pre-recovery database must not be empty"
        );
        snap
        // `db` (and its `Arc`, now uniquely held) drops here: flushes and
        // joins the WAL writer thread before `open_recovered` reopens the
        // same file below.
    };

    let recovered =
        TpccDatabase::open_recovered(RootIndexType::default(), &wal_path, flush_interval)
            .expect("recovery from a cleanly-closed WAL must succeed");
    let after = full_snapshot(&recovered);

    assert_snapshots_match(&before, &after);

    let _ = std::fs::remove_file(&wal_path);
}
