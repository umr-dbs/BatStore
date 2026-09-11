//! OLAP-side workload for the TPC-C + Scan methodology (Alhomssi & Leis,
//! VLDB'23): concurrent long-running/periodic analytical scans that stress
//! the MVCC engine's version-chain traversal while OLTP keeps running.
//!
//! Two modes reproduce the paper's two experiments:
//! - [`OlapMode::OpenAndSleep`] — "just opens a transaction (snapshot) and
//!   then sleeps" (Fig. 1/9): a worst-case robustness stress test for OLTP,
//!   since the held-open snapshot blocks GC/commit-log pruning.
//! - [`OlapMode::ScanDelaySweep`] — "scans other tables (or just sleeps) for
//!   a varying number of seconds, then scans all tuples in the warehouse and
//!   district relations" (Fig. 10): the transaction's snapshot is fixed
//!   *before* the delay, so the delayed scan must traverse however many
//!   versions OLTP piled up on those two hot tables in the meantime —
//!   reported as scanned tuples/sec vs. delay.

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;

use crate::bat_bench::tpcc_schema::*;
use crate::bat_bench::tpcc_txn::TpccTxn;
use crate::bat_bench::tpch_queries;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_record_model::version_info::Version;

#[derive(Clone, Debug)]
pub struct ScanResult {
    pub mode: &'static str,
    /// Wall-clock seconds since this OLAP worker thread started (i.e. since
    /// the timed phase began) — the x-axis for plotting how a metric moves
    /// over the run, since CSV row order alone doesn't carry a time value.
    pub elapsed_secs: f64,
    pub delay_secs: f64,
    pub snapshot: Version,
    pub scanned_tuples: usize,
    pub latency_ns: u128,
    /// A single characteristic numeric result, for modes where "scanned
    /// tuples" alone doesn't capture the query's output — e.g. a CH-
    /// benCHmark query's total revenue, or its output group count for
    /// `scanned_tuples` doesn't apply. `None` for the plain scan modes.
    pub summary: Option<f64>,
    /// HTAP freshness/staleness (`ChBenchmark` mode only): logical-clock
    /// versions between this query's snapshot (`snapshot`, i.e. `ts_start`)
    /// and whatever was freshest the instant the query finished
    /// (`tree.current_version()` read right after) — "how out of date is
    /// this analytical answer, in versions, the moment I have it." Since
    /// `GlobalClock` advances on every transaction begin *and* commit, this
    /// is a logical-tick count, not a raw commit count. `None` for the plain
    /// scan modes.
    pub staleness_versions: Option<u64>,
}

impl ScanResult {
    pub fn tuples_per_sec(&self) -> f64 {
        if self.latency_ns == 0 {
            0.0
        } else {
            self.scanned_tuples as f64 / (self.latency_ns as f64 / 1e9)
        }
    }
}

#[derive(Clone, Debug)]
pub enum OlapMode {
    /// Opens a snapshot and holds it open for `hold`, repeating until told
    /// to stop. Models Fig. 1/9's "1 OLTP + 1 long-running OLAP" robustness
    /// experiment.
    OpenAndSleep { hold: Duration },
    /// Sweeps a list of delays in one continuous run: for each, begins a
    /// fresh snapshot, sleeps `delay`, then scans the warehouse+district
    /// tables under that (now-aged) snapshot. Models Fig. 10.
    ScanDelaySweep { delays: Vec<Duration> },
    /// Repeatedly scans the *current* (freshest) snapshot as fast as
    /// possible — a throughput-oriented full-database scan, closer to this
    /// project's own base OLAP methodology (see `bat_test::olap_tests`).
    RepeatedFreshFullScan,
    /// H3: repeatedly scan one fixed snapshot while OLTP advances.
    RepeatedHistoricFullScan,
    /// Runs the CH-benCHmark analytical queries (`bat_bench::tpch_queries`)
    /// in rotation — Q1, Q6, Q4, Q5 in that order (cheapest/no-join queries
    /// first) — repeating until told to stop. `region_name` is Q5's region
    /// filter; `date_lo`/`date_hi` bound the entry/delivery-date filters
    /// every query but Q1 uses (Q1 only takes `date_hi`, as
    /// `delivered_before`).
    ChBenchmark {
        region_name: String,
        date_lo: i64,
        date_hi: i64,
    },
    /// Runs only Q1. Used by the cross-engine `htap_q1` workload so Q4/Q5
    /// cannot contaminate its OLTP-interference or query-latency results.
    /// If `TpccDatabase::enable_scan_pool(Table::OrderLine, _)` was called
    /// (see `DriverConfig::scan_pool_workers`), fans each query out across
    /// that shared pool via `parallel_scan::q1_parallel` — see
    /// `bat_tree::scan_pool::ScanWorkerPool`'s doc for why the pool is shared rather
    /// than spawned per OLAP thread. Falls back to the plain sequential
    /// `tpch_queries::q1` otherwise. `num_warehouses` is needed to compute
    /// the parallel case's partition.
    ChQ1 {
        delivered_before: i64,
        num_warehouses: u32,
    },
    /// Runs only Q6, for the corresponding isolated `htap_q6` workload. See
    /// `ChQ1`'s doc.
    ChQ6 {
        date_lo: i64,
        date_hi: i64,
        max_qty: u8,
        num_warehouses: u32,
    },
    /// Exact pinned-BenchBase CH-benCHmark Q1 SQL semantics. Its fixed delivery-date
    /// predicate is zone-map pruned by [`tpch_queries::q1_benchbase`].
    BenchbaseQ1,
    /// Exact pinned-BenchBase CH-benCHmark Q6 SQL semantics. Its fixed delivery-date
    /// interval is zone-map pruned by [`tpch_queries::q6_benchbase`].
    BenchbaseQ6,
}

fn sleep_checking_stop(dur: Duration, stop: &AtomicBool) {
    let step = Duration::from_millis(50);
    let mut remaining = dur;
    while remaining > Duration::ZERO {
        if stop.load(Relaxed) {
            return;
        }
        let this_step = remaining.min(step);
        std::thread::sleep(this_step);
        remaining -= this_step;
    }
}

/// Fig. 1/9-style: open a snapshot, hold it for `hold` (or until `stop`),
/// then release without ever reading — the worst case for OLTP robustness.
fn open_and_sleep_once(
    db: &TpccDatabase,
    hold: Duration,
    stop: &AtomicBool,
    run_start: Instant,
) -> ScanResult {
    let tx = TpccTxn::begin(db);
    let snapshot = tx.ts_start();
    sleep_checking_stop(hold, stop);
    tx.commit();

    ScanResult {
        mode: "open_and_sleep",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: hold.as_secs_f64(),
        snapshot,
        scanned_tuples: 0,
        latency_ns: hold.as_nanos(),
        summary: None,
        staleness_versions: None,
    }
}

/// Fig. 10-style: fixes a snapshot, ages it by `delay`, then scans the
/// warehouse+district relations under that aged snapshot.
fn scan_after_delay_once(db: &TpccDatabase, delay: Duration, run_start: Instant) -> ScanResult {
    let mut tx = TpccTxn::begin(db);
    let snapshot = tx.ts_start();
    std::thread::sleep(delay);

    let start = Instant::now();
    let mut scanned = |table, range| match tx.range(table, range, true) {
        CRUDOperationResult::MatchedRecords(v) => v.len(),
        other => panic!("tpcc olap scan: unexpected range result: {other}"),
    };
    let scanned = scanned(Table::Warehouse, warehouse_table_range())
        + scanned(Table::District, district_table_range());
    let latency = start.elapsed();
    tx.commit();

    ScanResult {
        mode: "scan_after_delay",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: delay.as_secs_f64(),
        snapshot,
        scanned_tuples: scanned,
        latency_ns: latency.as_nanos(),
        summary: None,
        staleness_versions: None,
    }
}

/// Freshest-snapshot full-database scan, for throughput-style measurements.
/// Since each table is now its own tree (no single "whole shared tree" to
/// scan in one call — see `bat_bench::tpcc_schema` module docs), this sums a
/// full-range scan over every table instead.
fn fresh_full_scan_once(db: &TpccDatabase, run_start: Instant) -> ScanResult {
    let mut tx = TpccTxn::begin(db);
    let snapshot = tx.ts_start();

    let start = Instant::now();
    let full_range = crate::bat_query::interval::Interval::new(TpccKey::MIN, TpccKey::MAX);
    let scanned = Table::ALL
        .iter()
        .map(|&table| tx.range_count(table, full_range))
        .sum();
    let latency = start.elapsed();
    tx.commit();

    ScanResult {
        mode: "fresh_full_scan",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: 0.0,
        snapshot,
        scanned_tuples: scanned,
        latency_ns: latency.as_nanos(),
        summary: None,
        staleness_versions: None,
    }
}

/// Runs each of the 4 implemented CH-benCHmark queries once (see
/// `tpch_queries` module docs), reporting one `ScanResult` per query.
/// `scanned_tuples` holds each query's *output* cardinality (group count,
/// or 1 for the scalar Q6) — these queries don't expose their raw input
/// scan size the way `fresh_full_scan_once` does — and `summary` holds a
/// characteristic aggregate value (Q1: total revenue across every group;
/// Q6: the forecasted revenue; Q4: total flagged orders; Q5: top nation's
/// revenue). `staleness_versions` is `tree.current_version()` (read right
/// after each query returns) minus that query's own snapshot — see
/// `ScanResult::staleness_versions` and `tpch_queries` module docs.
fn ch_benchmark_queries_once(
    db: &TpccDatabase,
    region_name: &str,
    date_lo: i64,
    date_hi: i64,
    run_start: Instant,
) -> Vec<ScanResult> {
    let mut out = Vec::with_capacity(4);
    let staleness = |ts_start: Version| Some(db.current_version().saturating_sub(ts_start));

    let start = Instant::now();
    let (q1, ts_start) = tpch_queries::q1(db, date_hi);
    out.push(ScanResult {
        mode: "ch_q1_pricing_summary",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: 0.0,
        snapshot: ts_start,
        scanned_tuples: q1.len(),
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q1.iter().map(|g| g.sum_amount).sum()),
        staleness_versions: staleness(ts_start),
    });

    let start = Instant::now();
    let (q6, ts_start) = tpch_queries::q6(db, date_lo, date_hi, 24);
    out.push(ScanResult {
        mode: "ch_q6_forecast_revenue",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: 0.0,
        snapshot: ts_start,
        scanned_tuples: 1,
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q6),
        staleness_versions: staleness(ts_start),
    });

    let start = Instant::now();
    let (q4, ts_start) = tpch_queries::q4(
        db,
        date_lo,
        date_hi,
        Duration::from_secs(3600 * 24).as_millis() as i64,
    );
    out.push(ScanResult {
        mode: "ch_q4_order_priority",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: 0.0,
        snapshot: ts_start,
        scanned_tuples: q4.len(),
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q4.iter().map(|g| g.order_count as f64).sum()),
        staleness_versions: staleness(ts_start),
    });

    let start = Instant::now();
    let (q5, ts_start) = tpch_queries::q5(db, region_name, date_lo, date_hi);
    out.push(ScanResult {
        mode: "ch_q5_revenue_by_nation",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: 0.0,
        snapshot: ts_start,
        scanned_tuples: q5.len(),
        latency_ns: start.elapsed().as_nanos(),
        summary: q5.first().map(|r| r.revenue),
        staleness_versions: staleness(ts_start),
    });

    out
}

fn ch_q1_once(db: &TpccDatabase, delivered_before: i64, run_start: Instant) -> ScanResult {
    let start = Instant::now();
    let (q1, ts_start) = tpch_queries::q1(db, delivered_before);
    ScanResult {
        mode: "ch_q1_variant",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: 0.0,
        snapshot: ts_start,
        scanned_tuples: q1.len(),
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q1.iter().map(|g| g.sum_amount).sum()),
        staleness_versions: Some(db.current_version().saturating_sub(ts_start)),
    }
}

/// Same shape as `ch_q1_once`, but fanned out across `pool` — see
/// `parallel_scan::q1_parallel`'s doc.
fn ch_q1_parallel_once(
    db: &TpccDatabase,
    pool: &crate::bat_bench::tpcc_schema::TpccScanWorkerPool,
    num_warehouses: u32,
    delivered_before: i64,
    run_start: Instant,
) -> ScanResult {
    let start = Instant::now();
    let (q1, ts_start) =
        crate::bat_bench::parallel_scan::q1_parallel(db, pool, num_warehouses, delivered_before);
    ScanResult {
        mode: "ch_q1_variant",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: 0.0,
        snapshot: ts_start,
        scanned_tuples: q1.len(),
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q1.iter().map(|g| g.sum_amount).sum()),
        staleness_versions: Some(db.current_version().saturating_sub(ts_start)),
    }
}

/// Same shape as `ch_q6_once`, but fanned out across `pool` — see
/// `parallel_scan::q6_parallel`'s doc.
fn ch_q6_parallel_once(
    db: &TpccDatabase,
    pool: &crate::bat_bench::tpcc_schema::TpccScanWorkerPool,
    num_warehouses: u32,
    date_lo: i64,
    date_hi: i64,
    max_qty: u8,
    run_start: Instant,
) -> ScanResult {
    let start = Instant::now();
    let (q6, ts_start) =
        crate::bat_bench::parallel_scan::q6_parallel(db, pool, num_warehouses, date_lo, date_hi, max_qty);
    ScanResult {
        mode: "ch_q6_variant",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: 0.0,
        snapshot: ts_start,
        scanned_tuples: 1,
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q6),
        staleness_versions: Some(db.current_version().saturating_sub(ts_start)),
    }
}

fn ch_q6_once(
    db: &TpccDatabase,
    date_lo: i64,
    date_hi: i64,
    max_qty: u8,
    run_start: Instant,
) -> ScanResult {
    let start = Instant::now();
    let (q6, ts_start) = tpch_queries::q6(db, date_lo, date_hi, max_qty);
    ScanResult {
        mode: "ch_q6_variant",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: 0.0,
        snapshot: ts_start,
        scanned_tuples: 1,
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q6),
        staleness_versions: Some(db.current_version().saturating_sub(ts_start)),
    }
}

fn benchbase_q1_once(db: &TpccDatabase, run_start: Instant) -> ScanResult {
    let start = Instant::now();
    let (q1, ts_start) = tpch_queries::q1_benchbase(db);
    ScanResult {
        mode: "ch_q1_pricing_summary",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: 0.0,
        snapshot: ts_start,
        scanned_tuples: q1.len(),
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q1.iter().map(|g| g.sum_amount).sum()),
        staleness_versions: Some(db.current_version().saturating_sub(ts_start)),
    }
}

fn benchbase_q6_once(db: &TpccDatabase, run_start: Instant) -> ScanResult {
    let start = Instant::now();
    let (q6, ts_start) = tpch_queries::q6_benchbase(db);
    ScanResult {
        mode: "ch_q6_forecast_revenue",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        delay_secs: 0.0,
        snapshot: ts_start,
        scanned_tuples: 1,
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q6),
        staleness_versions: Some(db.current_version().saturating_sub(ts_start)),
    }
}

/// One OLAP worker thread's whole run, streaming each completed scan/hold
/// back to `results` as it finishes. Runs until `stop` is set (checked
/// between iterations, and — for `OpenAndSleep` — during the hold itself).
pub fn run_olap_worker(
    db: &TpccDatabase,
    mode: OlapMode,
    stop: &AtomicBool,
    results: &Sender<ScanResult>,
) {
    let run_start = Instant::now();
    match mode {
        OlapMode::OpenAndSleep { hold } => {
            while !stop.load(Relaxed) {
                let r = open_and_sleep_once(db, hold, stop, run_start);
                let _ = results.send(r);
            }
        }
        OlapMode::ScanDelaySweep { delays } => {
            for delay in delays {
                if stop.load(Relaxed) {
                    break;
                }
                let r = scan_after_delay_once(db, delay, run_start);
                let _ = results.send(r);
            }
        }
        OlapMode::RepeatedHistoricFullScan => {
            let mut tx = TpccTxn::begin(db);
            let snapshot = tx.ts_start();
            let snapshot_started = Instant::now();
            let full_range = crate::bat_query::interval::Interval::new(TpccKey::MIN, TpccKey::MAX);
            let mut expected_count = None;
            while !stop.load(Relaxed) {
                let age = snapshot_started.elapsed();
                let start = Instant::now();
                // H3's cross-engine scan set: the nine logical TPC-C relations shared by
                // PostgreSQL, libmdbx and WiredTiger. Exclude BatStore's two derived
                // indexes and the three CH-benCHmark extension tables.
                let scanned = [
                    Table::Warehouse, Table::District, Table::Customer, Table::History,
                    Table::NewOrder, Table::Orders, Table::OrderLine, Table::Item, Table::Stock,
                ].iter()
                    .map(|&table| tx.range_count(table, full_range))
                    .sum();
                let latency = start.elapsed();
                if let Some(expected) = expected_count {
                    assert_eq!(scanned, expected, "historic snapshot cardinality changed");
                }
                expected_count = Some(scanned);
                let _ = results.send(ScanResult {
                    mode: "historic_full_scan",
                    elapsed_secs: run_start.elapsed().as_secs_f64(),
                    delay_secs: age.as_secs_f64(),
                    snapshot,
                    scanned_tuples: scanned,
                    latency_ns: latency.as_nanos(),
                    summary: None,
                    staleness_versions: Some(db.current_version().saturating_sub(snapshot)),
                });
            }
            tx.commit();
        }
        OlapMode::RepeatedFreshFullScan => {
            while !stop.load(Relaxed) {
                let r = fresh_full_scan_once(db, run_start);
                let _ = results.send(r);
            }
        }
        OlapMode::ChBenchmark {
            region_name,
            date_lo,
            date_hi,
        } => {
            while !stop.load(Relaxed) {
                for r in ch_benchmark_queries_once(db, &region_name, date_lo, date_hi, run_start) {
                    let _ = results.send(r);
                    if stop.load(Relaxed) {
                        break;
                    }
                }
            }
        }
        OlapMode::ChQ1 {
            delivered_before,
            num_warehouses,
        } => {
            // Looked up once per OLAP thread's whole run, not per query: the
            // pool (if any) is assigned once, database-wide, by whoever
            // called `TpccDatabase::enable_scan_pool` — see that method's
            // and `bat_tree::scan_pool::ScanWorkerPool`'s docs.
            match db.scan_pool(Table::OrderLine) {
                Some(pool) => {
                    while !stop.load(Relaxed) {
                        let _ = results.send(ch_q1_parallel_once(
                            db, &pool, num_warehouses, delivered_before, run_start,
                        ));
                    }
                }
                None => {
                    while !stop.load(Relaxed) {
                        let _ = results.send(ch_q1_once(db, delivered_before, run_start));
                    }
                }
            }
        }
        OlapMode::ChQ6 {
            date_lo,
            date_hi,
            max_qty,
            num_warehouses,
        } => match db.scan_pool(Table::OrderLine) {
            Some(pool) => {
                while !stop.load(Relaxed) {
                    let _ = results.send(ch_q6_parallel_once(
                        db, &pool, num_warehouses, date_lo, date_hi, max_qty, run_start,
                    ));
                }
            }
            None => {
                while !stop.load(Relaxed) {
                    let _ = results.send(ch_q6_once(db, date_lo, date_hi, max_qty, run_start));
                }
            }
        },
        OlapMode::BenchbaseQ1 => {
            while !stop.load(Relaxed) {
                let _ = results.send(benchbase_q1_once(db, run_start));
            }
        }
        OlapMode::BenchbaseQ6 => {
            while !stop.load(Relaxed) {
                let _ = results.send(benchbase_q6_once(db, run_start));
            }
        }
    }
}
