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

use crate::mv_bench::tpcc_schema::*;
use crate::mv_bench::tpch_queries;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_record_model::version_info::Version;

#[derive(Clone, Debug)]
pub struct ScanResult {
    pub mode: &'static str,
    pub delay_secs: f64,
    pub snapshot: Version,
    pub scanned_tuples: usize,
    pub latency_ns: u128,
    /// A single characteristic numeric result, for modes where "scanned
    /// tuples" alone doesn't capture the query's output — e.g. a CH-
    /// benCHmark query's total revenue, or its output group count for
    /// `scanned_tuples` doesn't apply. `None` for the plain scan modes.
    pub summary: Option<f64>,
}

impl ScanResult {
    pub fn tuples_per_sec(&self) -> f64 {
        if self.latency_ns == 0 { 0.0 } else { self.scanned_tuples as f64 / (self.latency_ns as f64 / 1e9) }
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
    /// project's own base OLAP methodology (see `mv_test::olap_tests`).
    RepeatedFreshFullScan,
    /// Runs the CH-benCHmark analytical queries (`mv_bench::tpch_queries`)
    /// in rotation — Q1, Q6, Q4, Q5 in that order (cheapest/no-join queries
    /// first) — repeating until told to stop. `region_name` is Q5's region
    /// filter; `date_lo`/`date_hi` bound the entry/delivery-date filters
    /// every query but Q1 uses (Q1 only takes `date_hi`, as
    /// `delivered_before`).
    ChBenchmark { region_name: String, date_lo: i64, date_hi: i64 },
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
fn open_and_sleep_once(tree: &TpccTree, hold: Duration, stop: &AtomicBool) -> ScanResult {
    let tx = TpccTxn::begin(tree);
    let snapshot = tx.ts_start();
    sleep_checking_stop(hold, stop);
    tx.commit();

    ScanResult { mode: "open_and_sleep", delay_secs: hold.as_secs_f64(), snapshot, scanned_tuples: 0, latency_ns: hold.as_nanos(), summary: None }
}

/// Fig. 10-style: fixes a snapshot, ages it by `delay`, then scans the
/// warehouse+district relations under that aged snapshot.
fn scan_after_delay_once(tree: &TpccTree, delay: Duration) -> ScanResult {
    let tx = TpccTxn::begin(tree);
    let snapshot = tx.ts_start();
    std::thread::sleep(delay);

    let start = Instant::now();
    let scanned = match tx.range(warehouse_and_district_range(), true) {
        CRUDOperationResult::MatchedRecords(v) => v.len(),
        other => panic!("tpcc olap scan: unexpected range result: {other}"),
    };
    let latency = start.elapsed();
    tx.commit();

    ScanResult { mode: "scan_after_delay", delay_secs: delay.as_secs_f64(), snapshot, scanned_tuples: scanned, latency_ns: latency.as_nanos(), summary: None }
}

/// Freshest-snapshot full-database scan, for throughput-style measurements.
fn fresh_full_scan_once(tree: &TpccTree) -> ScanResult {
    let tx = TpccTxn::begin(tree);
    let snapshot = tx.ts_start();

    let start = Instant::now();
    let scanned = match tx.range(crate::mv_utils::interval::Interval::new(TpccKey::MIN, TpccKey::MAX), true) {
        CRUDOperationResult::MatchedRecords(v) => v.len(),
        other => panic!("tpcc olap scan: unexpected range result: {other}"),
    };
    let latency = start.elapsed();
    tx.commit();

    ScanResult { mode: "fresh_full_scan", delay_secs: 0.0, snapshot, scanned_tuples: scanned, latency_ns: latency.as_nanos(), summary: None }
}

/// Runs each of the 4 implemented CH-benCHmark queries once (see
/// `tpch_queries` module docs), reporting one `ScanResult` per query.
/// `scanned_tuples` holds each query's *output* cardinality (group count,
/// or 1 for the scalar Q6) — these queries don't expose their raw input
/// scan size the way `fresh_full_scan_once` does — and `summary` holds a
/// characteristic aggregate value (Q1: total revenue across every group;
/// Q6: the forecasted revenue; Q4: total flagged orders; Q5: top nation's
/// revenue).
fn ch_benchmark_queries_once(tree: &TpccTree, region_name: &str, date_lo: i64, date_hi: i64) -> Vec<ScanResult> {
    let mut out = Vec::with_capacity(4);

    let start = Instant::now();
    let q1 = tpch_queries::q1(tree, date_hi);
    out.push(ScanResult {
        mode: "ch_q1_pricing_summary",
        delay_secs: 0.0,
        snapshot: 0,
        scanned_tuples: q1.len(),
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q1.iter().map(|g| g.sum_amount).sum()),
    });

    let start = Instant::now();
    let q6 = tpch_queries::q6(tree, date_lo, date_hi, 24);
    out.push(ScanResult {
        mode: "ch_q6_forecast_revenue",
        delay_secs: 0.0,
        snapshot: 0,
        scanned_tuples: 1,
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q6),
    });

    let start = Instant::now();
    let q4 = tpch_queries::q4(tree, date_lo, date_hi, Duration::from_secs(3600 * 24).as_millis() as i64);
    out.push(ScanResult {
        mode: "ch_q4_order_priority",
        delay_secs: 0.0,
        snapshot: 0,
        scanned_tuples: q4.len(),
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q4.iter().map(|g| g.order_count as f64).sum()),
    });

    let start = Instant::now();
    let q5 = tpch_queries::q5(tree, region_name, date_lo, date_hi);
    out.push(ScanResult {
        mode: "ch_q5_revenue_by_nation",
        delay_secs: 0.0,
        snapshot: 0,
        scanned_tuples: q5.len(),
        latency_ns: start.elapsed().as_nanos(),
        summary: q5.first().map(|r| r.revenue),
    });

    out
}

/// One OLAP worker thread's whole run, streaming each completed scan/hold
/// back to `results` as it finishes. Runs until `stop` is set (checked
/// between iterations, and — for `OpenAndSleep` — during the hold itself).
pub fn run_olap_worker(tree: &TpccTree, mode: OlapMode, stop: &AtomicBool, results: &Sender<ScanResult>) {
    match mode {
        OlapMode::OpenAndSleep { hold } => {
            while !stop.load(Relaxed) {
                let r = open_and_sleep_once(tree, hold, stop);
                let _ = results.send(r);
            }
        }
        OlapMode::ScanDelaySweep { delays } => {
            for delay in delays {
                if stop.load(Relaxed) {
                    break;
                }
                let r = scan_after_delay_once(tree, delay);
                let _ = results.send(r);
            }
        }
        OlapMode::RepeatedFreshFullScan => {
            while !stop.load(Relaxed) {
                let r = fresh_full_scan_once(tree);
                let _ = results.send(r);
            }
        }
        OlapMode::ChBenchmark { region_name, date_lo, date_hi } => {
            while !stop.load(Relaxed) {
                for r in ch_benchmark_queries_once(tree, &region_name, date_lo, date_hi) {
                    let _ = results.send(r);
                    if stop.load(Relaxed) {
                        break;
                    }
                }
            }
        }
    }
}
