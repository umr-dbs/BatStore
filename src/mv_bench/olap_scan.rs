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
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_record_model::version_info::Version;

#[derive(Clone, Debug)]
pub struct ScanResult {
    pub mode: &'static str,
    pub delay_secs: f64,
    pub snapshot: Version,
    pub scanned_tuples: usize,
    pub latency_ns: u128,
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

    ScanResult { mode: "open_and_sleep", delay_secs: hold.as_secs_f64(), snapshot, scanned_tuples: 0, latency_ns: hold.as_nanos() }
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

    ScanResult { mode: "scan_after_delay", delay_secs: delay.as_secs_f64(), snapshot, scanned_tuples: scanned, latency_ns: latency.as_nanos() }
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

    ScanResult { mode: "fresh_full_scan", delay_secs: 0.0, snapshot, scanned_tuples: scanned, latency_ns: latency.as_nanos() }
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
    }
}
