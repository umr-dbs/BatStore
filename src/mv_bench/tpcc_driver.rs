//! Benchmark driver: loads a TPC-C data set, then runs the standard 5-txn
//! OLTP mix concurrently with an OLAP scan workload for a fixed wall-clock
//! duration, following the mixed TPC-C+Scan methodology used to evaluate
//! MVCC storage engines (Alhomssi & Leis, VLDB'23) — see `mv_bench` module
//! docs. Reports per-second OLTP throughput (to reproduce "throughput over
//! time"/"vs. thread count" plots) and per-scan OLAP latency/throughput (to
//! reproduce "scan throughput vs. delay" plots).
//!
//! Threading constraint: every distinct OS thread that ever calls into the
//! tree (every terminal + every OLAP thread) permanently consumes one slot
//! of the tree's fixed `WorkerId` pool, sized to `num_cpus::get()` at tree
//! construction (see `mv_sync::worker::WorkerRegistry`) with no way to grow
//! it afterwards. `num_terminals + num_olap_threads` is therefore clamped to
//! that pool size.

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::unbounded;
use rand::prelude::*;

use crate::mv_bench::olap_scan::{run_olap_worker, OlapMode, ScanResult};
use crate::mv_bench::tpcc_load::{populate_items, populate_warehouse};
use crate::mv_bench::tpcc_schema::TpccConfig;
use crate::mv_bench::tpcc_schema::TpccTree;
use crate::mv_bench::tpcc_txn::{self, TxnOutcome};
use crate::mv_root::index_root::RootIndexType;

pub struct DriverConfig {
    pub tpcc: TpccConfig,
    pub num_terminals: usize,
    pub duration: Duration,
    /// Warehouse affinity (each terminal only ever touches its own assigned
    /// warehouse(s), 0% remote — Fig. 11's "Warehouse Affinity"/low
    /// contention case) vs. cross-warehouse (each transaction picks a
    /// random home warehouse and the spec's normal 1%/15% remote rates
    /// apply — Fig. 11's "Cross Warehouse"/moderate contention case).
    pub affinity: bool,
    pub gc: bool,
    pub update_in_place: bool,
    pub root_star_index: RootIndexType,
    pub olap_mode: OlapMode,
    pub num_olap_threads: usize,
    /// Attaches a live WAL at this path *before* population, so the whole
    /// data set (not just the timed OLTP/OLAP phase) is durable — matching
    /// the referenced benchmarks, where logging is an always-on part of the
    /// storage engine under test, not something toggled on only for the
    /// measured phase. `None` disables WAL entirely (population and OLTP
    /// writes take the plain, unlogged path).
    pub wal: Option<(std::path::PathBuf, Duration)>,
}

// Counter layout: 3 outcomes (Committed, Conflict, UserAbort) per read/write
// txn type, plus 2 for Delivery (which reports districts-delivered /
// conflicts instead, since it's a batch of up to 10 sub-transactions).
const NO: usize = 0;
const PAY: usize = 3;
const OS: usize = 6;
const SL: usize = 9;
const DELIV_DISTRICTS: usize = 12;
const DELIV_CONFLICTS: usize = 13;
const NUM_COUNTERS: usize = 14;

const COUNTER_NAMES: [&str; NUM_COUNTERS] = [
    "new_order_committed", "new_order_conflict", "new_order_user_abort",
    "payment_committed", "payment_conflict", "payment_user_abort",
    "order_status_committed", "order_status_conflict", "order_status_user_abort",
    "stock_level_committed", "stock_level_conflict", "stock_level_user_abort",
    "delivery_districts_delivered", "delivery_conflicts",
];

#[inline]
fn record(totals: &mut [u64; NUM_COUNTERS], base: usize, outcome: TxnOutcome) {
    match outcome {
        TxnOutcome::Committed => totals[base] += 1,
        TxnOutcome::Conflict => totals[base + 1] += 1,
        TxnOutcome::UserAbort => totals[base + 2] += 1,
    }
}

struct TerminalStats {
    new_order_committed_per_sec: Vec<u64>,
    totals: [u64; NUM_COUNTERS],
}

#[allow(clippy::too_many_arguments)]
fn terminal_thread(
    tree: Arc<TpccTree>,
    cfg: TpccConfig,
    my_warehouses: Vec<u32>,
    affinity: bool,
    duration: Duration,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    history_seq: Arc<AtomicU64>,
) -> TerminalStats {
    barrier.wait();

    let mut new_order_committed_per_sec = vec![0u64; duration.as_secs() as usize + 2];
    let mut totals = [0u64; NUM_COUNTERS];
    let start = Instant::now();

    while !stop.load(Relaxed) {
        let home_w = if affinity {
            my_warehouses[rand::rng().random_range(0..my_warehouses.len())]
        } else {
            rand::rng().random_range(1..=cfg.num_warehouses)
        };
        let allow_remote = !affinity;

        match rand::rng().random_range(1..=100u32) {
            1..=45 => {
                let outcome = tpcc_txn::new_order(&tree, &cfg, home_w, allow_remote);
                record(&mut totals, NO, outcome);
                if outcome == TxnOutcome::Committed {
                    let idx = (start.elapsed().as_secs() as usize).min(new_order_committed_per_sec.len() - 1);
                    new_order_committed_per_sec[idx] += 1;
                }
            }
            46..=88 => {
                let outcome = tpcc_txn::payment(&tree, &cfg, home_w, allow_remote, &history_seq);
                record(&mut totals, PAY, outcome);
            }
            89..=92 => {
                let outcome = tpcc_txn::order_status(&tree, &cfg, home_w);
                record(&mut totals, OS, outcome);
            }
            93..=96 => {
                let d = tpcc_txn::delivery(&tree, &cfg, home_w);
                totals[DELIV_DISTRICTS] += d.delivered_districts as u64;
                totals[DELIV_CONFLICTS] += d.conflicts as u64;
            }
            _ => {
                let outcome = tpcc_txn::stock_level(&tree, &cfg, home_w, 15);
                record(&mut totals, SL, outcome);
            }
        }
    }

    TerminalStats { new_order_committed_per_sec, totals }
}

pub fn run_tpcc(cfg: DriverConfig) {
    assert!(cfg.tpcc.num_warehouses >= 1, "tpcc: num_warehouses must be >= 1");

    let max_threads = crate::mv_tree::mvbt::default_max_workers().max(1);

    let mut num_terminals = cfg.num_terminals.max(1);
    if cfg.affinity {
        // Under strict affinity every terminal needs >= 1 owned warehouse.
        num_terminals = num_terminals.min(cfg.tpcc.num_warehouses as usize).max(1);
    }
    let mut num_olap = cfg.num_olap_threads;

    // +1: the main thread itself acquires a WorkerId too, since it does the
    // (sequential) data-set population directly via `dispatch_crud` before
    // any terminal/OLAP thread is spawned.
    if 1 + num_terminals + num_olap > max_threads {
        println!("!! 1 loader + {num_terminals} terminals + {num_olap} OLAP threads > max_workers ({max_threads} = num_cpus); clamping.");
        num_terminals = num_terminals.min(max_threads.saturating_sub(2).max(1));
        num_olap = max_threads.saturating_sub(1 + num_terminals);
    }

    let tree = Arc::new(TpccTree::make_standard(cfg.root_star_index));
    if cfg.gc {
        tree.enable_gc(cfg.update_in_place);
    }

    if let Some((wal_path, flush_interval)) = &cfg.wal {
        let _ = fs::remove_file(wal_path);
        tree.enable_wal(wal_path, *flush_interval).expect("failed to attach WAL");
    }

    println!(
        "TPC-C + OLAP scan benchmark\n\
         - warehouses            = {}\n\
         - terminals (OLTP)      = {num_terminals}\n\
         - OLAP threads          = {num_olap} ({})\n\
         - mode                  = {}\n\
         - duration              = {:?}\n\
         - GC                    = {} (update_in_place={})\n\
         - WAL                   = {}\n\
         - root*                 = {}\n\
         - items/customers/orders per district = {}/{}/{}",
        cfg.tpcc.num_warehouses,
        num_olap_mode_summary(&cfg.olap_mode),
        if cfg.affinity { "warehouse affinity (0% remote)" } else { "cross warehouse" },
        cfg.duration,
        cfg.gc, cfg.update_in_place,
        match &cfg.wal {
            Some((path, interval)) => format!("On ({} @ {interval:?} flush)", path.display()),
            None => "Off".to_string(),
        },
        cfg.root_star_index,
        cfg.tpcc.num_items, cfg.tpcc.customers_per_district, cfg.tpcc.initial_orders_per_district,
    );

    let mut assigned: Vec<Vec<u32>> = vec![Vec::new(); num_terminals];
    for w in 1..=cfg.tpcc.num_warehouses {
        assigned[((w - 1) as usize) % num_terminals].push(w);
    }

    let history_seq = Arc::new(AtomicU64::new(0));

    // Population is sequential, on this (main) thread — deliberately not
    // parallelized across warehouses: this tree's structural-modification
    // (split) and GC/block-reuse paths are only exercised concurrently by
    // the timed OLTP/OLAP phase below, matching how the rest of this
    // project's benchmarks (see `mv_test::main_load`) load their initial
    // data set single-threaded before spawning concurrent workers.
    println!("Loading item catalog ({} items)...", cfg.tpcc.num_items);
    let load_start = Instant::now();
    populate_items(&tree, &cfg.tpcc);
    println!("Loaded item catalog in {:?}. Loading {} warehouse(s)...", load_start.elapsed(), cfg.tpcc.num_warehouses);

    let wh_load_start = Instant::now();
    for w in 1..=cfg.tpcc.num_warehouses {
        populate_warehouse(&tree, &cfg.tpcc, w, &history_seq);
    }
    println!("Loaded {} warehouse(s) in {:?}.", cfg.tpcc.num_warehouses, wh_load_start.elapsed());
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(num_terminals + num_olap + 1));
    let (scan_tx, scan_rx) = unbounded::<ScanResult>();

    let duration = cfg.duration;
    let affinity = cfg.affinity;
    let tpcc_cfg = cfg.tpcc;

    let terminal_handles: Vec<_> = (0..num_terminals).map(|t| {
        let tree = tree.clone();
        let cfg = tpcc_cfg;
        let my_ws = assigned[t].clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let history_seq = history_seq.clone();
        thread::spawn(move || terminal_thread(tree, cfg, my_ws, affinity, duration, stop, barrier, history_seq))
    }).collect();

    let olap_handles: Vec<_> = (0..num_olap).map(|_| {
        let tree = tree.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let mode = cfg.olap_mode.clone();
        let scan_tx = scan_tx.clone();
        thread::spawn(move || {
            barrier.wait();
            run_olap_worker(&tree, mode, &stop, &scan_tx);
        })
    }).collect();
    drop(scan_tx);

    // Releases at the same instant as every worker thread, once loading is
    // done — so the timed phase (and this wall-clock measurement) excludes
    // load time entirely.
    barrier.wait();
    let run_start = Instant::now();
    println!("Loading done. Running timed phase for {duration:?}...");
    thread::sleep(duration);
    stop.store(true, Relaxed);

    let terminal_stats: Vec<TerminalStats> = terminal_handles.into_iter().map(|h| h.join().unwrap()).collect();
    for h in olap_handles {
        let _ = h.join();
    }
    let actual_wall = run_start.elapsed();

    let mut scan_results = Vec::new();
    while let Ok(r) = scan_rx.try_recv() {
        scan_results.push(r);
    }

    write_results(&terminal_stats, &scan_results, duration, actual_wall);
}

fn num_olap_mode_summary(mode: &OlapMode) -> &'static str {
    match mode {
        OlapMode::OpenAndSleep { .. } => "open_and_sleep",
        OlapMode::ScanDelaySweep { .. } => "scan_delay_sweep",
        OlapMode::RepeatedFreshFullScan => "repeated_fresh_full_scan",
    }
}

fn write_results(
    terminal_stats: &[TerminalStats],
    scan_results: &[ScanResult],
    requested_duration: Duration,
    actual_wall: Duration,
) {
    let series_len = requested_duration.as_secs() as usize + 2;
    let mut per_sec = vec![0u64; series_len];
    let mut totals = [0u64; NUM_COUNTERS];
    for t in terminal_stats {
        for (i, v) in t.new_order_committed_per_sec.iter().enumerate() {
            per_sec[i] += v;
        }
        for i in 0..NUM_COUNTERS {
            totals[i] += t.totals[i];
        }
    }

    let _ = fs::remove_file("tpcc_oltp_timeseries.csv");
    let mut ts_file = OpenOptions::new().create(true).append(true).open("tpcc_oltp_timeseries.csv").unwrap();
    ts_file.write_all(b"elapsed_sec,new_order_committed\n").unwrap();
    for (sec, count) in per_sec.iter().enumerate() {
        ts_file.write_all(format!("{sec},{count}\n").as_bytes()).unwrap();
    }

    let _ = fs::remove_file("tpcc_scan.csv");
    let mut scan_file = OpenOptions::new().create(true).append(true).open("tpcc_scan.csv").unwrap();
    scan_file.write_all(b"mode,delay_secs,snapshot,scanned_tuples,latency_ns,tuples_per_sec\n").unwrap();
    for r in scan_results {
        scan_file.write_all(format!(
            "{},{},{},{},{},{:.2}\n",
            r.mode, r.delay_secs, r.snapshot, r.scanned_tuples, r.latency_ns, r.tuples_per_sec()
        ).as_bytes()).unwrap();
    }

    let new_order_total = totals[NO];
    let tpm_c = new_order_total as f64 / (actual_wall.as_secs_f64() / 60.0);

    println!("\n===== Results (timed phase: {actual_wall:?}) =====");
    for i in 0..NUM_COUNTERS {
        println!("{:<32} {}", COUNTER_NAMES[i], totals[i]);
    }
    println!("{:<32} {:.2}", "tpmC (New-Order/min)", tpm_c);
    println!("{:<32} {}", "OLAP scans/holds completed", scan_results.len());
    if !scan_results.is_empty() {
        let avg_tps = scan_results.iter().map(|r| r.tuples_per_sec()).sum::<f64>() / scan_results.len() as f64;
        println!("{:<32} {:.1}", "OLAP avg tuples/sec", avg_tps);
    }
    println!("Wrote tpcc_oltp_timeseries.csv and tpcc_scan.csv");
}

pub fn main_tpcc(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms.get(idx).and_then(|s| s.parse().ok()).unwrap_or(default)
    }

    let num_warehouses: u32 = arg(&parms, 2, 4);
    let num_terminals: usize = arg(&parms, 3, num_warehouses as usize);
    let duration_secs: u64 = arg(&parms, 4, 30);
    let affinity: bool = arg(&parms, 5, true);
    let gc: bool = arg(&parms, 6, true);
    let update_in_place: bool = arg(&parms, 7, false);
    let root_star_index = match parms.get(8).map(|s| s.as_str()).unwrap_or("fg") {
        "sk" => RootIndexType::SkipList,
        "ll" => RootIndexType::LinkedList,
        "bt" => RootIndexType::BTree,
        _ => RootIndexType::FrugalList,
    };
    let olap_mode_str = parms.get(9).map(|s| s.as_str()).unwrap_or("scan_sweep").to_string();
    let num_olap_threads: usize = arg(&parms, 10, 1);
    let olap_param: f64 = arg(&parms, 11, 10.0);
    let num_items: u32 = arg(&parms, 12, 100_000);
    let customers_per_district: u32 = arg(&parms, 13, 3_000);
    let initial_orders_per_district: u32 = arg(&parms, 14, 3_000);
    let wal_enabled: bool = arg(&parms, 15, false);
    let wal_path: String = parms.get(16).cloned().unwrap_or_else(|| "tpcc_wal.log".to_string());
    let wal_flush_ms: u64 = arg(&parms, 17, 5);

    let (olap_mode, num_olap_threads) = match olap_mode_str.as_str() {
        "none" => (OlapMode::RepeatedFreshFullScan, 0),
        "sleep" => (OlapMode::OpenAndSleep { hold: Duration::from_secs_f64(olap_param) }, num_olap_threads),
        "fresh" => (OlapMode::RepeatedFreshFullScan, num_olap_threads),
        _ => (
            OlapMode::ScanDelaySweep { delays: (0..=(olap_param.max(0.0) as u64)).map(Duration::from_secs).collect() },
            num_olap_threads,
        ),
    };

    let tpcc_cfg = TpccConfig {
        num_warehouses,
        districts_per_warehouse: 10,
        customers_per_district,
        num_items,
        initial_orders_per_district,
        initial_new_orders: (initial_orders_per_district * 3 / 10).max(1),
    };

    run_tpcc(DriverConfig {
        tpcc: tpcc_cfg,
        num_terminals,
        duration: Duration::from_secs(duration_secs),
        affinity,
        gc,
        update_in_place,
        root_star_index,
        olap_mode,
        num_olap_threads,
        wal: wal_enabled.then(|| (std::path::PathBuf::from(wal_path), Duration::from_millis(wal_flush_ms))),
    });
}
