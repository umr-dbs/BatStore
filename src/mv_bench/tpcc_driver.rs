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
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::unbounded;
use rand::prelude::*;

use crate::mv_bench::mem_stats::{MemSampler, DEFAULT_SAMPLE_INTERVAL};
use crate::mv_bench::olap_scan::{run_olap_worker, OlapMode, ScanResult};
use crate::mv_bench::tpcc_load::{populate_items, populate_regions_and_nations, populate_suppliers, populate_warehouse};
use crate::mv_bench::tpcc_schema::TpccConfig;
use crate::mv_bench::tpcc_schema::TpccDatabase;
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
    /// `Table::Warehouse`/`Table::District`'s leaf capacity — see
    /// `mv_bench::tpcc_schema::BigTreeSize`'s doc for the measured
    /// root-contention-vs-OLAP-scan-throughput trade-off each variant sits
    /// at.
    pub big_tree_size: crate::mv_bench::tpcc_schema::BigTreeSize,
    pub olap_mode: OlapMode,
    pub num_olap_threads: usize,
    /// Attaches a live WAL at this path *before* population, so the whole
    /// data set (not just the timed OLTP/OLAP phase) is durable — matching
    /// the referenced benchmarks, where logging is an always-on part of the
    /// storage engine under test, not something toggled on only for the
    /// measured phase. `None` disables WAL entirely (population and OLTP
    /// writes take the plain, unlogged path).
    pub wal: Option<(std::path::PathBuf, Duration)>,
    /// `None` (default): `cfg.wal`, if set, attaches via `enable_wal`
    /// (`WalBackend::Batched` — channel + one background writer thread).
    /// `Some(batch_size)`: attaches via `enable_wal_lockfree` instead
    /// (`WalBackend::LockFree` — every worker thread reserves its own byte
    /// range and writes it directly, batching its own records up to
    /// `batch_size` before each `pwrite`; see `mv_wal::lockfree_writer`'s
    /// and `mv_wal::backend`'s docs). Ignored entirely when `cfg.wal` is
    /// `None`.
    pub wal_lockfree_batch_size: Option<usize>,
    /// HTAP interference measurement: if set, runs a short OLTP-only
    /// sub-phase of this duration (same terminals, zero OLAP threads) right
    /// after loading and *before* the real timed phase, so the real phase's
    /// tpmC (measured with OLAP running) can be compared against an
    /// OLAP-free baseline from the *same* loaded data set. `None` skips it
    /// entirely (no extra threads, no extra wall-clock cost) — the default.
    pub htap_baseline: Option<Duration>,
    /// Directory the 3 result CSVs (`tpcc_oltp_timeseries.csv`,
    /// `tpcc_scan.csv`, `mem_stats.csv`) are written to. Defaults to `.` for
    /// the standalone `tpcc`/`tpch`/`htap` subcommands (unchanged cwd
    /// behavior); `mv_bench::suite` sets this to a dedicated per-experiment
    /// directory so a multi-run suite doesn't clobber itself.
    pub output_dir: PathBuf,
}

/// Everything `mv_bench::suite`'s `benchmark` orchestrator needs to fold one
/// `run_tpcc` invocation into `manifest.csv`, without having to re-parse
/// stdout. Standalone callers (`main_tpcc`/`main_tpch`/`main_htap`) simply
/// ignore this return value, exactly as they ignored `run_tpcc`'s prior `()`.
pub struct TpccRunSummary {
    pub tpm_c: f64,
    pub baseline_tpm_c: Option<f64>,
    pub totals: [u64; NUM_COUNTERS],
    pub scan_count: usize,
    pub avg_scan_tuples_per_sec: f64,
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
    db: Arc<TpccDatabase>,
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
                let outcome = tpcc_txn::new_order(&db, &cfg, home_w, allow_remote);
                record(&mut totals, NO, outcome);
                if outcome == TxnOutcome::Committed {
                    let idx = (start.elapsed().as_secs() as usize).min(new_order_committed_per_sec.len() - 1);
                    new_order_committed_per_sec[idx] += 1;
                }
            }
            46..=88 => {
                let outcome = tpcc_txn::payment(&db, &cfg, home_w, allow_remote, &history_seq);
                record(&mut totals, PAY, outcome);
            }
            89..=92 => {
                let outcome = tpcc_txn::order_status(&db, &cfg, home_w);
                record(&mut totals, OS, outcome);
            }
            93..=96 => {
                let d = tpcc_txn::delivery(&db, &cfg, home_w);
                totals[DELIV_DISTRICTS] += d.delivered_districts as u64;
                totals[DELIV_CONFLICTS] += d.conflicts as u64;
            }
            _ => {
                let outcome = tpcc_txn::stock_level(&db, &cfg, home_w, 15);
                record(&mut totals, SL, outcome);
            }
        }
    }

    TerminalStats { new_order_committed_per_sec, totals }
}

pub fn run_tpcc(cfg: DriverConfig) -> TpccRunSummary {
    assert!(cfg.tpcc.num_warehouses >= 1, "tpcc: num_warehouses must be >= 1");

    // See `mv_test::reset_restart_trace`'s doc: without this, a caller that
    // invokes `run_tpcc` more than once in the same process (e.g.
    // `tests/tpcc_wal_backend_bench.rs`'s backend-comparison loop) would
    // accumulate every prior run's restart-trace data into this run's dump,
    // unbounded, whenever `RESTART_TRACE` is on.
    crate::mv_test::reset_restart_trace();
    crate::mv_test::reset_scan_trace();

    let max_threads = crate::mv_tree::mvbt::default_max_workers().max(1);

    let mut num_terminals = cfg.num_terminals.max(1);
    if cfg.affinity {
        // Under strict affinity every terminal needs >= 1 owned warehouse.
        num_terminals = num_terminals.min(cfg.tpcc.num_warehouses as usize).max(1);
    }
    let mut num_olap = cfg.num_olap_threads;

    // The HTAP baseline sub-phase (if enabled) spawns its own `num_terminals`
    // OS threads before the real phase's — a *different* set of threads from
    // the real phase's terminals, each still permanently claiming its own
    // WorkerId (see module docs), so it doubles the terminal thread budget.
    let terminal_cost = if cfg.htap_baseline.is_some() { 2 } else { 1 };

    // +1: the main thread itself acquires a WorkerId too, since it does the
    // (sequential) data-set population directly via `dispatch_crud` before
    // any terminal/OLAP thread is spawned.
    if 1 + num_terminals * terminal_cost + num_olap > max_threads {
        println!(
            "!! 1 loader + {num_terminals} terminals{} + {num_olap} OLAP threads > max_workers ({max_threads} = num_cpus); clamping.",
            if terminal_cost == 2 { " (x2: HTAP baseline sub-phase)" } else { "" }
        );
        num_terminals = (max_threads.saturating_sub(2) / terminal_cost).max(1);
        num_olap = max_threads.saturating_sub(1 + num_terminals * terminal_cost);
    }

    fs::create_dir_all(&cfg.output_dir)
        .unwrap_or_else(|e| panic!("tpcc: failed to create output_dir {}: {e}", cfg.output_dir.display()));
    let mem_sampler = MemSampler::start(cfg.output_dir.join("mem_stats.csv"), DEFAULT_SAMPLE_INTERVAL);

    let db = Arc::new(TpccDatabase::new_with_big_tree_size(cfg.root_star_index, cfg.big_tree_size));
    if cfg.gc {
        db.enable_gc(cfg.update_in_place);
    }

    if let Some((wal_path, flush_interval)) = &cfg.wal {
        let _ = fs::remove_file(wal_path);
        match cfg.wal_lockfree_batch_size {
            Some(batch_size) => db.enable_wal_lockfree(wal_path, *flush_interval, batch_size).expect("failed to attach lock-free WAL"),
            None => db.enable_wal(wal_path, *flush_interval).expect("failed to attach WAL"),
        }
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
         - items/customers/orders per district = {}/{}/{}\n\
         - CH-benCHmark suppliers = {}\n\
         - HTAP baseline         = {}",
        cfg.tpcc.num_warehouses,
        num_olap_mode_summary(&cfg.olap_mode),
        if cfg.affinity { "warehouse affinity (0% remote)" } else { "cross warehouse" },
        cfg.duration,
        cfg.gc, cfg.update_in_place,
        match (&cfg.wal, cfg.wal_lockfree_batch_size) {
            (Some((path, interval)), None) => format!("On, batched ({} @ {interval:?} flush)", path.display()),
            (Some((path, interval)), Some(batch_size)) => format!("On, lock-free batch={batch_size} ({} @ {interval:?} flush)", path.display()),
            (None, _) => "Off".to_string(),
        },
        cfg.root_star_index,
        cfg.tpcc.num_items, cfg.tpcc.customers_per_district, cfg.tpcc.initial_orders_per_district,
        cfg.tpcc.num_suppliers,
        match cfg.htap_baseline {
            Some(d) => format!("On ({d:?} OLTP-only sub-phase)"),
            None => "Off".to_string(),
        },
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
    println!("Loading CH-benCHmark dimension tables (5 regions, 25 nations, {} suppliers)...", cfg.tpcc.num_suppliers);
    let ch_load_start = Instant::now();
    populate_regions_and_nations(&db);
    populate_suppliers(&db, &cfg.tpcc);
    println!("Loaded CH-benCHmark dimension tables in {:?}.", ch_load_start.elapsed());

    println!("Loading item catalog ({} items)...", cfg.tpcc.num_items);
    let load_start = Instant::now();
    populate_items(&db, &cfg.tpcc);
    println!("Loaded item catalog in {:?}. Loading {} warehouse(s)...", load_start.elapsed(), cfg.tpcc.num_warehouses);

    let wh_load_start = Instant::now();
    for w in 1..=cfg.tpcc.num_warehouses {
        populate_warehouse(&db, &cfg.tpcc, w, &history_seq);
    }
    println!("Loaded {} warehouse(s) in {:?}.", cfg.tpcc.num_warehouses, wh_load_start.elapsed());

    // HTAP interference baseline (see `DriverConfig::htap_baseline` docs): a
    // short OLTP-only sub-phase, using the *same* loaded data set, same
    // `num_terminals`/affinity assignment, and same `history_seq` counter as
    // the real timed phase below — so its tpmC is a fair OLAP-free
    // comparison point for the real phase's tpmC (measured with OLAP
    // running), not a separate/differently-configured run.
    let baseline_tpm_c = cfg.htap_baseline.map(|baseline_duration| {
        println!("Running HTAP baseline (OLTP-only, no OLAP) for {baseline_duration:?}...");
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(num_terminals + 1));

        let handles: Vec<_> = (0..num_terminals).map(|t| {
            let db = db.clone();
            let tpcc_cfg = cfg.tpcc;
            let my_ws = assigned[t].clone();
            let affinity = cfg.affinity;
            let stop = stop.clone();
            let barrier = barrier.clone();
            let history_seq = history_seq.clone();
            thread::spawn(move || terminal_thread(db, tpcc_cfg, my_ws, affinity, baseline_duration, stop, barrier, history_seq))
        }).collect();

        barrier.wait();
        let start = Instant::now();
        thread::sleep(baseline_duration);
        stop.store(true, Relaxed);

        let stats: Vec<TerminalStats> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let wall = start.elapsed();

        let new_order_total: u64 = stats.iter().map(|s| s.totals[NO]).sum();
        let tpm_c = new_order_total as f64 / (wall.as_secs_f64() / 60.0);
        println!("HTAP baseline done: {new_order_total} New-Order commits in {wall:?} ({tpm_c:.2} tpmC, no OLAP).");
        tpm_c
    });

    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(num_terminals + num_olap + 1));
    let (scan_tx, scan_rx) = unbounded::<ScanResult>();

    let duration = cfg.duration;
    let affinity = cfg.affinity;
    let tpcc_cfg = cfg.tpcc;

    let terminal_handles: Vec<_> = (0..num_terminals).map(|t| {
        let db = db.clone();
        let cfg = tpcc_cfg;
        let my_ws = assigned[t].clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let history_seq = history_seq.clone();
        thread::spawn(move || terminal_thread(db, cfg, my_ws, affinity, duration, stop, barrier, history_seq))
    }).collect();

    let olap_handles: Vec<_> = (0..num_olap).map(|_| {
        let db = db.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let mode = cfg.olap_mode.clone();
        let scan_tx = scan_tx.clone();
        thread::spawn(move || {
            barrier.wait();
            run_olap_worker(&db, mode, &stop, &scan_tx);
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

    mem_sampler.stop();

    // Plain global atomics (no thread-local merge-on-drop, unlike
    // `RESTART_TRACE` below), so this is safe to read any time — placed
    // after the join purely to report a fully-settled count. No-op when
    // `SCAN_TRACE` is off.
    if crate::mv_test::SCAN_TRACE {
        crate::mv_test::dump_scan_trace();
    }

    // All terminal/OLAP worker threads are joined above, so every thread's
    // `RestartLocal` TLS has already torn down and merged into the global
    // aggregate by this point (see `mv_test::RestartLocal`'s doc) — safe to
    // dump now. No-op (writes an empty file) when `RESTART_TRACE` is off.
    if crate::mv_test::RESTART_TRACE {
        crate::mv_test::dump_restart_trace(
            cfg.output_dir.join("tpcc_restart_trace.csv").to_str().unwrap());
        crate::mv_test::dump_attempt_histogram(
            cfg.output_dir.join("tpcc_attempt_histogram.csv").to_str().unwrap());
        use crate::mv_bench::tpcc_schema::{BigTreeOp, Table, TpccKey, TpccRow};

        struct AddrOp;
        impl BigTreeOp for AddrOp {
            type Output = usize;
            fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(
                self,
                tree: &crate::mv_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>,
            ) -> usize {
                tree as *const _ as usize
            }
        }
        let mut table_names = db.db.table_names_by_addr();
        table_names.push((db.dispatch_big(Table::Warehouse, AddrOp), "warehouse".to_string()));
        table_names.push((db.dispatch_big(Table::District, AddrOp), "district".to_string()));
        crate::mv_test::dump_root_restarts_by_table(
            cfg.output_dir.join("tpcc_root_restarts_by_table.csv").to_str().unwrap(),
            &table_names);
    }

    write_results(&terminal_stats, &scan_results, duration, actual_wall, baseline_tpm_c, &cfg.output_dir)
}

fn num_olap_mode_summary(mode: &OlapMode) -> &'static str {
    match mode {
        OlapMode::OpenAndSleep { .. } => "open_and_sleep",
        OlapMode::ScanDelaySweep { .. } => "scan_delay_sweep",
        OlapMode::RepeatedFreshFullScan => "repeated_fresh_full_scan",
        OlapMode::ChBenchmark { .. } => "ch_benchmark",
        OlapMode::ChQ1 { .. } => "ch_q1",
        OlapMode::ChQ6 { .. } => "ch_q6",
    }
}

fn write_results(
    terminal_stats: &[TerminalStats],
    scan_results: &[ScanResult],
    requested_duration: Duration,
    actual_wall: Duration,
    baseline_tpm_c: Option<f64>,
    out_dir: &Path,
) -> TpccRunSummary {
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

    let oltp_ts_path = out_dir.join("tpcc_oltp_timeseries.csv");
    let _ = fs::remove_file(&oltp_ts_path);
    let mut ts_file = OpenOptions::new().create(true).append(true).open(&oltp_ts_path).unwrap();
    ts_file.write_all(b"elapsed_sec,new_order_committed\n").unwrap();
    for (sec, count) in per_sec.iter().enumerate() {
        ts_file.write_all(format!("{sec},{count}\n").as_bytes()).unwrap();
    }

    let scan_path = out_dir.join("tpcc_scan.csv");
    let _ = fs::remove_file(&scan_path);
    let mut scan_file = OpenOptions::new().create(true).append(true).open(&scan_path).unwrap();
    scan_file.write_all(b"mode,elapsed_secs,delay_secs,snapshot,scanned_tuples,latency_ns,tuples_per_sec,summary,staleness_versions\n").unwrap();
    for r in scan_results {
        scan_file.write_all(format!(
            "{},{:.3},{},{},{},{},{:.2},{},{}\n",
            r.mode, r.elapsed_secs, r.delay_secs, r.snapshot, r.scanned_tuples, r.latency_ns, r.tuples_per_sec(),
            r.summary.map(|s| format!("{s:.2}")).unwrap_or_default(),
            r.staleness_versions.map(|s| s.to_string()).unwrap_or_default(),
        ).as_bytes()).unwrap();
    }

    let new_order_total = totals[NO];
    let tpm_c = new_order_total as f64 / (actual_wall.as_secs_f64() / 60.0);

    println!("\n===== Results (timed phase: {actual_wall:?}) =====");
    for i in 0..NUM_COUNTERS {
        println!("{:<32} {}", COUNTER_NAMES[i], totals[i]);
    }
    println!("{:<32} {:.2}", "tpmC (New-Order/min)", tpm_c);
    if let Some(baseline) = baseline_tpm_c {
        let interference_pct = if baseline > 0.0 { (baseline - tpm_c) / baseline * 100.0 } else { 0.0 };
        println!("{:<32} {:.2}", "tpmC (HTAP baseline, no OLAP)", baseline);
        println!("{:<32} {:.1}%", "OLTP interference from OLAP", interference_pct);
    }
    println!("{:<32} {}", "OLAP scans/holds completed", scan_results.len());
    if !scan_results.is_empty() {
        let avg_tps = scan_results.iter().map(|r| r.tuples_per_sec()).sum::<f64>() / scan_results.len() as f64;
        println!("{:<32} {:.1}", "OLAP avg tuples/sec", avg_tps);
    }
    let staleness: Vec<u64> = scan_results.iter().filter_map(|r| r.staleness_versions).collect();
    if !staleness.is_empty() {
        let avg = staleness.iter().sum::<u64>() as f64 / staleness.len() as f64;
        let max = staleness.iter().max().unwrap();
        println!("{:<32} {:.1} (max {max})", "HTAP staleness (versions, avg)", avg);
    }
    println!("Wrote {} and {}", oltp_ts_path.display(), scan_path.display());

    let avg_scan_tuples_per_sec = if scan_results.is_empty() {
        0.0
    } else {
        scan_results.iter().map(|r| r.tuples_per_sec()).sum::<f64>() / scan_results.len() as f64
    };

    TpccRunSummary {
        tpm_c,
        baseline_tpm_c,
        totals,
        scan_count: scan_results.len(),
        avg_scan_tuples_per_sec,
    }
}

pub fn main_tpcc(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms.get(idx).and_then(|s| s.parse().ok()).unwrap_or(default)
    }

    let num_warehouses: u32 = arg(&parms, 2, num_cpus::get_physical() as u32);
    let num_terminals: usize = arg(&parms, 3, num_cpus::get());
    let duration_secs: u64 = arg(&parms, 4, 30);
    let affinity: bool = arg(&parms, 5, false);
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
    let ch_region: String = parms.get(18).cloned().unwrap_or_else(|| "EUROPE".to_string());
    let num_suppliers: u32 = arg(&parms, 19, 10_000);
    let htap_baseline_secs: u64 = arg(&parms, 20, 0);
    // Table::Warehouse/Table::District's leaf capacity — see
    // tpcc_schema::BigTreeSize's doc for the measured trade-off each named
    // size sits at (root-contention reduction vs. OLAP scan throughput).
    let big_tree_size = match parms.get(21).map(|s| s.as_str()).unwrap_or("32kib") {
        "1kib" => crate::mv_bench::tpcc_schema::BigTreeSize::KiB1,
        "2kib" => crate::mv_bench::tpcc_schema::BigTreeSize::KiB2,
        "4kib" => crate::mv_bench::tpcc_schema::BigTreeSize::KiB4,
        "8kib" => crate::mv_bench::tpcc_schema::BigTreeSize::KiB8,
        "16kib" => crate::mv_bench::tpcc_schema::BigTreeSize::KiB16,
        "64kib" => crate::mv_bench::tpcc_schema::BigTreeSize::KiB64,
        "512kib" => crate::mv_bench::tpcc_schema::BigTreeSize::KiB512,
        _ => crate::mv_bench::tpcc_schema::BigTreeSize::KiB32,
    };

    let (olap_mode, num_olap_threads) = match olap_mode_str.as_str() {
        "none" => (OlapMode::RepeatedFreshFullScan, 0),
        "sleep" => (OlapMode::OpenAndSleep { hold: Duration::from_secs_f64(olap_param) }, num_olap_threads),
        "fresh" => (OlapMode::RepeatedFreshFullScan, num_olap_threads),
        // Wide-open by default: every row loaded gets its date fields
        // (`o_entry_d`, `ol_delivery_d`, ...) stamped with the load's actual
        // wall-clock time (see `tpcc_random::now_millis`), not spread across
        // the simulated years a real TPC-H date filter would assume — so an
        // unrestricted range is what makes these queries see the whole
        // loaded data set by default. Pass a real i64-millis range here to
        // exercise actual date selectivity instead.
        "ch" => (
            OlapMode::ChBenchmark { region_name: ch_region, date_lo: i64::MIN, date_hi: i64::MAX },
            num_olap_threads,
        ),
        "ch_q1" => (OlapMode::ChQ1 { delivered_before: i64::MAX }, num_olap_threads),
        "ch_q6" => (OlapMode::ChQ6 {
            date_lo: i64::MIN,
            date_hi: i64::MAX,
            max_qty: 24,
        }, num_olap_threads),
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
        num_suppliers,
    };

    run_tpcc(DriverConfig {
        tpcc: tpcc_cfg,
        num_terminals,
        duration: Duration::from_secs(duration_secs),
        affinity,
        gc,
        update_in_place,
        root_star_index,
        big_tree_size,
        olap_mode,
        num_olap_threads,
        wal: wal_enabled.then(|| (std::path::PathBuf::from(wal_path), Duration::from_millis(wal_flush_ms))),
        wal_lockfree_batch_size: None,
        htap_baseline: (htap_baseline_secs > 0).then(|| Duration::from_secs(htap_baseline_secs)),
        output_dir: PathBuf::from("."),
    });
}

/// Shared defaults for the `tpch`/`htap` one-command presets below:
/// standard TPC-C/CH-benCHmark scale (`TpccConfig::default()` — 3,000
/// customers/orders per district, 100,000 items, 10,000 suppliers),
/// warehouse affinity, GC on, no WAL. Only what actually differs between the
/// two presets (OLAP mode, OLAP thread count, HTAP baseline) is left as a
/// parameter — the whole point of these presets is that the caller
/// shouldn't have to think about anything else.
fn standard_driver_config(
    num_warehouses: u32,
    duration: Duration,
    olap_mode: OlapMode,
    num_olap_threads: usize,
    htap_baseline: Option<Duration>,
) -> DriverConfig {
    DriverConfig {
        tpcc: TpccConfig { num_warehouses, ..TpccConfig::default() },
        num_terminals: num_warehouses as usize,
        duration,
        affinity: true,
        gc: true,
        update_in_place: false,
        root_star_index: RootIndexType::FrugalList,
        big_tree_size: Default::default(),
        olap_mode,
        num_olap_threads,
        wal: None,
        wal_lockfree_batch_size: None,
        htap_baseline,
        output_dir: PathBuf::from("."),
    }
}

/// One-command CH-benCHmark preset ("typical TPC-H" run in this harness):
/// the standard TPC-C OLTP mix running concurrently with the 4 implemented
/// CH-benCHmark analytical queries (`tpch_queries`) in rotation — see
/// `OlapMode::ChBenchmark`. There's no standalone "TPC-H alone" mode:
/// CH-benCHmark's whole premise is TPC-H-style queries layered on the live
/// TPC-C schema, so this mixed run *is* what "run TPC-H" means here.
///
/// Args: `[num_warehouses=4] [duration_secs=60] [num_olap_threads=1]
/// [region_name=EUROPE]` — for full control over every other TPC-C/CH
/// parameter, use `tpcc ... 9=ch ...` directly (see `main_tpcc`).
pub fn main_tpch(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms.get(idx).and_then(|s| s.parse().ok()).unwrap_or(default)
    }

    let num_warehouses: u32 = arg(&parms, 2, 4);
    let duration_secs: u64 = arg(&parms, 3, 60);
    let num_olap_threads: usize = arg(&parms, 4, 1);
    let region_name: String = parms.get(5).cloned().unwrap_or_else(|| "EUROPE".to_string());

    run_tpcc(standard_driver_config(
        num_warehouses,
        Duration::from_secs(duration_secs),
        OlapMode::ChBenchmark { region_name, date_lo: i64::MIN, date_hi: i64::MAX },
        num_olap_threads,
        None,
    ));
}

/// One-command HTAP preset: identical to [`main_tpch`] but additionally
/// enables the OLTP-only baseline sub-phase (`DriverConfig::htap_baseline`),
/// so the report includes the HTAP-specific interference (%) and
/// freshness/staleness (versions) metrics — see `tpcc_driver`/`tpch_queries`
/// module docs.
///
/// Args: `[num_warehouses=4] [duration_secs=60] [num_olap_threads=1]
/// [baseline_secs=15] [region_name=EUROPE]`.
pub fn main_htap(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms.get(idx).and_then(|s| s.parse().ok()).unwrap_or(default)
    }

    let num_warehouses: u32 = arg(&parms, 2, 4);
    let duration_secs: u64 = arg(&parms, 3, 60);
    let num_olap_threads: usize = arg(&parms, 4, 1);
    let baseline_secs: u64 = arg(&parms, 5, 15);
    let region_name: String = parms.get(6).cloned().unwrap_or_else(|| "EUROPE".to_string());

    run_tpcc(standard_driver_config(
        num_warehouses,
        Duration::from_secs(duration_secs),
        OlapMode::ChBenchmark { region_name, date_lo: i64::MIN, date_hi: i64::MAX },
        num_olap_threads,
        Some(Duration::from_secs(baseline_secs)),
    ));
}
