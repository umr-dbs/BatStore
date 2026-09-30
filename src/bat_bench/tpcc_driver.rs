//! Benchmark driver: loads a TPC-C data set, then runs the standard 5-txn
//! OLTP mix concurrently with an OLAP scan workload for a fixed wall-clock
//! duration, following the mixed TPC-C+Scan methodology used to evaluate
//! MVCC storage engines (Alhomssi & Leis, VLDB'23) — see `bat_bench` module
//! docs. Reports per-second OLTP throughput (to reproduce "throughput over
//! time"/"vs. thread count" plots) and per-scan OLAP latency/throughput (to
//! reproduce "scan throughput vs. delay" plots).
//!
//! Threading constraint: every distinct OS thread that ever calls into the
//! tree (every terminal + every OLAP thread) permanently consumes one slot
//! of the tree's fixed `WorkerId` pool. Size that pool at construction for
//! every requested terminal, OLAP thread, loader and idle-compaction worker,
//! including a separate set of terminals when an HTAP baseline is enabled.
//! CPU count does not limit the requested concurrency.

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

use crate::bat_bench::mem_stats::{DEFAULT_SAMPLE_INTERVAL, MemSampler};
use crate::bat_bench::olap_scan::{OlapMode, ScanResult, run_olap_worker};
use crate::bat_bench::tpcc_load::{
    populate_items, populate_regions_and_nations, populate_suppliers, populate_warehouse,
};
use crate::bat_bench::tpcc_random::now_millis;
use crate::bat_bench::tpcc_schema::{TpccConfig, TpccDatabase, htap_query_date_bounds};
#[cfg(feature = "gc-stats")]
use crate::bat_bench::tpcc_schema::{BigTreeOp, Table, TpccKey, TpccRow};
#[cfg(feature = "gc-stats")]
use crate::bat_gc::tracker_handle::GcStats;
use crate::bat_bench::tpcc_txn::{self, TxnOutcome};
use crate::bat_root::index_root::RootIndexType;
use crate::bat_tree::idle_compaction::{DEFAULT_VACUUM_DEAD_RATIO, DEFAULT_VACUUM_SWEEP_INTERVAL};

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
    /// `bat_bench::tpcc_schema::BigTreeSize`'s doc for the measured
    /// root-contention-vs-OLAP-scan-throughput trade-off each variant sits
    /// at.
    pub big_tree_size: crate::bat_bench::tpcc_schema::BigTreeSize,
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
    /// `batch_size` before each `pwrite`; see `bat_wal::lockfree_writer`'s
    /// and `bat_wal::backend`'s docs). Ignored entirely when `cfg.wal` is
    /// `None`.
    pub wal_lockfree_batch_size: Option<usize>,
    /// HTAP interference measurement: if set, runs a short OLTP-only
    /// sub-phase of this duration (same terminals, zero OLAP threads) right
    /// after loading and *before* the real timed phase, so the real phase's
    /// tpmC (measured with OLAP running) can be compared against an
    /// OLAP-free baseline from the *same* loaded data set. `None` skips it
    /// entirely (no extra threads, no extra wall-clock cost) — the default.
    pub htap_baseline: Option<Duration>,
    /// Idle/proactive compaction (`bat_tree::idle_compaction`): `Some((dead_ratio_threshold,
    /// sweep_interval))` spawns one extra background thread that repeatedly
    /// sweeps every table (`Table::ALL`), forcing a compaction on any leaf
    /// whose dead/(active+dead) ratio is at or above `dead_ratio_threshold`,
    /// sleeping `sweep_interval` between sweeps — see that module's doc for
    /// why this exists: a read-heavy table (few, infrequent writes to any
    /// one leaf) can otherwise sit at a garbage-inflated ratio indefinitely,
    /// since nothing on the ordinary write path ever revisits such a leaf.
    /// `None` (default) disables it entirely — no extra thread, no extra
    /// `WorkerId` cost, unchanged behavior.
    pub idle_compaction: Option<(f64, Duration)>,
    /// Assigns `Table::OrderLine` a shared scan-worker pool (see
    /// `scan_pool::ScanWorkerPool`'s and `TpccDatabase::enable_scan_pool`'s
    /// docs) of this many *total* threads, for any OLAP mode that scans
    /// `OrderLine` (via `TpccTxn::range`/`range_count`, or `OlapMode::ChQ1`/
    /// `ChQ6`'s own partitioners) to fan its queries out across. This is
    /// the pool's whole capacity, not what any one query asks for: `run_tpcc`
    /// also tells the pool how many OLAP threads (`num_olap_threads`) are
    /// expected to share it, so `ScanWorkerPool::fair_query_fanout` can
    /// divide this number evenly across them — see
    /// that method's doc.
    ///
    /// `None`/`Some(0)` disables it entirely: no pool, every query runs
    /// sequentially, unchanged from every prior behavior — this struct's
    /// own field-by-field construction means there's no single "unset"
    /// default here, but `main_tpcc`'s CLI parsing auto-sizes this whenever
    /// its own arg is omitted entirely, there's at least one OLAP thread,
    /// *and* the population is estimated large enough for the pool to
    /// actually pay off (`parallel_scan::MIN_ROWS_FOR_SCAN_POOL` — below
    /// that, the pool's own per-job overhead costs more than sequential
    /// just takes, see that constant's doc for the measurement). That
    /// auto-sizing is `num_cpus.max(scan_pool::DEFAULT_QUERY_FANOUT *
    /// num_olap_threads)`, not just `num_cpus` — guaranteeing every OLAP
    /// thread at least `DEFAULT_QUERY_FANOUT` workers of its own even when
    /// there are more OLAP threads than cores, rather than letting
    /// `fair_query_fanout` divide a `num_cpus`-sized pool down to a
    /// too-thin (or, past `num_cpus / 2` callers, `None`, i.e. no
    /// parallelism at all) share. Any `Some(n)` is floored to 2 (a "pool" of
    /// 1 buys no parallelism over the sequential path, see
    /// `ScanWorkerPool::spawn`'s doc).
    ///
    /// Unlike every other thread `run_tpcc` budgets, this one is
    /// deliberately *not* counted against `max_workers`: a pool worker
    /// thread never calls `tree.worker_id()` (see `bat_sync::worker::
    /// READ_ONLY_SCAN_WORKER_ID`'s doc for why that's sound for a
    /// pure-reader thread), so it never draws from the tree's fixed
    /// `WorkerRegistry` and this can safely oversubscribe past the
    /// machine's core count — sizing it generously costs nothing but idle,
    /// blocked (not spinning) threads, since a query's own fanout is
    /// capped by its fair share regardless of the pool's total size. One
    /// pool total regardless of `num_olap_threads`, since every OLAP
    /// thread shares it rather than each spawning its own. A query only
    /// ever actually queues on this pool while its fair share has spare
    /// capacity — `parallel_scan::q1_parallel`/`q6_parallel` call
    /// `ScanWorkerPool::try_dispatch`, so a busy pool (or a fair share
    /// below 2 — too many OLAP threads sharing it, see
    /// `fair_query_fanout`'s doc) makes the calling OLAP thread run the
    /// scan itself instead of waiting in line.
    pub scan_pool_workers: Option<usize>,
    /// Directory the 3 result CSVs (`tpcc_oltp_timeseries.csv`,
    /// `tpcc_scan.csv`, `mem_stats.csv`) are written to. Defaults to `.` for
    /// the standalone `tpcc`/`tpch`/`htap` subcommands (unchanged cwd
    /// behavior); `bat_bench::suite` sets this to a dedicated per-experiment
    /// directory so a multi-run suite doesn't clobber itself.
    pub output_dir: PathBuf,
}

/// Everything `bat_bench::suite`'s `benchmark` orchestrator needs to fold one
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
    "new_order_committed",
    "new_order_conflict",
    "new_order_user_abort",
    "payment_committed",
    "payment_conflict",
    "payment_user_abort",
    "order_status_committed",
    "order_status_conflict",
    "order_status_user_abort",
    "stock_level_committed",
    "stock_level_conflict",
    "stock_level_user_abort",
    "delivery_districts_delivered",
    "delivery_conflicts",
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
                    let idx = (start.elapsed().as_secs() as usize)
                        .min(new_order_committed_per_sec.len() - 1);
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

    TerminalStats {
        new_order_committed_per_sec,
        totals,
    }
}

#[cfg(feature = "tpcc-tree-stats")]
#[derive(Clone, Debug)]
pub struct TreeStatsRunConfig {
    pub warmup: Duration,
}

pub fn run_tpcc(cfg: DriverConfig) -> TpccRunSummary {
    #[cfg(feature = "tpcc-tree-stats")]
    {
        return run_tpcc_impl(cfg, None);
    }
    #[cfg(not(feature = "tpcc-tree-stats"))]
    {
        run_tpcc_impl(cfg)
    }
}

#[cfg(feature = "tpcc-tree-stats")]
pub fn run_tpcc_with_tree_stats(
    cfg: DriverConfig,
    stats: TreeStatsRunConfig,
) -> TpccRunSummary {
    run_tpcc_impl(cfg, Some(stats))
}

fn run_tpcc_impl(
    mut cfg: DriverConfig,
    #[cfg(feature = "tpcc-tree-stats")] tree_stats: Option<TreeStatsRunConfig>,
) -> TpccRunSummary {
    let historic = matches!(&cfg.olap_mode, OlapMode::RepeatedHistoricFullScan);
    if historic {
        cfg.gc = false;
        cfg.update_in_place = false;
        cfg.idle_compaction = None;
    }
    assert!(
        cfg.tpcc.num_warehouses >= 1,
        "tpcc: num_warehouses must be >= 1"
    );

    crate::bat_test::reset_restart_trace();
    crate::bat_test::reset_scan_trace();

    assert!(cfg.num_terminals > 0, "tpcc: num_terminals must be >= 1");
    let num_terminals = cfg.num_terminals;
    if cfg.affinity {
        assert!(
            num_terminals <= cfg.tpcc.num_warehouses as usize,
            "tpcc: warehouse affinity requires at least one warehouse per terminal; increase num_warehouses",
        );
    }
    let num_olap = cfg.num_olap_threads;

    #[cfg(feature = "tpcc-tree-stats")]
    if tree_stats.is_some() {
        assert!(
            cfg.htap_baseline.is_none(),
            "tpcc tree-stats run has its own warm-up; HTAP baseline must be disabled"
        );
    }

    let terminal_sets = 1
        + usize::from(cfg.htap_baseline.is_some())
        + {
            #[cfg(feature = "tpcc-tree-stats")]
            {
                usize::from(tree_stats.is_some())
            }
            #[cfg(not(feature = "tpcc-tree-stats"))]
            {
                0
            }
        };
    let terminal_cost = terminal_sets;

    let olap_thread_cost = 1;

    let idle_compaction_cost = if cfg.idle_compaction.is_some() { 2 } else { 0 };

    let fixed_cost = 1 + idle_compaction_cost;
    fs::create_dir_all(&cfg.output_dir).unwrap_or_else(|e| {
        panic!(
            "tpcc: failed to create output_dir {}: {e}",
            cfg.output_dir.display()
        )
    });
    let mem_sampler = MemSampler::start(
        cfg.output_dir.join("mem_stats.csv"),
        DEFAULT_SAMPLE_INTERVAL,
    );

    let worker_capacity = fixed_cost + num_terminals * terminal_cost + num_olap * olap_thread_cost;
    assert!(
        worker_capacity <= u16::MAX as usize,
        "tpcc: requested concurrency exceeds WorkerId capacity"
    );
    let db = Arc::new(match &cfg.wal {
        Some((wal_path, flush_interval)) => {
            let _ = fs::remove_file(wal_path);
            TpccDatabase::new_with_big_tree_size_and_max_workers_and_wal(
                cfg.root_star_index,
                cfg.big_tree_size,
                worker_capacity,
                wal_path,
                *flush_interval,
                cfg.wal_lockfree_batch_size,
            )
            .expect("failed to configure WAL at database construction")
        }
        None => TpccDatabase::new_with_big_tree_size_and_max_workers(
            cfg.root_star_index,
            cfg.big_tree_size,
            worker_capacity,
        ),
    });
    if historic {
        db.allow_historic_query(true);
    } else if cfg.gc {
        db.enable_gc(cfg.update_in_place, None);
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
        if cfg.affinity {
            "warehouse affinity (0% remote)"
        } else {
            "cross warehouse"
        },
        cfg.duration,
        cfg.gc,
        cfg.update_in_place,
        match (&cfg.wal, cfg.wal_lockfree_batch_size) {
            (Some((path, interval)), None) =>
                format!("On, batched ({} @ {interval:?} flush)", path.display()),
            (Some((path, interval)), Some(batch_size)) => format!(
                "On, lock-free batch={batch_size} ({} @ {interval:?} flush)",
                path.display()
            ),
            (None, _) => "Off".to_string(),
        },
        cfg.root_star_index,
        cfg.tpcc.num_items,
        cfg.tpcc.customers_per_district,
        cfg.tpcc.initial_orders_per_district,
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

    #[cfg(feature = "tpcc-tree-stats")]
    if tree_stats.is_some() {
        for filename in [
            "node_filling.csv",
            "tree_summary.csv",
            "smo_counts.csv",
            "run_metadata.json",
            "experiment_summary.txt",
        ] {
            let _ = fs::remove_file(cfg.output_dir.join(filename));
        }
    }

    println!(
        "Loading CH-benCHmark dimension tables (5 regions, 25 nations, {} suppliers)...",
        cfg.tpcc.num_suppliers
    );
    let ch_load_start = Instant::now();
    populate_regions_and_nations(&db);
    populate_suppliers(&db, &cfg.tpcc);
    println!(
        "Loaded CH-benCHmark dimension tables in {:?}.",
        ch_load_start.elapsed()
    );

    println!("Loading item catalog ({} items)...", cfg.tpcc.num_items);
    let load_start = Instant::now();
    populate_items(&db, &cfg.tpcc);
    println!(
        "Loaded item catalog in {:?}. Loading {} warehouse(s)...",
        load_start.elapsed(),
        cfg.tpcc.num_warehouses
    );

    let wh_load_start = Instant::now();
    for w in 1..=cfg.tpcc.num_warehouses {
        populate_warehouse(&db, &cfg.tpcc, w, &history_seq);
    }
    println!(
        "Loaded {} warehouse(s) in {:?}.",
        cfg.tpcc.num_warehouses,
        wh_load_start.elapsed()
    );

    #[cfg(feature = "tpcc-tree-stats")]
    let stats_after_load = if tree_stats.is_some() {
        crate::bat_bench::tpcc_tree_stats::write_checkpoint(&db, &cfg.output_dir, "after_load")
            .expect("failed to write after-load TPC-C tree audit");
        Some(crate::bat_bench::tpcc_tree_stats::snapshot_smos(&db))
    } else {
        None
    };

    #[cfg(feature = "tpcc-tree-stats")]
    let (stats_after_warmup, warmup_committed) = if let Some(stats_cfg) = &tree_stats {
        println!("Running tree-stats warm-up for {:?}...", stats_cfg.warmup);
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(num_terminals + 1));
        let handles: Vec<_> = (0..num_terminals)
            .map(|t| {
                let db = db.clone();
                let tpcc_cfg = cfg.tpcc;
                let my_ws = assigned[t].clone();
                let affinity = cfg.affinity;
                let stop = stop.clone();
                let barrier = barrier.clone();
                let history_seq = history_seq.clone();
                let duration = stats_cfg.warmup;
                thread::spawn(move || {
                    terminal_thread(
                        db,
                        tpcc_cfg,
                        my_ws,
                        affinity,
                        duration,
                        stop,
                        barrier,
                        history_seq,
                    )
                })
            })
            .collect();
        barrier.wait();
        thread::sleep(stats_cfg.warmup);
        stop.store(true, Relaxed);
        let warmup_stats: Vec<TerminalStats> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        crate::bat_bench::tpcc_tree_stats::write_checkpoint(
            &db,
            &cfg.output_dir,
            "after_warmup",
        )
        .expect("failed to write after-warmup TPC-C tree audit");
        (
            Some(crate::bat_bench::tpcc_tree_stats::snapshot_smos(&db)),
            committed_transaction_units(&warmup_stats),
        )
    } else {
        (None, 0)
    };
    #[cfg(feature = "gc-stats")]
    write_gc_stats(&db, &cfg.output_dir, "gc_stats_after_load.csv");

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

    let terminal_handles: Vec<_> = (0..num_terminals)
        .map(|t| {
            let db = db.clone();
            let cfg = tpcc_cfg;
            let my_ws = assigned[t].clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            let history_seq = history_seq.clone();
            thread::spawn(move || {
                terminal_thread(
                    db,
                    cfg,
                    my_ws,
                    affinity,
                    duration,
                    stop,
                    barrier,
                    history_seq,
                )
            })
        })
        .collect();

    let olap_handles: Vec<_> = (0..num_olap)
        .map(|_| {
            let db = db.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            let mode = cfg.olap_mode.clone();
            let scan_tx = scan_tx.clone();
            thread::spawn(move || {
                barrier.wait();
                run_olap_worker(&db, mode, &stop, &scan_tx);
            })
        })
        .collect();
    drop(scan_tx);

    // Releases at the same instant as every worker thread, once loading is
    // done — so the timed phase (and this wall-clock measurement) excludes
    // load time entirely.
    barrier.wait();

    if let Some((dead_ratio_threshold, sweep_interval)) = cfg.idle_compaction {
        db.set_vacuum(Some((dead_ratio_threshold, sweep_interval)));
    }
    if let Some(num_workers) = cfg.scan_pool_workers.filter(|&n| n > 0) {
        db.enable_scan_pool(
            crate::bat_bench::tpcc_schema::Table::OrderLine,
            num_workers,
            Some(num_olap),
        );
    }

    let run_start = Instant::now();
    println!("Loading done. Running timed phase for {duration:?}...");
    thread::sleep(duration);
    stop.store(true, Relaxed);
    db.set_vacuum(None);
    db.disable_scan_pool(crate::bat_bench::tpcc_schema::Table::OrderLine);

    let terminal_stats: Vec<TerminalStats> = terminal_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    for h in olap_handles {
        let _ = h.join();
    }
    let actual_wall = run_start.elapsed();

    let mut scan_results = Vec::new();
    while let Ok(r) = scan_rx.try_recv() {
        scan_results.push(r);
    }

    if std::env::var_os("BATSTORE_VERIFY_Q1").is_some() {
        let [fe, sq, cp] = crate::bat_bench::tpch_queries::verify_q1_read_paths(&db);
        println!("VERIFY_Q1 for_each_ref={fe:?} sequential_next={sq:?} collect_parallel={cp:?}");
    }

    mem_sampler.stop();
    #[cfg(feature = "gc-stats")]
    write_gc_stats(&db, &cfg.output_dir, "gc_stats.csv");

    #[cfg(feature = "tpcc-tree-stats")]
    if tree_stats.is_some() {
        // The experiment command deliberately disables idle compaction; all
        // terminal and OLAP workers have joined at this point, so raw page
        // inspection is quiescent.
        let final_audit = crate::bat_bench::tpcc_tree_stats::write_checkpoint(
            &db,
            &cfg.output_dir,
            "after_run",
        )
        .expect("failed to write after-run TPC-C tree audit");
        let stats_after_run = crate::bat_bench::tpcc_tree_stats::snapshot_smos(&db);
        let after_load = stats_after_load.as_ref().unwrap();
        let after_warmup = stats_after_warmup.as_ref().unwrap();
        crate::bat_bench::tpcc_tree_stats::write_smo_phase(
            &cfg.output_dir,
            "load",
            after_load,
            0,
        )
        .expect("failed to write load SMO counts");
        crate::bat_bench::tpcc_tree_stats::write_smo_phase(
            &cfg.output_dir,
            "warmup",
            &after_warmup.phase_since(after_load),
            warmup_committed,
        )
        .expect("failed to write warm-up SMO counts");
        let measured_smos = stats_after_run.phase_since(after_warmup);
        let measured_committed = committed_transaction_units(&terminal_stats);
        crate::bat_bench::tpcc_tree_stats::write_smo_phase(
            &cfg.output_dir,
            "measured",
            &measured_smos,
            measured_committed,
        )
        .expect("failed to write measured SMO counts");
        crate::bat_bench::tpcc_tree_stats::write_human_summary(
            &cfg.output_dir,
            &final_audit,
            &measured_smos,
            measured_committed,
        )
        .expect("failed to write human-readable tree-stats summary");
        let (git_commit, git_dirty) = crate::bat_bench::tpcc_tree_stats::git_state();
        crate::bat_bench::tpcc_tree_stats::write_metadata(
            &cfg.output_dir,
            &crate::bat_bench::tpcc_tree_stats::RunMetadata {
                schema_version: 1,
                command: "tpcc_tree_stats",
                git_commit,
                git_dirty,
                build_profile: if cfg!(debug_assertions) {
                    "debug"
                } else {
                    "release"
                },
                allocator: if cfg!(feature = "mimalloc") {
                    "mimalloc"
                } else {
                    "jemalloc"
                },
                logical_cpus: num_cpus::get(),
                completed_unix_seconds: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                warehouses: cfg.tpcc.num_warehouses,
                terminals: cfg.num_terminals,
                warmup_seconds: tree_stats.as_ref().unwrap().warmup.as_secs(),
                measured_seconds: cfg.duration.as_secs(),
                gc: cfg.gc,
                update_in_place: cfg.update_in_place,
                idle_compaction: cfg.idle_compaction.is_some(),
                root_star: cfg.root_star_index.to_string(),
                big_tree_size: format!("{:?}", cfg.big_tree_size),
                num_items: cfg.tpcc.num_items,
                customers_per_district: cfg.tpcc.customers_per_district,
                initial_orders_per_district: cfg.tpcc.initial_orders_per_district,
                final_version: db.current_version(),
            },
        )
        .expect("failed to write tree-stats metadata");
    }

    if crate::bat_test::SCAN_TRACE {
        crate::bat_test::dump_scan_trace();
    }

    if crate::bat_test::SCAN_TRACE || crate::bat_test::RESTART_TRACE {
        use crate::bat_bench::tpcc_schema::{BigTreeOp, Table, TpccKey, TpccRow};

        struct AddrOp;
        impl BigTreeOp for AddrOp {
            type Output = usize;
            fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(
                self,
                tree: &crate::bat_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>,
            ) -> usize {
                tree as *const _ as usize
            }
        }
        let mut table_names = db.db.table_names_by_addr();
        table_names.push((
            db.dispatch_big(Table::Warehouse, AddrOp),
            "warehouse".to_string(),
        ));
        table_names.push((
            db.dispatch_big(Table::District, AddrOp),
            "district".to_string(),
        ));

        if crate::bat_test::SCAN_TRACE {
            crate::bat_test::dump_scan_trace_by_table(
                cfg.output_dir
                    .join("tpcc_scan_trace_by_table.csv")
                    .to_str()
                    .unwrap(),
                &table_names,
            );
        }

        if crate::bat_test::RESTART_TRACE {
            crate::bat_test::dump_restart_trace(
                cfg.output_dir
                    .join("tpcc_restart_trace.csv")
                    .to_str()
                    .unwrap(),
            );
            crate::bat_test::dump_attempt_histogram(
                cfg.output_dir
                    .join("tpcc_attempt_histogram.csv")
                    .to_str()
                    .unwrap(),
            );
            crate::bat_test::dump_root_restarts_by_table(
                cfg.output_dir
                    .join("tpcc_root_restarts_by_table.csv")
                    .to_str()
                    .unwrap(),
                &table_names,
            );
        }
    }

    write_results(
        &terminal_stats,
        &scan_results,
        duration,
        actual_wall,
        baseline_tpm_c,
        &cfg.output_dir,
    )
}

#[cfg(feature = "gc-stats")]
fn write_gc_stats(db: &TpccDatabase, out_dir: &Path, filename: &str) {
    fn add_stats(totals: &mut Vec<[u64; 9]>, stats: Vec<GcStats>) {
        if totals.len() < stats.len() {
            totals.resize(stats.len(), [0; 9]);
        }
        for (total, stat) in totals.iter_mut().zip(stats) {
            total[0] += stat.local_reuse;
            total[1] += stat.steal;
            total[2] += stat.fresh_alloc;
            total[3] += stat.request_count;
            total[4] += stat.latency_ns;
            total[5] = total[5].max(stat.latency_max_ns);
            total[6] += stat.scan_count;
            total[7] += stat.lists_checked;
            total[8] = total[8].max(stat.lists_checked_max);
        }
    }

    struct ReadStats;
    impl BigTreeOp for ReadStats {
        type Output = Vec<GcStats>;

        fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(
            self,
            tree: &crate::bat_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>,
        ) -> Self::Output {
            tree.tracker().gc_stats_per_shard()
        }
    }

    let mut totals = Vec::new();
    for table in Table::ALL {
        match table {
            Table::Warehouse | Table::District => {
                add_stats(&mut totals, db.dispatch_big(table, ReadStats));
            }
            _ => add_stats(&mut totals, db.tree_for(table).tracker().gc_stats_per_shard()),
        }
    }

    let path = out_dir.join(filename);
    let _ = fs::remove_file(&path);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("gc_stats: failed to open {}: {e}", path.display()));
    file.write_all(b"shard,local_reuse,steal,fresh_alloc,schema_version,request_count,latency_ns,latency_max_ns,scan_count,lists_checked,lists_checked_max\n").unwrap();
    for (shard, stat) in totals.into_iter().enumerate() {
        file.write_all(
            format!(
                "{shard},{},{},{},2,{},{},{},{},{},{}\n",
                stat[0], stat[1], stat[2], stat[3], stat[4], stat[5], stat[6], stat[7],
                stat[8]
            )
            .as_bytes(),
        )
        .unwrap();
    }
}

fn num_olap_mode_summary(mode: &OlapMode) -> &'static str {
    match mode {
        OlapMode::OpenAndSleep { .. } => "open_and_sleep",
        OlapMode::ScanDelaySweep { .. } => "scan_delay_sweep",
        OlapMode::RepeatedFreshFullScan => "repeated_fresh_full_scan",
        OlapMode::RepeatedHistoricFullScan => "repeated_historic_full_scan",
        OlapMode::ChBenchmark { .. } => "ch_benchmark",
        OlapMode::ChQ1 { .. } => "ch_q1",
        OlapMode::ChQ6 { .. } => "ch_q6",
        OlapMode::BenchbaseQ1 { .. } => "benchbase_q1",
        OlapMode::BenchbaseQ6 { .. } => "benchbase_q6",
    }
}

#[cfg(feature = "tpcc-tree-stats")]
fn committed_transaction_units(stats: &[TerminalStats]) -> u64 {
    stats
        .iter()
        .map(|s| {
            s.totals[NO]
                + s.totals[PAY]
                + s.totals[OS]
                + s.totals[SL]
                + s.totals[DELIV_DISTRICTS]
        })
        .sum()
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
    let mut ts_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&oltp_ts_path)
        .unwrap();
    ts_file
        .write_all(b"elapsed_sec,new_order_committed\n")
        .unwrap();
    for (sec, count) in per_sec.iter().enumerate() {
        ts_file
            .write_all(format!("{sec},{count}\n").as_bytes())
            .unwrap();
    }

    let scan_path = out_dir.join("tpcc_scan.csv");
    let _ = fs::remove_file(&scan_path);
    let mut scan_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&scan_path)
        .unwrap();
    scan_file.write_all(b"mode,elapsed_secs,delay_secs,snapshot,scanned_tuples,latency_ns,tuples_per_sec,summary,staleness_versions\n").unwrap();
    for r in scan_results {
        scan_file
            .write_all(
                format!(
                    "{},{:.3},{},{},{},{},{:.2},{},{}\n",
                    r.mode,
                    r.elapsed_secs,
                    r.delay_secs,
                    r.snapshot,
                    r.scanned_tuples,
                    r.latency_ns,
                    r.tuples_per_sec(),
                    r.summary.map(|s| format!("{s:.2}")).unwrap_or_default(),
                    r.staleness_versions
                        .map(|s| s.to_string())
                        .unwrap_or_default(),
                )
                .as_bytes(),
            )
            .unwrap();
    }

    let new_order_total = totals[NO];
    let tpm_c = new_order_total as f64 / (actual_wall.as_secs_f64() / 60.0);

    println!("\n===== Results (timed phase: {actual_wall:?}) =====");
    for i in 0..NUM_COUNTERS {
        println!("{:<32} {}", COUNTER_NAMES[i], totals[i]);
    }
    println!("{:<32} {:.2}", "tpmC (New-Order/min)", tpm_c);
    if let Some(baseline) = baseline_tpm_c {
        let interference_pct = if baseline > 0.0 {
            (baseline - tpm_c) / baseline * 100.0
        } else {
            0.0
        };
        println!("{:<32} {:.2}", "tpmC (HTAP baseline, no OLAP)", baseline);
        println!(
            "{:<32} {:.1}%",
            "OLTP interference from OLAP", interference_pct
        );
    }
    println!(
        "{:<32} {}",
        "OLAP scans/holds completed",
        scan_results.len()
    );
    if !scan_results.is_empty() {
        let avg_tps = scan_results.iter().map(|r| r.tuples_per_sec()).sum::<f64>()
            / scan_results.len() as f64;
        println!("{:<32} {:.1}", "OLAP avg tuples/sec", avg_tps);
    }
    let staleness: Vec<u64> = scan_results
        .iter()
        .filter_map(|r| r.staleness_versions)
        .collect();
    if !staleness.is_empty() {
        let avg = staleness.iter().sum::<u64>() as f64 / staleness.len() as f64;
        let max = staleness.iter().max().unwrap();
        println!(
            "{:<32} {:.1} (max {max})",
            "HTAP staleness (versions, avg)", avg
        );
    }
    println!(
        "Wrote {} and {}",
        oltp_ts_path.display(),
        scan_path.display()
    );

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

#[cfg(feature = "tpcc-tree-stats")]
pub fn main_tpcc_tree_stats(parms: Vec<String>) {
    fn value<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
        args.iter()
            .position(|arg| arg == name)
            .and_then(|index| args.get(index + 1))
            .map(String::as_str)
    }
    fn parsed<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> T {
        value(args, name)
            .and_then(|raw| raw.parse().ok())
            .unwrap_or(default)
    }
    fn enabled(args: &[String], name: &str, default: bool) -> bool {
        value(args, name)
            .map(|raw| matches!(raw, "1" | "true" | "on" | "yes"))
            .unwrap_or(default)
    }

    if parms.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "TPC-C current-node filling, weak-condition, and exact SMO experiment\n\
             Usage: batstore tpcc_tree_stats [options]\n\
             \n\
             --warehouses N       default 16\n\
             --terminals N        default 16\n\
             --warmup SECONDS     default 30\n\
             --duration SECONDS   default 180\n\
             --gc on|off          default on\n\
             --affinity on|off    default off\n\
             --big-tree-size SIZE 1kib|2kib|4kib|8kib|16kib|32kib|64kib|512kib\n\
             --items N            default 100000\n\
             --customers N        customers per district, default 3000\n\
             --orders N           initial orders per district, default 3000\n\
             --suppliers N        default 10000\n\
             --output-dir PATH    default experiments/tpcc_tree_stats_<unix-seconds>\n\
             --quick              1 warehouse, 1 terminal, small cardinalities, 1s+2s\n\
             \n\
             Build with: cargo build --release --features tpcc-tree-stats"
        );
        return;
    }

    let quick = parms.iter().any(|arg| arg == "--quick");
    let warehouses = parsed(&parms, "--warehouses", if quick { 1 } else { 16 });
    let terminals = parsed(&parms, "--terminals", if quick { 1 } else { 16 });
    let warmup_seconds = parsed(&parms, "--warmup", if quick { 1 } else { 30 });
    let duration_seconds = parsed(&parms, "--duration", if quick { 2 } else { 180 });
    let num_items = parsed(&parms, "--items", if quick { 1_000 } else { 100_000 });
    let customers = parsed(&parms, "--customers", if quick { 100 } else { 3_000 });
    let orders = parsed(&parms, "--orders", if quick { 100 } else { 3_000 });
    let suppliers = parsed(&parms, "--suppliers", if quick { 100 } else { 10_000 });
    let big_tree_size = match value(&parms, "--big-tree-size").unwrap_or("32kib") {
        "1kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB1,
        "2kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB2,
        "4kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB4,
        "8kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB8,
        "16kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB16,
        "64kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB64,
        "512kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB512,
        "32kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB32,
        other => panic!("unknown --big-tree-size '{other}'"),
    };
    let default_output = format!(
        "experiments/tpcc_tree_stats_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    let output_dir = PathBuf::from(value(&parms, "--output-dir").unwrap_or(&default_output));

    println!("Tree-stats output directory: {}", output_dir.display());
    run_tpcc_with_tree_stats(
        DriverConfig {
            tpcc: TpccConfig {
                num_warehouses: warehouses,
                districts_per_warehouse: 10,
                customers_per_district: customers,
                num_items,
                initial_orders_per_district: orders,
                initial_new_orders: (orders * 3 / 10).max(1),
                num_suppliers: suppliers,
            },
            num_terminals: terminals,
            duration: Duration::from_secs(duration_seconds),
            affinity: enabled(&parms, "--affinity", false),
            gc: enabled(&parms, "--gc", true),
            update_in_place: false,
            root_star_index: RootIndexType::FrugalList,
            big_tree_size,
            olap_mode: OlapMode::RepeatedFreshFullScan,
            num_olap_threads: 0,
            wal: None,
            wal_lockfree_batch_size: None,
            htap_baseline: None,
            // The raw-page audit must be quiescent. Existing vacuum threads
            // are fire-and-forget, so this dedicated command intentionally
            // measures the write-path experiment with vacuum disabled.
            idle_compaction: None,
            scan_pool_workers: None,
            output_dir,
        },
        TreeStatsRunConfig {
            warmup: Duration::from_secs(warmup_seconds),
        },
    );
}

pub fn main_tpcc(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms
            .get(idx)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
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
    let olap_mode_str = parms
        .get(9)
        .map(|s| s.as_str())
        .unwrap_or("scan_sweep")
        .to_string();
    let num_olap_threads: usize = arg(&parms, 10, 1);
    let olap_param: f64 = arg(&parms, 11, 10.0);
    let num_items: u32 = arg(&parms, 12, 100_000);
    let customers_per_district: u32 = arg(&parms, 13, 3_000);
    let initial_orders_per_district: u32 = arg(&parms, 14, 3_000);
    let wal_enabled: bool = arg(&parms, 15, false);
    let wal_path: String = parms
        .get(16)
        .cloned()
        .unwrap_or_else(|| "tpcc_wal.log".to_string());
    let wal_flush_ms: u64 = arg(&parms, 17, 5);
    let ch_region: String = parms
        .get(18)
        .cloned()
        .unwrap_or_else(|| "EUROPE".to_string());
    let num_suppliers: u32 = arg(&parms, 19, 10_000);
    let htap_baseline_secs: u64 = arg(&parms, 20, 0);
    // Table::Warehouse/Table::District's leaf capacity — see
    // tpcc_schema::BigTreeSize's doc for the measured trade-off each named
    // size sits at (root-contention reduction vs. OLAP scan throughput).
    let big_tree_size = match parms.get(21).map(|s| s.as_str()).unwrap_or("32kib") {
        "1kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB1,
        "2kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB2,
        "4kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB4,
        "8kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB8,
        "16kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB16,
        "64kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB64,
        "512kib" => crate::bat_bench::tpcc_schema::BigTreeSize::KiB512,
        _ => crate::bat_bench::tpcc_schema::BigTreeSize::KiB32,
    };
    const DISTRICTS_PER_WAREHOUSE: u32 = 10;
    const AVG_ORDER_LINES_PER_ORDER: u32 = 10;
    let estimated_order_line_rows = num_warehouses as u64
        * DISTRICTS_PER_WAREHOUSE as u64
        * initial_orders_per_district as u64
        * AVG_ORDER_LINES_PER_ORDER as u64;
    let scan_pool_workers: Option<usize> = match parms.get(22).map(|s| s.as_str()) {
        None if num_olap_threads > 0
            && estimated_order_line_rows
                >= crate::bat_bench::parallel_scan::MIN_ROWS_FOR_SCAN_POOL =>
        {
            let fair_share_floor =
                crate::bat_tree::scan_pool::DEFAULT_QUERY_FANOUT * num_olap_threads;
            Some(
                crate::bat_tree::mvbt::default_max_workers()
                    .max(fair_share_floor)
                    .max(2),
            )
        }
        None => None,
        Some(s) => match s.parse::<usize>() {
            Ok(0) | Err(_) => None,
            Ok(n) => Some(n.max(2)),
        },
    };
    let idle_compaction_dead_ratio: f64 =
        arg(&parms, 23, if gc { DEFAULT_VACUUM_DEAD_RATIO } else { 0.0 });
    let idle_compaction_sweep_secs: f64 =
        arg(&parms, 24, DEFAULT_VACUUM_SWEEP_INTERVAL.as_secs_f64());
    let idle_compaction = (idle_compaction_dead_ratio > 0.0).then(|| {
        (
            idle_compaction_dead_ratio,
            Duration::from_secs_f64(idle_compaction_sweep_secs),
        )
    });

    let (q1_cutoff, q6_date_lo, q6_date_hi) = htap_query_date_bounds(now_millis());
    let (olap_mode, num_olap_threads) = match olap_mode_str.as_str() {
        "none" => (OlapMode::RepeatedFreshFullScan, 0),
        "sleep" => (
            OlapMode::OpenAndSleep {
                hold: Duration::from_secs_f64(olap_param),
            },
            num_olap_threads,
        ),
        "fresh" => (OlapMode::RepeatedFreshFullScan, num_olap_threads),
        "historic" => (OlapMode::RepeatedHistoricFullScan, num_olap_threads),
        "ch" => (
            OlapMode::ChBenchmark {
                region_name: ch_region,
                date_lo: q6_date_lo,
                date_hi: q6_date_hi,
            },
            num_olap_threads,
        ),
        "ch_q1_variant" => (
            OlapMode::ChQ1 {
                delivered_before: q1_cutoff,
                num_warehouses,
            },
            num_olap_threads,
        ),
        "ch_q6_variant" => (
            OlapMode::ChQ6 {
                date_lo: q6_date_lo,
                date_hi: q6_date_hi,
                max_qty: 24,
                num_warehouses,
            },
            num_olap_threads,
        ),
        "ch_q1" => (OlapMode::BenchbaseQ1 { num_warehouses }, num_olap_threads),
        "ch_q6" => (OlapMode::BenchbaseQ6 { num_warehouses }, num_olap_threads),
        _ => (
            OlapMode::ScanDelaySweep {
                delays: (0..=(olap_param.max(0.0) as u64))
                    .map(Duration::from_secs)
                    .collect(),
            },
            num_olap_threads,
        ),
    };

    let tpcc_cfg = TpccConfig {
        num_warehouses,
        districts_per_warehouse: DISTRICTS_PER_WAREHOUSE as u8,
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
        wal: wal_enabled.then(|| {
            (
                std::path::PathBuf::from(wal_path),
                Duration::from_millis(wal_flush_ms),
            )
        }),
        wal_lockfree_batch_size: None,
        htap_baseline: (htap_baseline_secs > 0).then(|| Duration::from_secs(htap_baseline_secs)),
        idle_compaction,
        scan_pool_workers,
        output_dir: PathBuf::from("."),
    });
}

fn standard_driver_config(
    num_warehouses: u32,
    duration: Duration,
    olap_mode: OlapMode,
    num_olap_threads: usize,
    htap_baseline: Option<Duration>,
) -> DriverConfig {
    DriverConfig {
        tpcc: TpccConfig {
            num_warehouses,
            ..TpccConfig::default()
        },
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
        idle_compaction: Some((DEFAULT_VACUUM_DEAD_RATIO, DEFAULT_VACUUM_SWEEP_INTERVAL)),
        scan_pool_workers: None,
        output_dir: PathBuf::from("."),
    }
}

pub fn main_tpch(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms
            .get(idx)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }

    let num_warehouses: u32 = arg(&parms, 2, 4);
    let duration_secs: u64 = arg(&parms, 3, 60);
    let num_olap_threads: usize = arg(&parms, 4, 1);
    let region_name: String = parms
        .get(5)
        .cloned()
        .unwrap_or_else(|| "EUROPE".to_string());

    run_tpcc(standard_driver_config(
        num_warehouses,
        Duration::from_secs(duration_secs),
        OlapMode::ChBenchmark {
            region_name,
            date_lo: i64::MIN,
            date_hi: i64::MAX,
        },
        num_olap_threads,
        None,
    ));
}

pub fn main_htap(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms
            .get(idx)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }

    let num_warehouses: u32 = arg(&parms, 2, 4);
    let duration_secs: u64 = arg(&parms, 3, 60);
    let num_olap_threads: usize = arg(&parms, 4, 1);
    let baseline_secs: u64 = arg(&parms, 5, 15);
    let region_name: String = parms
        .get(6)
        .cloned()
        .unwrap_or_else(|| "EUROPE".to_string());

    run_tpcc(standard_driver_config(
        num_warehouses,
        Duration::from_secs(duration_secs),
        OlapMode::ChBenchmark {
            region_name,
            date_lo: i64::MIN,
            date_hi: i64::MAX,
        },
        num_olap_threads,
        Some(Duration::from_secs(baseline_secs)),
    ));
}
