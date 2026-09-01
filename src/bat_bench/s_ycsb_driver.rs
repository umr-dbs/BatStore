//! "S-YCSB" streaming-workload driver: loads a cold historical corpus,
//! then runs two concurrent thread pools for a fixed wall-clock duration —
//! write threads (near-sorted arrivals + recency-biased hot-tail updates,
//! `s_ycsb_random`/`s_ycsb_txn`) and OLAP threads (long range scans
//! straddling the cold/hot boundary, `ycsb_txn::scan_with_mode`) — modeling
//! a streaming-ingest-plus-dashboard workload where slow analytical queries
//! run continuously over the same narrow key window that ingestion keeps
//! revising. Mirrors `ycsb_driver`'s structure (population, barrier-
//! synchronized workers, timed phase, CSV + summary output).

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use triomphe::Arc;

use crate::bat_bench::mem_stats::{DEFAULT_SAMPLE_INTERVAL, MemSampler};
use crate::bat_bench::s_ycsb_random::{
    HotTailSampler, SYcsbMix, SYcsbWriteOp, mint_arrival_key, olap_scan_bounds,
    pick_write_op,
};
use crate::bat_bench::s_ycsb_txn;
use crate::bat_bench::ycsb_load::populate;
use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbScanPool, YcsbTree};
use crate::bat_bench::ycsb_txn::{self, YcsbExecutionMode};
use crate::bat_root::index_root::RootIndexType;

pub struct DriverConfig {
    /// `record_count` is the cold historical corpus loaded before the timed
    /// phase starts (the "already settled behind the watermark" data);
    /// `field_count`/`field_length` shape every row, same as YCSB.
    pub ycsb: YcsbConfig,
    pub num_write_threads: usize,
    pub num_olap_threads: usize,
    pub duration: Duration,
    pub mix: SYcsbMix,
    /// Width, in keys, of the recency-biased hot-update window (the tail
    /// that repeatedly gets revised).
    pub hot_window: u64,
    /// Zipf skew for hot-tail updates (higher = more concentrated on the
    /// very newest keys), same meaning as YCSB's `Latest` theta.
    pub hot_theta: f64,
    /// Bound on how far behind the arrival ticket a late event's key can
    /// land (0 = strictly monotonic arrival, no lateness at all).
    pub max_lateness: u64,
    /// How far behind the current tail an OLAP scan's newest edge sits (0 =
    /// the scan reaches all the way to the tail).
    pub olap_lag: u64,
    /// Width, in keys, of each OLAP scan.
    pub olap_span: u64,
    pub write_all_fields: bool,
    pub read_payload: bool,
    pub execution_mode: YcsbExecutionMode,
    pub gc: bool,
    pub update_in_place: bool,
    pub root_star_index: RootIndexType,
    /// See `DriverConfig::wal` in `tpcc_driver` — same semantics here.
    pub wal: Option<(PathBuf, Duration)>,
    pub wal_lockfree_batch_size: Option<usize>,
    pub output_dir: PathBuf,
    /// Assigns the usertable a shared scan-worker pool (see
    /// `bat_tree::scan_pool::ScanWorkerPool`'s doc) for `olap_worker_thread`
    /// to fan its scans out across via `ycsb_txn::scan_parallel` instead of
    /// walking each one on its own thread — same feature/semantics as
    /// `ycsb_driver::DriverConfig::scan_pool_workers`, except every OLAP
    /// thread here is *always* scanning (unlike YCSB's mixed workload), so
    /// `expected_concurrent_queries` is simply `num_olap_threads`, not an
    /// estimate weighted by an op-mix fraction. `None`/`Some(0)` disables it
    /// entirely: every OLAP thread scans sequentially, unchanged from every
    /// prior behavior.
    pub scan_pool_workers: Option<usize>,
    /// See `ycsb_driver::DriverConfig::idle_compaction`'s doc — same
    /// mechanism/semantics for this driver's one bare tree.
    pub idle_compaction: Option<(f64, Duration)>,
}

pub struct SYcsbRunSummary {
    pub write_throughput_ops_sec: f64,
    pub totals: [u64; NUM_WRITE_COUNTERS],
    pub olap_scans_completed: u64,
}

const NEW_ARRIVAL: usize = 0;
const LATE_UPSERT: usize = 1;
const HOT_UPDATE: usize = 2;
const NUM_WRITE_COUNTERS: usize = 3;
const WRITE_COUNTER_NAMES: [&str; NUM_WRITE_COUNTERS] =
    ["new_arrival", "late_upsert", "hot_update"];

struct WriteWorkerStats {
    ops_per_sec: Vec<u64>,
    totals: [u64; NUM_WRITE_COUNTERS],
}

#[allow(clippy::too_many_arguments)]
fn write_worker_thread(
    tree: Arc<YcsbTree>,
    cfg: YcsbConfig,
    mix: SYcsbMix,
    hot_sampler: Arc<HotTailSampler>,
    write_all_fields: bool,
    execution_mode: YcsbExecutionMode,
    current_max_key: Arc<AtomicU64>,
    max_lateness: u64,
    duration: Duration,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) -> WriteWorkerStats {
    barrier.wait();

    let mut ops_per_sec = vec![0u64; duration.as_secs() as usize + 2];
    let mut totals = [0u64; NUM_WRITE_COUNTERS];
    let start = Instant::now();

    while !stop.load(Relaxed) {
        match pick_write_op(&mix) {
            SYcsbWriteOp::Arrival => {
                let key = mint_arrival_key(&current_max_key, max_lateness);
                let was_new =
                    s_ycsb_txn::arrival_upsert(&tree, &cfg, key, write_all_fields, execution_mode);
                totals[if was_new { NEW_ARRIVAL } else { LATE_UPSERT }] += 1;
            }
            SYcsbWriteOp::HotUpdate => {
                let max_key_now = current_max_key.load(Relaxed);
                let key = hot_sampler.sample(max_key_now);
                ycsb_txn::update_with_execution_mode(
                    &tree,
                    &cfg,
                    key,
                    write_all_fields,
                    execution_mode,
                );
                totals[HOT_UPDATE] += 1;
            }
        }

        let idx = (start.elapsed().as_secs() as usize).min(ops_per_sec.len() - 1);
        ops_per_sec[idx] += 1;
    }

    WriteWorkerStats { ops_per_sec, totals }
}

struct OlapWorkerStats {
    scanned_tuples: u64,
    scans_completed: u64,
    scan_latencies_ns: Vec<u64>,
    staleness_versions: Vec<u64>,
}

fn olap_worker_thread(
    tree: Arc<YcsbTree>,
    scan_pool: Option<Arc<YcsbScanPool>>,
    current_max_key: Arc<AtomicU64>,
    olap_lag: u64,
    olap_span: u64,
    read_payload: bool,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) -> OlapWorkerStats {
    barrier.wait();

    let mut scanned_tuples = 0u64;
    let mut scans_completed = 0u64;
    let mut scan_latencies_ns = Vec::new();
    let mut staleness_versions = Vec::new();

    while !stop.load(Relaxed) {
        let (lo, len) = olap_scan_bounds(current_max_key.load(Relaxed), olap_lag, olap_span);
        let ts_before = tree.current_version();
        let scan_start = Instant::now();
        scanned_tuples +=
            ycsb_txn::scan_parallel(scan_pool.as_deref(), &tree, lo, len, read_payload) as u64;
        scan_latencies_ns.push(scan_start.elapsed().as_nanos() as u64);
        staleness_versions.push(tree.current_version().saturating_sub(ts_before));
        scans_completed += 1;
    }

    OlapWorkerStats {
        scanned_tuples,
        scans_completed,
        scan_latencies_ns,
        staleness_versions,
    }
}

pub fn run_s_ycsb(cfg: DriverConfig) -> SYcsbRunSummary {
    assert!(
        cfg.ycsb.record_count >= 1,
        "s_ycsb: record_count must be >= 1"
    );
    assert!(cfg.hot_window >= 1, "s_ycsb: hot_window must be >= 1");
    assert!(cfg.olap_span >= 1, "s_ycsb: olap_span must be >= 1");

    let max_threads = crate::bat_tree::mvbt::default_max_workers().max(1);
    let mut num_write_threads = cfg.num_write_threads.max(1);
    let mut num_olap_threads = cfg.num_olap_threads.max(1);
    // One more permanent WorkerId if idle compaction is enabled — see
    // `ycsb_driver::run_ycsb`'s identical `idle_compaction_cost`: the
    // vacuum thread `spawn_vacuum_thread` starts below calls
    // `compact_idle_pass`, which acquires its own `WorkerId` via
    // `self.worker_id()` just like any writer/OLAP thread, so it has to be
    // budgeted here too or its first sweep panics the registry once the
    // loader + writers + OLAP threads have already filled every other slot.
    let idle_compaction_cost = if cfg.gc && cfg.idle_compaction.is_some() { 1 } else { 0 };
    // +1: the main thread also acquires a WorkerId, for the sequential
    // population phase before any worker thread is spawned (see ycsb_driver).
    let fixed_cost = 1 + idle_compaction_cost;
    if fixed_cost + num_write_threads + num_olap_threads > max_threads {
        println!(
            "!! {fixed_cost} loader/idle-compaction + {num_write_threads} writers + {num_olap_threads} OLAP threads > max_workers ({max_threads} = num_cpus); clamping writers."
        );
        num_write_threads = max_threads
            .saturating_sub(fixed_cost + num_olap_threads)
            .max(1);
        if fixed_cost + num_write_threads + num_olap_threads > max_threads {
            num_olap_threads = max_threads.saturating_sub(fixed_cost + 1).max(1);
        }
    }

    fs::create_dir_all(&cfg.output_dir).unwrap_or_else(|e| {
        panic!(
            "s_ycsb: failed to create output_dir {}: {e}",
            cfg.output_dir.display()
        )
    });
    let mem_sampler = MemSampler::start(
        cfg.output_dir.join("mem_stats.csv"),
        DEFAULT_SAMPLE_INTERVAL,
    );

    let total_workers = fixed_cost + num_write_threads + num_olap_threads;
    let tree = match &cfg.wal {
        Some((wal_path, flush_interval)) => {
            let _ = fs::remove_file(wal_path);
            let base =
                YcsbTree::make_standard_with_max_workers(cfg.root_star_index, total_workers);
            Arc::new(
                match cfg.wal_lockfree_batch_size {
                    Some(batch_size) => {
                        base.with_wal_lockfree(wal_path, *flush_interval, batch_size)
                    }
                    None => base.with_wal(wal_path, *flush_interval),
                }
                .expect("failed to configure WAL at tree construction"),
            )
        }
        None => Arc::new(YcsbTree::make_standard_with_max_workers(
            cfg.root_star_index,
            total_workers,
        )),
    };
    if cfg.gc {
        tree.enable_gc(cfg.update_in_place);
    }
    let vacuum_stop = std::sync::Arc::new(AtomicBool::new(false));
    if let Some((dead_ratio_threshold, sweep_interval)) = cfg.idle_compaction.filter(|_| cfg.gc) {
        crate::bat_tree::idle_compaction::spawn_vacuum_thread(
            tree.clone(),
            dead_ratio_threshold,
            sweep_interval,
            vacuum_stop.clone(),
        );
    }

    // See `DriverConfig::scan_pool_workers`'s doc: never counted against
    // `max_threads`/`total_workers` above — a pool worker thread never
    // calls `tree.worker_id()` (see `ycsb_driver::run_ycsb`'s identical
    // pool setup for the same reasoning).
    let scan_pool: Option<Arc<YcsbScanPool>> = cfg.scan_pool_workers.filter(|&n| n > 0).map(|n| {
        Arc::new(YcsbScanPool::spawn(tree.clone(), n, Some(num_olap_threads)))
    });

    println!(
        "S-YCSB benchmark\n\
         - cold record_count   = {}\n\
         - field_count/length  = {}/{}\n\
         - write workers       = {num_write_threads}\n\
         - OLAP workers        = {num_olap_threads}\n\
         - duration            = {:?}\n\
         - mix (arrival/hot)   = {:.2}/{:.2}\n\
         - hot_window          = {}\n\
         - hot_theta           = {}\n\
         - max_lateness        = {}\n\
         - olap_lag/span       = {}/{}\n\
         - write_all_fields    = {}\n\
         - read_payload        = {}\n\
         - execution_mode      = {:?}\n\
         - GC                  = {} (update_in_place={})\n\
         - WAL                 = {}\n\
         - root*               = {}\n\
         - scan_pool           = {}",
        cfg.ycsb.record_count,
        cfg.ycsb.field_count,
        cfg.ycsb.field_length,
        cfg.duration,
        cfg.mix.arrival,
        cfg.mix.hot_update,
        cfg.hot_window,
        cfg.hot_theta,
        cfg.max_lateness,
        cfg.olap_lag,
        cfg.olap_span,
        cfg.write_all_fields,
        cfg.read_payload,
        cfg.execution_mode,
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
        match &scan_pool {
            Some(p) => format!("On, {} workers", p.num_workers()),
            None => "Off".to_string(),
        },
    );

    println!("Loading {} cold records...", cfg.ycsb.record_count);
    let load_start = Instant::now();
    populate(&tree, &cfg.ycsb);
    println!(
        "Loaded {} records in {:?}.",
        cfg.ycsb.record_count,
        load_start.elapsed()
    );

    let hot_sampler = Arc::new(HotTailSampler::new(cfg.hot_theta, cfg.hot_window));
    let current_max_key = Arc::new(AtomicU64::new(cfg.ycsb.record_count));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(num_write_threads + num_olap_threads + 1));

    let duration = cfg.duration;
    let mix = cfg.mix;
    let ycsb_cfg = cfg.ycsb;
    let write_all_fields = cfg.write_all_fields;
    let read_payload = cfg.read_payload;
    let execution_mode = cfg.execution_mode;
    let max_lateness = cfg.max_lateness;
    let olap_lag = cfg.olap_lag;
    let olap_span = cfg.olap_span;

    let write_handles: Vec<_> = (0..num_write_threads)
        .map(|_| {
            let tree = tree.clone();
            let cfg = ycsb_cfg;
            let hot_sampler = hot_sampler.clone();
            let current_max_key = current_max_key.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                write_worker_thread(
                    tree,
                    cfg,
                    mix,
                    hot_sampler,
                    write_all_fields,
                    execution_mode,
                    current_max_key,
                    max_lateness,
                    duration,
                    stop,
                    barrier,
                )
            })
        })
        .collect();

    let olap_handles: Vec<_> = (0..num_olap_threads)
        .map(|_| {
            let tree = tree.clone();
            let scan_pool = scan_pool.clone();
            let current_max_key = current_max_key.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                olap_worker_thread(
                    tree,
                    scan_pool,
                    current_max_key,
                    olap_lag,
                    olap_span,
                    read_payload,
                    stop,
                    barrier,
                )
            })
        })
        .collect();

    // Releases at the same instant as every worker, once loading is done —
    // so the timed phase excludes load time entirely (see ycsb_driver).
    barrier.wait();
    let run_start = Instant::now();
    println!("Loading done. Running timed phase for {duration:?}...");
    thread::sleep(duration);
    stop.store(true, Relaxed);
    vacuum_stop.store(true, Relaxed);

    let write_stats: Vec<WriteWorkerStats> =
        write_handles.into_iter().map(|h| h.join().unwrap()).collect();
    let olap_stats: Vec<OlapWorkerStats> =
        olap_handles.into_iter().map(|h| h.join().unwrap()).collect();
    let actual_wall = run_start.elapsed();

    mem_sampler.stop();

    write_results(&write_stats, &olap_stats, duration, actual_wall, &cfg.output_dir)
}

fn write_results(
    write_stats: &[WriteWorkerStats],
    olap_stats: &[OlapWorkerStats],
    requested_duration: Duration,
    actual_wall: Duration,
    out_dir: &Path,
) -> SYcsbRunSummary {
    let series_len = requested_duration.as_secs() as usize + 2;
    let mut per_sec = vec![0u64; series_len];
    let mut totals = [0u64; NUM_WRITE_COUNTERS];
    for s in write_stats {
        for (i, v) in s.ops_per_sec.iter().enumerate() {
            per_sec[i] += v;
        }
        for i in 0..NUM_WRITE_COUNTERS {
            totals[i] += s.totals[i];
        }
    }

    let mut scanned_tuples = 0u64;
    let mut scans_completed = 0u64;
    let mut scan_latencies_ns: Vec<u64> = Vec::new();
    let mut staleness_versions: Vec<u64> = Vec::new();
    for s in olap_stats {
        scanned_tuples += s.scanned_tuples;
        scans_completed += s.scans_completed;
        scan_latencies_ns.extend_from_slice(&s.scan_latencies_ns);
        staleness_versions.extend_from_slice(&s.staleness_versions);
    }

    let ts_path = out_dir.join("s_ycsb_timeseries.csv");
    let _ = fs::remove_file(&ts_path);
    let mut ts_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&ts_path)
        .unwrap();
    ts_file.write_all(b"elapsed_sec,ops_completed\n").unwrap();
    for (sec, count) in per_sec.iter().enumerate() {
        ts_file
            .write_all(format!("{sec},{count}\n").as_bytes())
            .unwrap();
    }

    // Same nearest-rank-percentile summary format as
    // ycsb_driver::write_results's `ycsb_scan_latency_summary.csv` (see that
    // function's comment) - every OLAP scan is sampled here (unlike YCSB-E's
    // throttled sampling), since this workload's scans are deliberately few
    // and slow rather than many and tiny.
    scan_latencies_ns.sort_unstable();
    let scan_latency_path = out_dir.join("s_ycsb_scan_latency_summary.csv");
    let _ = fs::remove_file(&scan_latency_path);
    let mut scan_latency_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&scan_latency_path)
        .unwrap();
    scan_latency_file
        .write_all(b"p50_us,p95_us,p99_us,count,avg_us\n")
        .unwrap();
    let pct = |samples: &[u64], p: f64| -> f64 {
        if samples.is_empty() {
            0.0
        } else {
            let idx = ((p * (samples.len() - 1) as f64).round() as usize).min(samples.len() - 1);
            samples[idx] as f64 / 1000.0
        }
    };
    let avg_us = if scan_latencies_ns.is_empty() {
        0.0
    } else {
        scan_latencies_ns.iter().sum::<u64>() as f64 / scan_latencies_ns.len() as f64 / 1000.0
    };
    scan_latency_file
        .write_all(
            format!(
                "{:.3},{:.3},{:.3},{},{:.3}\n",
                pct(&scan_latencies_ns, 0.50),
                pct(&scan_latencies_ns, 0.95),
                pct(&scan_latencies_ns, 0.99),
                scan_latencies_ns.len(),
                avg_us,
            )
            .as_bytes(),
        )
        .unwrap();

    // Version-chain staleness a scan observed by the time it finished
    // (`ScanResult::staleness_versions` in olap_scan.rs is the same
    // concept) - a direct signal of how far behind GC/coldpages let the
    // OLAP-visible snapshot fall while writers kept revising the hot tail.
    let staleness_path = out_dir.join("s_ycsb_staleness_summary.csv");
    let _ = fs::remove_file(&staleness_path);
    let mut staleness_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&staleness_path)
        .unwrap();
    staleness_file
        .write_all(b"p50,p95,p99,count,avg\n")
        .unwrap();
    staleness_versions.sort_unstable();
    let avg_staleness = if staleness_versions.is_empty() {
        0.0
    } else {
        staleness_versions.iter().sum::<u64>() as f64 / staleness_versions.len() as f64
    };
    staleness_file
        .write_all(
            format!(
                "{:.3},{:.3},{:.3},{},{:.3}\n",
                pct(&staleness_versions, 0.50),
                pct(&staleness_versions, 0.95),
                pct(&staleness_versions, 0.99),
                staleness_versions.len(),
                avg_staleness,
            )
            .as_bytes(),
        )
        .unwrap();

    let total_write_ops: u64 = totals.iter().sum();
    let write_throughput = total_write_ops as f64 / actual_wall.as_secs_f64();

    println!("\n===== Results (timed phase: {actual_wall:?}) =====");
    for i in 0..NUM_WRITE_COUNTERS {
        println!("{:<20} {}", WRITE_COUNTER_NAMES[i], totals[i]);
    }
    println!("{:<20} {}", "total_write_ops", total_write_ops);
    println!("{:<20} {:.2}", "write throughput (ops/sec)", write_throughput);
    println!("{:<20} {}", "olap_scans", scans_completed);
    println!("{:<20} {}", "olap_scanned_tuples", scanned_tuples);
    println!(
        "Wrote {}, {} and {}",
        ts_path.display(),
        scan_latency_path.display(),
        staleness_path.display()
    );

    SYcsbRunSummary {
        write_throughput_ops_sec: write_throughput,
        totals,
        olap_scans_completed: scans_completed,
    }
}

pub fn main_s_ycsb(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms
            .get(idx)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }

    let record_count: u64 = arg(&parms, 2, 1_000_000);
    let num_write_threads: usize = arg(&parms, 3, num_cpus::get().max(2) - 1);
    let num_olap_threads: usize = arg(&parms, 4, 1);
    let duration_secs: u64 = arg(&parms, 5, 30);
    let hot_window: u64 = arg(&parms, 6, 10_000);
    let hot_theta: f64 = arg(&parms, 7, 0.99);
    let arrival_ratio: f64 = arg(&parms, 8, 0.2);
    let max_lateness: u64 = arg(&parms, 9, 50);
    let olap_lag: u64 = arg(&parms, 10, 0);
    let olap_span: u64 = arg(&parms, 11, hot_window.saturating_mul(3).max(1));
    let field_count: usize = arg(&parms, 12, 10);
    let field_length: usize = arg(&parms, 13, 100);
    let write_all_fields: bool = arg(&parms, 14, false);
    let read_payload: bool = arg(&parms, 15, true);
    let root_star_index = match parms.get(16).map(|s| s.as_str()).unwrap_or("fg") {
        "sk" => RootIndexType::SkipList,
        "ll" => RootIndexType::LinkedList,
        "bt" => RootIndexType::BTree,
        _ => RootIndexType::FrugalList,
    };
    let gc: bool = arg(&parms, 17, true);
    let update_in_place: bool = if gc { arg(&parms, 18, false) } else { false };
    let wal_enabled: bool = arg(&parms, 19, false);
    let wal_path: String = parms
        .get(20)
        .cloned()
        .unwrap_or_else(|| "s_ycsb_wal.log".to_string());
    let wal_flush_ms: u64 = arg(&parms, 21, 5);
    let execution_mode = match parms.get(22).map(String::as_str).unwrap_or("atomic") {
        "transaction" | "tx" => YcsbExecutionMode::Transaction,
        "atomic" | "auto" | "autocommit" => YcsbExecutionMode::Atomic,
        other => {
            panic!("s_ycsb: invalid execution mode '{other}' (expected atomic or transaction)")
        }
    };
    // Same "0.0 explicitly opts out, otherwise defaults on whenever GC is
    // on" convention as `tpcc_driver::main_tpcc`'s idle-compaction args.
    let idle_compaction_dead_ratio: f64 = arg(
        &parms,
        23,
        if gc { crate::bat_tree::idle_compaction::DEFAULT_VACUUM_DEAD_RATIO } else { 0.0 },
    );
    let idle_compaction_sweep_secs: f64 = arg(
        &parms,
        24,
        crate::bat_tree::idle_compaction::DEFAULT_VACUUM_SWEEP_INTERVAL.as_secs_f64(),
    );
    let idle_compaction = (idle_compaction_dead_ratio > 0.0)
        .then(|| (idle_compaction_dead_ratio, Duration::from_secs_f64(idle_compaction_sweep_secs)));

    // Same 3-way convention as `tpcc_driver::main_tpcc`'s `scan_pool_workers`
    // arg: omitted entirely -> on by default, sized to
    // `num_cpus.max(DEFAULT_QUERY_FANOUT * num_olap_threads)`, whenever the
    // cold corpus clears `MIN_ROWS_FOR_SCAN_POOL`; explicit "0" -> off;
    // explicit "N" -> exactly N workers. Every OLAP thread here always
    // scans (unlike YCSB's mixed workload), so `num_olap_threads` itself is
    // the right `expected_concurrent_queries` — no op-mix weighting needed.
    let scan_pool_workers: Option<usize> = match parms.get(25).map(|s| s.as_str()) {
        None if record_count >= crate::bat_bench::parallel_scan::MIN_ROWS_FOR_SCAN_POOL => {
            let fair_share_floor = crate::bat_tree::scan_pool::DEFAULT_QUERY_FANOUT * num_olap_threads;
            Some(crate::bat_tree::mvbt::default_max_workers().max(fair_share_floor).max(2))
        }
        None => None,
        Some(s) => match s.parse::<usize>() {
            Ok(0) | Err(_) => None,
            Ok(n) => Some(n.max(2)),
        },
    };

    run_s_ycsb(DriverConfig {
        ycsb: YcsbConfig {
            record_count,
            field_count,
            field_length,
        },
        num_write_threads,
        num_olap_threads,
        duration: Duration::from_secs(duration_secs),
        mix: SYcsbMix {
            arrival: arrival_ratio,
            hot_update: (1.0 - arrival_ratio).max(0.0),
        },
        hot_window,
        hot_theta,
        max_lateness,
        olap_lag,
        olap_span,
        write_all_fields,
        read_payload,
        execution_mode,
        gc,
        update_in_place,
        root_star_index,
        wal: wal_enabled.then(|| {
            (
                PathBuf::from(wal_path),
                Duration::from_millis(wal_flush_ms),
            )
        }),
        wal_lockfree_batch_size: None,
        output_dir: PathBuf::from("."),
        scan_pool_workers,
        idle_compaction,
    });
}
