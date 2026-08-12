//! YCSB benchmark driver: loads `record_count` rows, then runs a mix of the
//! five Core Workload operations (`ycsb_txn`) concurrently for a fixed
//! wall-clock duration, reporting per-op-type totals and overall throughput.
//! Mirrors `tpcc_driver`'s structure (population, barrier-synchronized
//! worker threads, timed phase, CSV + summary output).

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use crate::mv_bench::mem_stats::{MemSampler, DEFAULT_SAMPLE_INTERVAL};
use crate::mv_bench::ycsb_load::populate;
use crate::mv_bench::ycsb_random::{pick_op, random_scan_length, KeySampler, RequestDistribution, YcsbMix, YcsbOpType};
use crate::mv_bench::ycsb_schema::{YcsbConfig, YcsbTree};
use crate::mv_bench::ycsb_txn;
use crate::mv_root::index_root::RootIndexType;

pub struct DriverConfig {
    pub ycsb: YcsbConfig,
    pub num_threads: usize,
    pub duration: Duration,
    pub mix: YcsbMix,
    pub distribution: RequestDistribution,
    /// YCSB `maxscanlength`: a Scan op's length is uniform in `[1, this]`.
    pub max_scan_length: u64,
    /// YCSB `writeallfields`. False is the standard default: update one
    /// randomly selected field while preserving the others.
    pub write_all_fields: bool,
    /// Consume every payload byte on reads/scans. False measures key/visibility only.
    pub read_payload: bool,
    pub gc: bool,
    pub update_in_place: bool,
    pub root_star_index: RootIndexType,
    /// See `DriverConfig::wal` in `tpcc_driver` — same semantics here.
    pub wal: Option<(std::path::PathBuf, Duration)>,
    /// See `DriverConfig::wal_lockfree_batch_size` in `tpcc_driver` — same
    /// semantics here.
    pub wal_lockfree_batch_size: Option<usize>,
    /// See `DriverConfig::output_dir` in `tpcc_driver` — same semantics here;
    /// defaults to `.` for the standalone `ycsb` subcommand.
    pub output_dir: PathBuf,
}

/// See `tpcc_driver::TpccRunSummary` — same purpose, YCSB's shape.
pub struct YcsbRunSummary {
    pub throughput_ops_sec: f64,
    pub totals: [u64; NUM_COUNTERS],
}

const READ: usize = 0;
const UPDATE: usize = 1;
const INSERT: usize = 2;
const SCAN: usize = 3;
const RMW: usize = 4;
const NUM_COUNTERS: usize = 5;
/// Systematic latency sampling keeps percentile storage and clock reads
/// bounded even when YCSB-E completes millions of tiny scans per second.
const SCAN_LATENCY_SAMPLE_EVERY: u64 = 1024;
const TIMESERIES_CLOCK_EVERY: u64 = 256;

const COUNTER_NAMES: [&str; NUM_COUNTERS] = ["read", "update", "insert", "scan", "read_modify_write"];

struct WorkerStats {
    ops_per_sec: Vec<u64>,
    totals: [u64; NUM_COUNTERS],
    scanned_tuples: u64,
    /// Per-scan-op wall-clock latency (nanoseconds), one entry per YCSB-E scan op
    /// completed by this thread - empty for every other workload (A/B/C/D/F never
    /// take the `Scan` arm below).
    scan_latencies_ns: Vec<u64>,
}

#[allow(clippy::too_many_arguments)]
fn worker_thread(
    tree: Arc<YcsbTree>,
    cfg: YcsbConfig,
    mix: YcsbMix,
    sampler: Arc<KeySampler>,
    max_scan_length: u64,
    write_all_fields: bool,
    read_payload: bool,
    current_max_key: Arc<AtomicU64>,
    duration: Duration,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) -> WorkerStats {
    barrier.wait();

    let mut ops_per_sec = vec![0u64; duration.as_secs() as usize + 2];
    let mut totals = [0u64; NUM_COUNTERS];
    let mut scanned_tuples = 0u64;
    let mut scan_latencies_ns = Vec::new();
    let start = Instant::now();
    let mut completed = 0u64;
    let mut current_second = 0usize;

    while !stop.load(Relaxed) {
        let record_count = cfg.record_count;
        let max_key_now = current_max_key.load(Relaxed);

        match pick_op(&mix) {
            YcsbOpType::Read => {
                let key = sampler.sample(record_count, max_key_now);
                ycsb_txn::read_with_mode(&tree, key, read_payload);
                totals[READ] += 1;
            }
            YcsbOpType::Update => {
                let key = sampler.sample(record_count, max_key_now);
                ycsb_txn::update(&tree, &cfg, key, write_all_fields);
                totals[UPDATE] += 1;
            }
            YcsbOpType::Insert => {
                // Mints the next never-before-used key, past the initially
                // loaded range and every key inserted by this run so far.
                let key = current_max_key.fetch_add(1, Relaxed) + 1;
                ycsb_txn::insert(&tree, &cfg, key);
                totals[INSERT] += 1;
            }
            YcsbOpType::Scan => {
                let key = sampler.sample(record_count, max_key_now);
                let len = random_scan_length(max_scan_length);
                if totals[SCAN] % SCAN_LATENCY_SAMPLE_EVERY == 0 {
                    let scan_start = Instant::now();
                    scanned_tuples += ycsb_txn::scan_with_mode(&tree, key, len, read_payload) as u64;
                    scan_latencies_ns.push(scan_start.elapsed().as_nanos() as u64);
                } else {
                    scanned_tuples += ycsb_txn::scan_with_mode(&tree, key, len, read_payload) as u64;
                }
                totals[SCAN] += 1;
            }
            YcsbOpType::ReadModifyWrite => {
                let key = sampler.sample(record_count, max_key_now);
                ycsb_txn::read_modify_write_with_mode(&tree, &cfg, key, write_all_fields, read_payload);
                totals[RMW] += 1;
            }
        }

        completed += 1;
        if completed % TIMESERIES_CLOCK_EVERY == 0 {
            current_second = (start.elapsed().as_secs() as usize).min(ops_per_sec.len() - 1);
        }
        ops_per_sec[current_second] += 1;
    }

    WorkerStats { ops_per_sec, totals, scanned_tuples, scan_latencies_ns }
}

pub fn run_ycsb(cfg: DriverConfig) -> YcsbRunSummary {
    assert!(cfg.ycsb.record_count >= 1, "ycsb: record_count must be >= 1");

    let max_threads = crate::mv_tree::mvbt::default_max_workers().max(1);
    let mut num_threads = cfg.num_threads.max(1);
    // +1: the main thread also acquires a WorkerId, for the sequential
    // population phase before any worker thread is spawned (see tpcc_driver).
    if 1 + num_threads > max_threads {
        println!("!! 1 loader + {num_threads} workers > max_workers ({max_threads} = num_cpus); clamping.");
        num_threads = max_threads.saturating_sub(1).max(1);
    }

    fs::create_dir_all(&cfg.output_dir)
        .unwrap_or_else(|e| panic!("ycsb: failed to create output_dir {}: {e}", cfg.output_dir.display()));
    let mem_sampler = MemSampler::start(cfg.output_dir.join("mem_stats.csv"), DEFAULT_SAMPLE_INTERVAL);

    let tree = Arc::new(YcsbTree::make_standard(cfg.root_star_index));
    if cfg.gc {
        tree.enable_gc(cfg.update_in_place);
    }
    if let Some((wal_path, flush_interval)) = &cfg.wal {
        let _ = fs::remove_file(wal_path);
        match cfg.wal_lockfree_batch_size {
            Some(batch_size) => tree.enable_wal_lockfree(wal_path, *flush_interval, batch_size).expect("failed to attach lock-free WAL"),
            None => tree.enable_wal(wal_path, *flush_interval).expect("failed to attach WAL"),
        }
    }

    println!(
        "YCSB benchmark\n\
         - record_count        = {}\n\
         - field_count/length  = {}/{}\n\
         - workers             = {num_threads}\n\
         - duration            = {:?}\n\
         - mix                 = {:?}\n\
         - distribution        = {:?}\n\
         - max_scan_length     = {}\n\
         - write_all_fields    = {}\n\
         - read_payload       = {}\n\
         - GC                  = {} (update_in_place={})\n\
         - WAL                 = {}\n\
         - root*               = {}",
        cfg.ycsb.record_count, cfg.ycsb.field_count, cfg.ycsb.field_length,
        cfg.duration, cfg.mix, cfg.distribution, cfg.max_scan_length, cfg.write_all_fields, cfg.read_payload,
        cfg.gc, cfg.update_in_place,
        match (&cfg.wal, cfg.wal_lockfree_batch_size) {
            (Some((path, interval)), None) => format!("On, batched ({} @ {interval:?} flush)", path.display()),
            (Some((path, interval)), Some(batch_size)) => format!("On, lock-free batch={batch_size} ({} @ {interval:?} flush)", path.display()),
            (None, _) => "Off".to_string(),
        },
        cfg.root_star_index,
    );

    println!("Loading {} records...", cfg.ycsb.record_count);
    let load_start = Instant::now();
    populate(&tree, &cfg.ycsb);
    println!("Loaded {} records in {:?}.", cfg.ycsb.record_count, load_start.elapsed());

    let sampler = Arc::new(KeySampler::new(cfg.distribution, cfg.ycsb.record_count));
    let current_max_key = Arc::new(AtomicU64::new(cfg.ycsb.record_count));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(num_threads + 1));

    let duration = cfg.duration;
    let mix = cfg.mix;
    let ycsb_cfg = cfg.ycsb;
    let max_scan_length = cfg.max_scan_length;
    let write_all_fields = cfg.write_all_fields;
    let read_payload = cfg.read_payload;

    let handles: Vec<_> = (0..num_threads).map(|_| {
        let tree = tree.clone();
        let cfg = ycsb_cfg;
        let sampler = sampler.clone();
        let current_max_key = current_max_key.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        thread::spawn(move || worker_thread(tree, cfg, mix, sampler, max_scan_length,
            write_all_fields, read_payload, current_max_key, duration, stop, barrier))
    }).collect();

    // Releases at the same instant as every worker, once loading is done —
    // so the timed phase excludes load time entirely (see tpcc_driver).
    barrier.wait();
    let run_start = Instant::now();
    println!("Loading done. Running timed phase for {duration:?}...");
    thread::sleep(duration);
    stop.store(true, Relaxed);

    let stats: Vec<WorkerStats> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let actual_wall = run_start.elapsed();

    mem_sampler.stop();

    write_results(&stats, duration, actual_wall, &cfg.output_dir)
}

fn write_results(stats: &[WorkerStats], requested_duration: Duration, actual_wall: Duration, out_dir: &Path) -> YcsbRunSummary {
    let series_len = requested_duration.as_secs() as usize + 2;
    let mut per_sec = vec![0u64; series_len];
    let mut totals = [0u64; NUM_COUNTERS];
    let mut scanned_tuples = 0u64;
    let mut scan_latencies_ns: Vec<u64> = Vec::new();
    for s in stats {
        for (i, v) in s.ops_per_sec.iter().enumerate() {
            per_sec[i] += v;
        }
        for i in 0..NUM_COUNTERS {
            totals[i] += s.totals[i];
        }
        scanned_tuples += s.scanned_tuples;
        scan_latencies_ns.extend_from_slice(&s.scan_latencies_ns);
    }

    let ts_path = out_dir.join("ycsb_timeseries.csv");
    let _ = fs::remove_file(&ts_path);
    let mut ts_file = OpenOptions::new().create(true).append(true).open(&ts_path).unwrap();
    ts_file.write_all(b"elapsed_sec,ops_completed\n").unwrap();
    for (sec, count) in per_sec.iter().enumerate() {
        ts_file.write_all(format!("{sec},{count}\n").as_bytes()).unwrap();
    }

    // Systematically sampled summary (one of every SCAN_LATENCY_SAMPLE_EVERY scans),
    // avoiding a clock read and retained u64 for every one of millions of tiny scans.
    // Nearest-rank percentiles over the sorted sample,
    // in microseconds - empty (all-zero) file for every other workload. Every engine's
    // wrapper computes this the same way (see scripts/engines/common.py's
    // read_latency_summary, mirroring this exact nearest-rank formula) so percentiles are
    // comparable across engines even though each is computed in its own process/language.
    scan_latencies_ns.sort_unstable();
    let scan_latency_path = out_dir.join("ycsb_scan_latency_summary.csv");
    let _ = fs::remove_file(&scan_latency_path);
    let mut scan_latency_file = OpenOptions::new().create(true).append(true).open(&scan_latency_path).unwrap();
    scan_latency_file.write_all(b"p50_us,p95_us,p99_us,count,avg_us\n").unwrap();
    let pct = |p: f64| -> f64 {
        if scan_latencies_ns.is_empty() {
            0.0
        } else {
            let idx = ((p * (scan_latencies_ns.len() - 1) as f64).round() as usize).min(scan_latencies_ns.len() - 1);
            scan_latencies_ns[idx] as f64 / 1000.0
        }
    };
    let avg_us = if scan_latencies_ns.is_empty() {
        0.0
    } else {
        scan_latencies_ns.iter().sum::<u64>() as f64 / scan_latencies_ns.len() as f64 / 1000.0
    };
    scan_latency_file.write_all(format!(
        "{:.3},{:.3},{:.3},{},{:.3}\n",
        pct(0.50), pct(0.95), pct(0.99), scan_latencies_ns.len(), avg_us,
    ).as_bytes()).unwrap();

    let total_ops: u64 = totals.iter().sum();
    let throughput = total_ops as f64 / actual_wall.as_secs_f64();

    println!("\n===== Results (timed phase: {actual_wall:?}) =====");
    for i in 0..NUM_COUNTERS {
        println!("{:<20} {}", COUNTER_NAMES[i], totals[i]);
    }
    println!("{:<20} {}", "scanned_tuples", scanned_tuples);
    println!("{:<20} {}", "total_ops", total_ops);
    println!("{:<20} {:.2}", "throughput (ops/sec)", throughput);
    if !scan_latencies_ns.is_empty() {
        println!("{:<20} {}", "scan ops timed", scan_latencies_ns.len());
    }
    println!("Wrote {} and {}", ts_path.display(), scan_latency_path.display());

    YcsbRunSummary { throughput_ops_sec: throughput, totals }
}

pub fn main_ycsb(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms.get(idx).and_then(|s| s.parse().ok()).unwrap_or(default)
    }

    let workload: String = parms.get(2).cloned().unwrap_or_else(|| "a".to_string());
    let mix = YcsbMix::workload(&workload)
        .unwrap_or_else(|| panic!("ycsb: unknown workload '{workload}' (expected one of a, b, c, d, e, f)"));

    let record_count: u64 = arg(&parms, 3, 100_000);
    let num_threads: usize = arg(&parms, 4, num_cpus::get());
    let duration_secs: u64 = arg(&parms, 5, 30);

    let distribution_str = parms.get(6).map(|s| s.as_str()).unwrap_or("default").to_string();
    let theta: f64 = arg(&parms, 7, 0.99);
    let distribution = match distribution_str.as_str() {
        "uniform" => RequestDistribution::Uniform,
        "zipfian" => RequestDistribution::Zipfian { theta },
        "latest" => RequestDistribution::Latest { theta },
        _ => YcsbMix::default_distribution(&workload),
    };

    let field_count: usize = arg(&parms, 8, 10);
    let field_length: usize = arg(&parms, 9, 100);
    let max_scan_length: u64 = arg(&parms, 10, 100);
    let root_star_index = match parms.get(11).map(|s| s.as_str()).unwrap_or("fg") {
        "sk" => RootIndexType::SkipList,
        "ll" => RootIndexType::LinkedList,
        "bt" => RootIndexType::BTree,
        _ => RootIndexType::FrugalList,
    };
    let gc: bool = arg(&parms, 12, true);
    let update_in_place: bool = if gc { arg(&parms, 13, false) } else { false };
    let wal_enabled: bool = arg(&parms, 14, false);
    let wal_path: String = parms.get(15).cloned().unwrap_or_else(|| "ycsb_wal.log".to_string());
    let wal_flush_ms: u64 = arg(&parms, 16, 5);
    let write_all_fields: bool = arg(&parms, 17, false);
    let read_payload: bool = arg(&parms, 18, true);

    run_ycsb(DriverConfig {
        ycsb: YcsbConfig { record_count, field_count, field_length },
        num_threads,
        duration: Duration::from_secs(duration_secs),
        mix,
        distribution,
        max_scan_length,
        write_all_fields,
        read_payload,
        gc,
        update_in_place,
        root_star_index,
        wal: wal_enabled.then(|| (std::path::PathBuf::from(wal_path), Duration::from_millis(wal_flush_ms))),
        wal_lockfree_batch_size: None,
        output_dir: PathBuf::from("."),
    });
}
