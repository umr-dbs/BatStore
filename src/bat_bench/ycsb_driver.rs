//! YCSB benchmark driver: loads `record_count` rows, then runs a mix of the
//! five Core Workload operations (`ycsb_txn`) concurrently for a fixed
//! wall-clock duration, reporting per-op-type totals and overall throughput.
//! Mirrors `tpcc_driver`'s structure (population, barrier-synchronized
//! worker threads, timed phase, CSV + summary output).

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::thread;
use std::time::{Duration, Instant};

use triomphe::Arc;

use crate::bat_bench::mem_stats::{DEFAULT_SAMPLE_INTERVAL, MemSampler};
use crate::bat_bench::parallel_scan::MIN_ROWS_FOR_SCAN_POOL;
use crate::bat_bench::ycsb_load::populate;
use crate::bat_bench::ycsb_random::{
    KeySampler, RequestDistribution, YcsbMix, YcsbOpType, pick_op, random_scan_length,
};
use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbScanPool, YcsbTree};
use crate::bat_bench::ycsb_txn;
use crate::bat_bench::ycsb_txn::YcsbExecutionMode;
use crate::bat_root::index_root::RootIndexType;

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
    /// Single-operation commit-before-publish fast path, or the ordinary
    /// registered transaction lifecycle for controlled comparison.
    pub execution_mode: YcsbExecutionMode,
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
    /// Assigns the usertable a shared scan-worker pool (see
    /// `bat_tree::scan_pool::ScanWorkerPool`'s doc) of this many total
    /// threads, for `Scan` ops to fan out across via `ycsb_txn::
    /// scan_parallel` instead of walking the whole range on the calling
    /// thread — same feature and semantics as `tpcc_driver::DriverConfig::
    /// scan_pool_workers`. `None`/`Some(0)` disables it: every scan runs
    /// sequentially through `ycsb_txn::scan_with_mode`, unchanged from
    /// before this field existed. Every `Some(n)` is floored to 2 by
    /// `ScanWorkerPool::spawn`.
    ///
    /// Every builder of a `DriverConfig` (`main_ycsb`'s CLI parsing,
    /// `bat_bench::suite`) uses `default_scan_pool_workers` to fill this in
    /// rather than picking their own default, so the pool is on by default
    /// whenever the mix actually issues scans (`mix.scan > 0.0`) and the
    /// population is large enough for the pool to pay off, with no flag
    /// required to opt in — auto-sized to `num_cpus.max(scan_pool::
    /// DEFAULT_QUERY_FANOUT * expected_scan_concurrency)`, not just
    /// `num_cpus`, so a scan-issuing thread is still guaranteed a real
    /// fanout even when `num_threads` is large — see
    /// `expected_scan_concurrency`'s doc for why that estimate, not the raw
    /// thread count, is what both this sizing and `ScanWorkerPool::spawn`'s
    /// own `expected_concurrent_queries` below divide by.
    pub scan_pool_workers: Option<usize>,
    /// GC's own background vacuum sweep for this driver's one bare tree —
    /// see `bat_tree::idle_compaction::spawn_vacuum_thread`'s doc for the
    /// mechanism (`tpcc_driver::DriverConfig::idle_compaction` is the same
    /// concept for a `bat_db::Database`-backed workload). `Some((
    /// dead_ratio_threshold, sweep_interval))` runs it for the whole timed
    /// phase; `None` disables it — has no effect unless `gc` is also `true`.
    pub idle_compaction: Option<(f64, Duration)>,
}

/// How many of `num_threads` worker threads are expected to be running a
/// Scan op at any given instant: `num_threads * mix.scan`, rounded up and
/// floored at 1 — not the raw `num_threads`, since most of YCSB's mixes are
/// read/update/insert-heavy and only a `mix.scan` fraction of ops (issued by
/// whichever thread happens to draw one) are scans at all. Using the full
/// thread count here would overstate how many callers are actually sharing
/// the pool at once, understating each one's real `fair_query_fanout` share
/// (or tipping it into `None`, i.e. no parallelism) for any mix where scans
/// are a minority of ops. Shared by `default_scan_pool_workers` (to size the
/// pool) and `run_ycsb` (as `ScanWorkerPool::spawn`'s own
/// `expected_concurrent_queries`) so the two stay consistent regardless of
/// whether the pool ended up this size via that default or an explicit
/// override.
fn expected_scan_concurrency(num_threads: usize, mix: &YcsbMix) -> usize {
    ((num_threads as f64 * mix.scan).ceil() as usize).max(1)
}

/// Auto-sizes `DriverConfig::scan_pool_workers`: on whenever `mix` actually
/// issues scans (`mix.scan > 0.0`) and `record_count` is at or above
/// `parallel_scan::MIN_ROWS_FOR_SCAN_POOL` (below that, the pool's own
/// per-job overhead costs more than a sequential scan just takes — see that
/// constant's doc), `None` (off) otherwise. Sized to `num_cpus.max(
/// scan_pool::DEFAULT_QUERY_FANOUT * expected_scan_concurrency(num_threads,
/// mix))`, mirroring `tpcc_driver::main_tpcc`'s CLI parsing for `Table::
/// OrderLine`'s pool — guaranteeing every concurrent scanner at least
/// `DEFAULT_QUERY_FANOUT` workers of its own rather than letting
/// `fair_query_fanout` divide a plain `num_cpus`-sized pool down to a
/// too-thin share. Reusable by every caller that builds a `DriverConfig` —
/// not just CLI parsing — so a workload with no scans (A/B/C/D/F) never pays
/// for idle pool threads while YCSB-E gets the pool by default without any
/// extra configuration.
pub fn default_scan_pool_workers(
    record_count: u64,
    mix: &YcsbMix,
    num_threads: usize,
) -> Option<usize> {
    (mix.scan > 0.0 && record_count >= MIN_ROWS_FOR_SCAN_POOL).then(|| {
        let expected_scanners = expected_scan_concurrency(num_threads, mix);
        crate::bat_tree::mvbt::default_max_workers()
            .max(crate::bat_tree::scan_pool::DEFAULT_QUERY_FANOUT * expected_scanners)
    })
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
/// Systematic latency sampling keeps percentile storage and clock reads bounded even
/// when an in-memory YCSB workload completes millions of operations per second.
const OPERATION_LATENCY_SAMPLE_EVERY: u64 = 1024;
const TIMESERIES_CLOCK_EVERY: u64 = 256;

const COUNTER_NAMES: [&str; NUM_COUNTERS] =
    ["read", "update", "insert", "scan", "read_modify_write"];

struct WorkerStats {
    ops_per_sec: Vec<u64>,
    totals: [u64; NUM_COUNTERS],
    scanned_tuples: u64,
    /// Sampled wall-clock latency (nanoseconds), indexed like `totals`. Keeping one
    /// vector per operation makes mixed workloads such as A report reads and updates
    /// independently without retaining a sample for every completed operation.
    operation_latencies_ns: [Vec<u64>; NUM_COUNTERS],
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
    execution_mode: YcsbExecutionMode,
    current_max_key: Arc<AtomicU64>,
    duration: Duration,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    scan_pool: Option<Arc<YcsbScanPool>>,
) -> WorkerStats {
    barrier.wait();

    let mut ops_per_sec = vec![0u64; duration.as_secs() as usize + 2];
    let mut totals = [0u64; NUM_COUNTERS];
    let mut scanned_tuples = 0u64;
    let mut operation_latencies_ns: [Vec<u64>; NUM_COUNTERS] = std::array::from_fn(|_| Vec::new());
    let start = Instant::now();
    let mut completed = 0u64;
    let mut current_second = 0usize;

    while !stop.load(Relaxed) {
        let record_count = cfg.record_count;
        let max_key_now = current_max_key.load(Relaxed);

        match pick_op(&mix) {
            YcsbOpType::Read => {
                let key = sampler.sample(record_count, max_key_now);
                let op_start =
                    (totals[READ] % OPERATION_LATENCY_SAMPLE_EVERY == 0).then(Instant::now);
                ycsb_txn::read_with_mode(&tree, key, read_payload);
                if let Some(op_start) = op_start {
                    operation_latencies_ns[READ].push(op_start.elapsed().as_nanos() as u64);
                }
                totals[READ] += 1;
            }
            YcsbOpType::Update => {
                let key = sampler.sample(record_count, max_key_now);
                let op_start =
                    (totals[UPDATE] % OPERATION_LATENCY_SAMPLE_EVERY == 0).then(Instant::now);
                ycsb_txn::update_with_execution_mode(
                    &tree,
                    &cfg,
                    key,
                    write_all_fields,
                    execution_mode,
                );
                if let Some(op_start) = op_start {
                    operation_latencies_ns[UPDATE].push(op_start.elapsed().as_nanos() as u64);
                }
                totals[UPDATE] += 1;
            }
            YcsbOpType::Insert => {
                // Mints the next never-before-used key, past the initially
                // loaded range and every key inserted by this run so far.
                let key = current_max_key.fetch_add(1, Relaxed) + 1;
                let op_start =
                    (totals[INSERT] % OPERATION_LATENCY_SAMPLE_EVERY == 0).then(Instant::now);
                ycsb_txn::insert_with_execution_mode(&tree, &cfg, key, execution_mode);
                if let Some(op_start) = op_start {
                    operation_latencies_ns[INSERT].push(op_start.elapsed().as_nanos() as u64);
                }
                totals[INSERT] += 1;
            }
            YcsbOpType::Scan => {
                let key = sampler.sample(record_count, max_key_now);
                let len = random_scan_length(max_scan_length);
                if totals[SCAN] % OPERATION_LATENCY_SAMPLE_EVERY == 0 {
                    let scan_start = Instant::now();
                    scanned_tuples += ycsb_txn::scan_parallel(
                        scan_pool.as_deref(),
                        &tree,
                        key,
                        len,
                        read_payload,
                    ) as u64;
                    operation_latencies_ns[SCAN].push(scan_start.elapsed().as_nanos() as u64);
                } else {
                    scanned_tuples += ycsb_txn::scan_parallel(
                        scan_pool.as_deref(),
                        &tree,
                        key,
                        len,
                        read_payload,
                    ) as u64;
                }
                totals[SCAN] += 1;
            }
            YcsbOpType::ReadModifyWrite => {
                let key = sampler.sample(record_count, max_key_now);
                let op_start =
                    (totals[RMW] % OPERATION_LATENCY_SAMPLE_EVERY == 0).then(Instant::now);
                ycsb_txn::read_modify_write_with_execution_mode(
                    &tree,
                    &cfg,
                    key,
                    write_all_fields,
                    read_payload,
                    execution_mode,
                );
                if let Some(op_start) = op_start {
                    operation_latencies_ns[RMW].push(op_start.elapsed().as_nanos() as u64);
                }
                totals[RMW] += 1;
            }
        }

        completed += 1;
        if completed % TIMESERIES_CLOCK_EVERY == 0 {
            current_second = (start.elapsed().as_secs() as usize).min(ops_per_sec.len() - 1);
        }
        ops_per_sec[current_second] += 1;
    }

    WorkerStats {
        ops_per_sec,
        totals,
        scanned_tuples,
        operation_latencies_ns,
    }
}

pub fn run_ycsb(cfg: DriverConfig) -> YcsbRunSummary {
    assert!(
        cfg.ycsb.record_count >= 1,
        "ycsb: record_count must be >= 1"
    );

    let max_threads = crate::bat_tree::mvbt::default_max_workers().max(1);
    let mut num_threads = cfg.num_threads.max(1);
    // One more permanent WorkerId if idle compaction is enabled — see
    // `tpcc_driver::run_tpcc`'s identical `idle_compaction_cost`: the
    // vacuum thread `spawn_vacuum_thread` starts below calls
    // `compact_idle_pass`, which acquires its own `WorkerId` via
    // `self.worker_id()` just like any terminal thread, so it has to be
    // budgeted here too or its first sweep panics the registry once the
    // loader + workers have already filled every other slot.
    let idle_compaction_cost = if cfg.gc && cfg.idle_compaction.is_some() {
        1
    } else {
        0
    };
    // +1: the main thread also acquires a WorkerId, for the sequential
    // population phase before any worker thread is spawned (see tpcc_driver).
    let fixed_cost = 1 + idle_compaction_cost;
    if fixed_cost + num_threads > max_threads {
        println!(
            "!! {fixed_cost} loader/idle-compaction + {num_threads} workers > max_workers ({max_threads} = num_cpus); clamping."
        );
        num_threads = max_threads.saturating_sub(fixed_cost).max(1);
    }

    fs::create_dir_all(&cfg.output_dir).unwrap_or_else(|e| {
        panic!(
            "ycsb: failed to create output_dir {}: {e}",
            cfg.output_dir.display()
        )
    });
    let mem_sampler = MemSampler::start(
        cfg.output_dir.join("mem_stats.csv"),
        DEFAULT_SAMPLE_INTERVAL,
    );

    let tree = match &cfg.wal {
        Some((wal_path, flush_interval)) => {
            let _ = fs::remove_file(wal_path);
            let base = YcsbTree::make_standard_with_max_workers(
                cfg.root_star_index,
                fixed_cost + num_threads,
            );
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
            fixed_cost + num_threads,
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

    // See `DriverConfig::scan_pool_workers`'s doc: a pool worker thread
    // never calls `tree.worker_id()` (it only ever runs jobs built around
    // `READ_ONLY_SCAN_WORKER_ID`, same as `ycsb_txn::scan_parallel`'s own
    // sequential path), so unlike `num_threads` this is never counted
    // against `max_threads`/`fixed_cost + num_threads` above.
    let scan_pool: Option<Arc<YcsbScanPool>> = cfg.scan_pool_workers.filter(|&n| n > 0).map(|n| {
        Arc::new(YcsbScanPool::spawn(
            tree.clone(),
            n,
            Some(expected_scan_concurrency(num_threads, &cfg.mix)),
        ))
    });

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
         - execution_mode    = {:?}\n\
         - GC                  = {} (update_in_place={})\n\
         - WAL                 = {}\n\
         - root*               = {}\n\
         - scan_pool           = {}",
        cfg.ycsb.record_count,
        cfg.ycsb.field_count,
        cfg.ycsb.field_length,
        cfg.duration,
        cfg.mix,
        cfg.distribution,
        cfg.max_scan_length,
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
            Some(pool) => format!("On, {} workers", pool.num_workers()),
            None => "Off".to_string(),
        },
    );

    println!("Loading {} records...", cfg.ycsb.record_count);
    let load_start = Instant::now();
    populate(&tree, &cfg.ycsb);
    println!(
        "Loaded {} records in {:?}.",
        cfg.ycsb.record_count,
        load_start.elapsed()
    );

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
    let execution_mode = cfg.execution_mode;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let tree = tree.clone();
            let cfg = ycsb_cfg;
            let sampler = sampler.clone();
            let current_max_key = current_max_key.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            let scan_pool = scan_pool.clone();
            thread::spawn(move || {
                worker_thread(
                    tree,
                    cfg,
                    mix,
                    sampler,
                    max_scan_length,
                    write_all_fields,
                    read_payload,
                    execution_mode,
                    current_max_key,
                    duration,
                    stop,
                    barrier,
                    scan_pool,
                )
            })
        })
        .collect();

    // Releases at the same instant as every worker, once loading is done —
    // so the timed phase excludes load time entirely (see tpcc_driver).
    barrier.wait();
    let run_start = Instant::now();
    println!("Loading done. Running timed phase for {duration:?}...");
    thread::sleep(duration);
    stop.store(true, Relaxed);
    vacuum_stop.store(true, Relaxed);

    let stats: Vec<WorkerStats> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let actual_wall = run_start.elapsed();

    mem_sampler.stop();
    #[cfg(feature = "gc-stats")]
    write_gc_stats(&tree, &cfg.output_dir);

    write_results(&stats, duration, actual_wall, &cfg.output_dir)
}

/// Dumps the per-shard local-reuse/steal/fresh-alloc breakdown accumulated
/// over the whole run (population + timed phase) — see `bat_gc::GcStats`'s
/// doc. Written the same way `mem_stats.csv` is (a plain CSV in `out_dir`),
/// not stdout, so it stays parseable by a Python harness at scale. Only
/// compiled in with the `gc-stats` feature (see its doc in `Cargo.toml`) —
/// without it, no `gc_stats.csv` is written at all (rather than an
/// all-zero/misleading one), so a Python reader can tell "feature off" apart
/// from "no reclaim activity happened."
#[cfg(feature = "gc-stats")]
fn write_gc_stats(tree: &YcsbTree, out_dir: &Path) {
    let path = out_dir.join("gc_stats.csv");
    let _ = fs::remove_file(&path);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("gc_stats: failed to open {}: {e}", path.display()));
    file.write_all(b"shard,local_reuse,steal,fresh_alloc\n").unwrap();
    for (shard, s) in tree.tracker().gc_stats_per_shard().into_iter().enumerate() {
        file.write_all(
            format!("{shard},{},{},{}\n", s.local_reuse, s.steal, s.fresh_alloc).as_bytes(),
        )
        .unwrap();
    }
}

fn write_results(
    stats: &[WorkerStats],
    requested_duration: Duration,
    actual_wall: Duration,
    out_dir: &Path,
) -> YcsbRunSummary {
    let series_len = requested_duration.as_secs() as usize + 2;
    let mut per_sec = vec![0u64; series_len];
    let mut totals = [0u64; NUM_COUNTERS];
    let mut scanned_tuples = 0u64;
    let mut operation_latencies_ns: [Vec<u64>; NUM_COUNTERS] = std::array::from_fn(|_| Vec::new());
    for s in stats {
        for (i, v) in s.ops_per_sec.iter().enumerate() {
            per_sec[i] += v;
        }
        for i in 0..NUM_COUNTERS {
            totals[i] += s.totals[i];
        }
        scanned_tuples += s.scanned_tuples;
        for (combined, worker) in operation_latencies_ns
            .iter_mut()
            .zip(&s.operation_latencies_ns)
        {
            combined.extend_from_slice(worker);
        }
    }

    let ts_path = out_dir.join("ycsb_timeseries.csv");
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

    let operation_latency_path = out_dir.join("ycsb_operation_latency_summary.csv");
    let _ = fs::remove_file(&operation_latency_path);
    let mut operation_latency_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&operation_latency_path)
        .unwrap();
    operation_latency_file
        .write_all(b"operation,p50_us,p95_us,p99_us,count,avg_us,sample_every\n")
        .unwrap();

    for samples in &mut operation_latencies_ns {
        samples.sort_unstable();
    }
    let stats = |samples: &[u64]| -> (f64, f64, f64, f64) {
        let pct = |p: f64| -> f64 {
            if samples.is_empty() {
                0.0
            } else {
                let idx =
                    ((p * (samples.len() - 1) as f64).round() as usize).min(samples.len() - 1);
                samples[idx] as f64 / 1000.0
            }
        };
        let avg_us = if samples.is_empty() {
            0.0
        } else {
            samples.iter().map(|&v| v as u128).sum::<u128>() as f64 / samples.len() as f64 / 1000.0
        };
        (pct(0.50), pct(0.95), pct(0.99), avg_us)
    };
    for (operation, samples) in COUNTER_NAMES.iter().zip(&operation_latencies_ns) {
        let (p50, p95, p99, avg) = stats(samples);
        operation_latency_file
            .write_all(
                format!(
                    "{operation},{p50:.3},{p95:.3},{p99:.3},{},{avg:.3},{}\n",
                    samples.len(),
                    OPERATION_LATENCY_SAMPLE_EVERY,
                )
                .as_bytes(),
            )
            .unwrap();
    }

    // Retain the original scan-only file so existing manifests and plotting scripts keep
    // working. Its row is now derived from the scan row in the all-operation summary.
    let scan_latency_path = out_dir.join("ycsb_scan_latency_summary.csv");
    let _ = fs::remove_file(&scan_latency_path);
    let mut scan_latency_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&scan_latency_path)
        .unwrap();
    scan_latency_file
        .write_all(b"p50_us,p95_us,p99_us,count,avg_us\n")
        .unwrap();
    let scan_samples = &operation_latencies_ns[SCAN];
    let (scan_p50, scan_p95, scan_p99, scan_avg) = stats(scan_samples);
    scan_latency_file
        .write_all(
            format!(
                "{:.3},{:.3},{:.3},{},{:.3}\n",
                scan_p50,
                scan_p95,
                scan_p99,
                scan_samples.len(),
                scan_avg,
            )
            .as_bytes(),
        )
        .unwrap();

    let total_ops: u64 = totals.iter().sum();
    let throughput = total_ops as f64 / actual_wall.as_secs_f64();

    println!("\n===== Results (timed phase: {actual_wall:?}) =====");
    for i in 0..NUM_COUNTERS {
        println!("{:<20} {}", COUNTER_NAMES[i], totals[i]);
    }
    println!("{:<20} {}", "scanned_tuples", scanned_tuples);
    println!("{:<20} {}", "total_ops", total_ops);
    println!("{:<20} {:.2}", "throughput (ops/sec)", throughput);
    for (operation, samples) in COUNTER_NAMES.iter().zip(&operation_latencies_ns) {
        if !samples.is_empty() {
            println!("{operation:<20} {} latency samples", samples.len());
        }
    }
    println!(
        "Wrote {}, {} and {}",
        ts_path.display(),
        operation_latency_path.display(),
        scan_latency_path.display()
    );

    YcsbRunSummary {
        throughput_ops_sec: throughput,
        totals,
    }
}

pub fn main_ycsb(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms
            .get(idx)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }

    let workload: String = parms.get(2).cloned().unwrap_or_else(|| "a".to_string());
    let mix = YcsbMix::workload(&workload).unwrap_or_else(|| {
        panic!("ycsb: unknown workload '{workload}' (expected one of a, b, c, d, e, f)")
    });

    let record_count: u64 = arg(&parms, 3, 100_000);
    let num_threads: usize = arg(&parms, 4, num_cpus::get());
    let duration_secs: u64 = arg(&parms, 5, 30);

    let distribution_str = parms
        .get(6)
        .map(|s| s.as_str())
        .unwrap_or("default")
        .to_string();
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
    let wal_path: String = parms
        .get(15)
        .cloned()
        .unwrap_or_else(|| "ycsb_wal.log".to_string());
    let wal_flush_ms: u64 = arg(&parms, 16, 5);
    let write_all_fields: bool = arg(&parms, 17, false);
    let read_payload: bool = arg(&parms, 18, true);
    let execution_mode = match parms.get(19).map(String::as_str).unwrap_or("atomic") {
        "transaction" | "tx" => YcsbExecutionMode::Transaction,
        "atomic" | "auto" | "autocommit" => YcsbExecutionMode::Atomic,
        other => panic!("ycsb: invalid execution mode '{other}' (expected atomic or transaction)"),
    };
    // Same 3-way convention as `tpcc_driver::main_tpcc`'s position 22
    // (`scan_pool_workers`'s doc): omitted entirely -> `default_scan_pool_workers`
    // decides (on by default for a scan-issuing mix with enough rows);
    // explicit "0" -> off; explicit "N" -> exactly N workers.
    let scan_pool_workers: Option<usize> = match parms.get(20).map(|s| s.as_str()) {
        None => default_scan_pool_workers(record_count, &mix, num_threads),
        Some(s) => match s.parse::<usize>() {
            Ok(0) | Err(_) => None,
            Ok(n) => Some(n.max(2)),
        },
    };
    // Same "0.0 explicitly opts out, otherwise defaults on whenever GC is
    // on" convention as `tpcc_driver::main_tpcc`'s idle-compaction args.
    let idle_compaction_dead_ratio: f64 = arg(
        &parms,
        21,
        if gc {
            crate::bat_tree::idle_compaction::DEFAULT_VACUUM_DEAD_RATIO
        } else {
            0.0
        },
    );
    let idle_compaction_sweep_secs: f64 = arg(
        &parms,
        22,
        crate::bat_tree::idle_compaction::DEFAULT_VACUUM_SWEEP_INTERVAL.as_secs_f64(),
    );
    let idle_compaction = (idle_compaction_dead_ratio > 0.0).then(|| {
        (
            idle_compaction_dead_ratio,
            Duration::from_secs_f64(idle_compaction_sweep_secs),
        )
    });

    run_ycsb(DriverConfig {
        ycsb: YcsbConfig {
            record_count,
            field_count,
            field_length,
        },
        num_threads,
        duration: Duration::from_secs(duration_secs),
        mix,
        distribution,
        max_scan_length,
        write_all_fields,
        read_payload,
        execution_mode,
        gc,
        update_in_place,
        root_star_index,
        wal: wal_enabled.then(|| {
            (
                std::path::PathBuf::from(wal_path),
                Duration::from_millis(wal_flush_ms),
            )
        }),
        wal_lockfree_batch_size: None,
        output_dir: PathBuf::from("."),
        scan_pool_workers,
        idle_compaction,
    });
}
