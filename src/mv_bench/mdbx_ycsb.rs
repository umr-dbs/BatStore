//! libmdbx-backed YCSB benchmark driver - a second MVCC engine (path-copying/
//! copy-on-write B+Tree via the `libmdbx` crate) run through the SAME
//! standard YCSB A-F workloads as `ycsb_driver.rs`, for a genuine
//! version-chain-vs-path-copying MVCC comparison. Mirrors `ycsb_driver.rs`'s
//! structure (population, barrier-synchronized worker threads, timed phase,
//! CSV + summary output) as closely as possible, reusing everything about
//! `ycsb_random`/`ycsb_schema` that's storage-engine-agnostic (row/mix/key
//! generation, `YcsbRow`'s `WalPayload` byte codec as the value encoding) -
//! only the actual read/write/scan operations are reimplemented against
//! libmdbx's `Transaction<RO|RW>` API, since there's no trait boundary in
//! this codebase between cMVBT's own tree and its transaction/business logic
//! (a fresh `mdbx_tpcc.rs`/this file, not a generic backend swapped into the
//! existing drivers).
//!
//! libmdbx is a single-writer MVCC store (like LMDB): only one read-write
//! transaction can be active process-wide at a time, so any workload with a
//! write component (update/insert/read-modify-write) will show throughput
//! serializing on that single writer as thread count grows - this is a real,
//! expected characteristic of path-copying/CoW MVCC to surface honestly, not
//! a driver bug.
//!
//! Durability: `SyncMode::SafeNoSync` (not full fsync-per-commit durability)
//! by default - matching the earlier libmdbx comparison tool
//! (`mdbx_crud_load.c`, see project memory)'s own finding that full
//! per-commit fsync durability is impractically slow for a benchmark where
//! every op is its own transaction.

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use libmdbx::{Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode, TableFlags, WriteFlags};

use crate::mv_bench::mem_stats::{MemSampler, DEFAULT_SAMPLE_INTERVAL};
use crate::mv_bench::ycsb_random::{pick_op, random_row, random_scan_length, KeySampler, RequestDistribution, YcsbMix, YcsbOpType};
use crate::mv_bench::ycsb_schema::{YcsbConfig, YcsbKey};
use crate::mv_wal::record::WalPayload;

pub struct MdbxYcsbConfig {
    pub ycsb: YcsbConfig,
    pub num_threads: usize,
    pub duration: Duration,
    pub mix: YcsbMix,
    pub distribution: RequestDistribution,
    pub max_scan_length: u64,
    /// The libmdbx environment directory - caller is responsible for wiping
    /// it beforehand (see scripts/engines/libmdbx.py's use of
    /// common.fresh_scratch_dir), matching this suite's own `--trunc`-style
    /// convention for every other on-disk engine.
    pub db_path: PathBuf,
    pub output_dir: PathBuf,
}

pub struct MdbxYcsbRunSummary {
    pub throughput_ops_sec: f64,
    pub totals: [u64; NUM_COUNTERS],
}

const READ: usize = 0;
const UPDATE: usize = 1;
const INSERT: usize = 2;
const SCAN: usize = 3;
const RMW: usize = 4;
const NUM_COUNTERS: usize = 5;
const COUNTER_NAMES: [&str; NUM_COUNTERS] = ["read", "update", "insert", "scan", "read_modify_write"];

fn open_db(path: &std::path::Path, num_threads: usize) -> Database<NoWriteMap> {
    fs::create_dir_all(path).unwrap_or_else(|e| panic!("mdbx_ycsb: failed to create db dir {}: {e}", path.display()));
    // libmdbx's reader-slot table defaults to 61 (MDBX_READERS_FULL beyond that) -
    // below our own thread-count sweep (up to 128), which was silently aborting/
    // hanging worker threads via the `.expect` calls below. Size it to the actual
    // thread count plus headroom for the table-creation txn and any internal use.
    let options = DatabaseOptions {
        max_readers: Some((num_threads as std::ffi::c_uint).saturating_add(8)),
        mode: Mode::ReadWrite(ReadWriteOptions { sync_mode: SyncMode::SafeNoSync, ..Default::default() }),
        ..Default::default()
    };
    let db = Database::<NoWriteMap>::open_with_options(path, options)
        .unwrap_or_else(|e| panic!("mdbx_ycsb: failed to open database at {}: {e}", path.display()));
    // The unnamed/default table needs one RW transaction to exist before any reader can
    // open it.
    let txn = db.begin_rw_txn().expect("mdbx_ycsb: begin_rw_txn (table creation)");
    txn.create_table(None, TableFlags::empty()).expect("mdbx_ycsb: create_table");
    txn.commit().expect("mdbx_ycsb: commit (table creation)");
    db
}

fn encode_row(cfg: &YcsbConfig) -> Vec<u8> {
    let row = random_row(cfg);
    let mut buf = Vec::new();
    row.wal_encode(&mut buf);
    buf
}

fn mdbx_read(db: &Database<NoWriteMap>, key: YcsbKey) -> bool {
    let txn = db.begin_ro_txn().expect("mdbx_ycsb: begin_ro_txn");
    let table = txn.open_table(None).expect("mdbx_ycsb: open_table");
    txn.get::<Vec<u8>>(&table, &key.to_be_bytes()).expect("mdbx_ycsb: get").is_some()
}

fn mdbx_update(db: &Database<NoWriteMap>, cfg: &YcsbConfig, key: YcsbKey) -> bool {
    let txn = db.begin_rw_txn().expect("mdbx_ycsb: begin_rw_txn");
    let table = txn.open_table(None).expect("mdbx_ycsb: open_table");
    let exists = txn.get::<Vec<u8>>(&table, &key.to_be_bytes()).expect("mdbx_ycsb: get").is_some();
    if exists {
        let buf = encode_row(cfg);
        txn.put(&table, key.to_be_bytes(), &buf, WriteFlags::UPSERT).expect("mdbx_ycsb: put");
    }
    txn.commit().expect("mdbx_ycsb: commit");
    exists
}

fn mdbx_insert(db: &Database<NoWriteMap>, cfg: &YcsbConfig, key: YcsbKey) {
    let txn = db.begin_rw_txn().expect("mdbx_ycsb: begin_rw_txn");
    let table = txn.open_table(None).expect("mdbx_ycsb: open_table");
    let buf = encode_row(cfg);
    txn.put(&table, key.to_be_bytes(), &buf, WriteFlags::UPSERT).expect("mdbx_ycsb: put");
    txn.commit().expect("mdbx_ycsb: commit");
}

/// Returns the number of rows actually scanned (can be `< len` near the end of the loaded
/// key range) - same contract as `ycsb_txn::scan`.
fn mdbx_scan(db: &Database<NoWriteMap>, start_key: YcsbKey, len: u64) -> usize {
    let txn = db.begin_ro_txn().expect("mdbx_ycsb: begin_ro_txn");
    let table = txn.open_table(None).expect("mdbx_ycsb: open_table");
    let mut cursor = txn.cursor(&table).expect("mdbx_ycsb: cursor");
    let mut count = 0u64;
    let mut item = cursor
        .set_range::<Vec<u8>, Vec<u8>>(&start_key.to_be_bytes())
        .expect("mdbx_ycsb: cursor.set_range");
    while item.is_some() && count < len {
        count += 1;
        if count >= len {
            break;
        }
        item = cursor.next::<Vec<u8>, Vec<u8>>().expect("mdbx_ycsb: cursor.next");
    }
    count as usize
}

fn mdbx_read_modify_write(db: &Database<NoWriteMap>, cfg: &YcsbConfig, key: YcsbKey) -> bool {
    let _ = mdbx_read(db, key);
    mdbx_update(db, cfg, key)
}

fn populate(db: &Database<NoWriteMap>, cfg: &YcsbConfig) {
    for key in 1..=cfg.record_count {
        mdbx_insert(db, cfg, key as YcsbKey);
    }
}

struct WorkerStats {
    ops_per_sec: Vec<u64>,
    totals: [u64; NUM_COUNTERS],
    scanned_tuples: u64,
    scan_latencies_ns: Vec<u64>,
}

#[allow(clippy::too_many_arguments)]
fn worker_thread(
    db: Arc<Database<NoWriteMap>>,
    cfg: YcsbConfig,
    mix: YcsbMix,
    sampler: Arc<KeySampler>,
    max_scan_length: u64,
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

    while !stop.load(Relaxed) {
        let record_count = cfg.record_count;
        let max_key_now = current_max_key.load(Relaxed);

        match pick_op(&mix) {
            YcsbOpType::Read => {
                let key = sampler.sample(record_count, max_key_now);
                mdbx_read(&db, key);
                totals[READ] += 1;
            }
            YcsbOpType::Update => {
                let key = sampler.sample(record_count, max_key_now);
                mdbx_update(&db, &cfg, key);
                totals[UPDATE] += 1;
            }
            YcsbOpType::Insert => {
                let key = current_max_key.fetch_add(1, Relaxed) + 1;
                mdbx_insert(&db, &cfg, key);
                totals[INSERT] += 1;
            }
            YcsbOpType::Scan => {
                let key = sampler.sample(record_count, max_key_now);
                let len = random_scan_length(max_scan_length);
                let scan_start = Instant::now();
                scanned_tuples += mdbx_scan(&db, key, len) as u64;
                scan_latencies_ns.push(scan_start.elapsed().as_nanos() as u64);
                totals[SCAN] += 1;
            }
            YcsbOpType::ReadModifyWrite => {
                let key = sampler.sample(record_count, max_key_now);
                mdbx_read_modify_write(&db, &cfg, key);
                totals[RMW] += 1;
            }
        }

        let idx = (start.elapsed().as_secs() as usize).min(ops_per_sec.len() - 1);
        ops_per_sec[idx] += 1;
    }

    WorkerStats { ops_per_sec, totals, scanned_tuples, scan_latencies_ns }
}

pub fn run_mdbx_ycsb(cfg: MdbxYcsbConfig) -> MdbxYcsbRunSummary {
    assert!(cfg.ycsb.record_count >= 1, "mdbx_ycsb: record_count must be >= 1");

    fs::create_dir_all(&cfg.output_dir)
        .unwrap_or_else(|e| panic!("mdbx_ycsb: failed to create output_dir {}: {e}", cfg.output_dir.display()));
    let mem_sampler = MemSampler::start(cfg.output_dir.join("mem_stats.csv"), DEFAULT_SAMPLE_INTERVAL);

    let num_threads = cfg.num_threads.max(1);
    let db = Arc::new(open_db(&cfg.db_path, num_threads));

    println!(
        "libmdbx YCSB benchmark\n\
         - record_count        = {}\n\
         - field_count/length  = {}/{}\n\
         - workers             = {num_threads}\n\
         - duration            = {:?}\n\
         - mix                 = {:?}\n\
         - distribution        = {:?}\n\
         - max_scan_length     = {}\n\
         - db_path             = {}",
        cfg.ycsb.record_count, cfg.ycsb.field_count, cfg.ycsb.field_length,
        cfg.duration, cfg.mix, cfg.distribution, cfg.max_scan_length, cfg.db_path.display(),
    );

    println!("Loading {} records...", cfg.ycsb.record_count);
    let load_start = Instant::now();
    populate(&db, &cfg.ycsb);
    println!("Loaded {} records in {:?}.", cfg.ycsb.record_count, load_start.elapsed());

    let sampler = Arc::new(KeySampler::new(cfg.distribution, cfg.ycsb.record_count));
    let current_max_key = Arc::new(AtomicU64::new(cfg.ycsb.record_count));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(num_threads + 1));

    let duration = cfg.duration;
    let mix = cfg.mix;
    let ycsb_cfg = cfg.ycsb;
    let max_scan_length = cfg.max_scan_length;

    let handles: Vec<_> = (0..num_threads).map(|_| {
        let db = db.clone();
        let cfg = ycsb_cfg;
        let sampler = sampler.clone();
        let current_max_key = current_max_key.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        thread::spawn(move || worker_thread(db, cfg, mix, sampler, max_scan_length, current_max_key, duration, stop, barrier))
    }).collect();

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

fn write_results(stats: &[WorkerStats], requested_duration: Duration, actual_wall: Duration, out_dir: &std::path::Path) -> MdbxYcsbRunSummary {
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

    // Summary (not raw per-op samples), same format/reasoning as cMVBT's own
    // ycsb_driver.rs::write_results - see that file's comment on why.
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
    println!("Wrote {} and {}", ts_path.display(), scan_latency_path.display());

    MdbxYcsbRunSummary { throughput_ops_sec: throughput, totals }
}

pub fn main_mdbx_ycsb(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms.get(idx).and_then(|s| s.parse().ok()).unwrap_or(default)
    }

    // Positional order mirrors the existing `ycsb` subcommand (main_ycsb) wherever the
    // concept overlaps, dropping MVBTree-internal knobs (root_star_index, gc,
    // update_in_place, WAL) that have no libmdbx equivalent - see mdbx_tpcc.rs/
    // scripts/engines/libmdbx.py for the same convention.
    let workload: String = parms.get(2).cloned().unwrap_or_else(|| "a".to_string());
    let mix = YcsbMix::workload(&workload)
        .unwrap_or_else(|| panic!("mdbx_ycsb: unknown workload '{workload}' (expected one of a, b, c, d, e, f)"));

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
    let db_path: String = parms.get(11).cloned().unwrap_or_else(|| "mdbx_ycsb_db".to_string());

    run_mdbx_ycsb(MdbxYcsbConfig {
        ycsb: YcsbConfig { record_count, field_count, field_length },
        num_threads,
        duration: Duration::from_secs(duration_secs),
        mix,
        distribution,
        max_scan_length,
        db_path: PathBuf::from(db_path),
        output_dir: PathBuf::from("."),
    });
}
