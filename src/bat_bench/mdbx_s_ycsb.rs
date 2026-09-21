//! libmdbx-backed "S-YCSB" streaming benchmark - the same near-sorted-
//! arrival + hot-tail-update + straddling-OLAP-scan workload as
//! `s_ycsb_driver.rs`, run against libmdbx for a version-chain-vs-
//! path-copying MVCC comparison. Mirrors `mdbx_ycsb.rs`'s structure and
//! conventions (own `open_db`/read/write/scan reimplementations against
//! `libmdbx::Transaction<RO|RW>`, since there's no shared trait boundary in
//! this codebase between BatStore's tree and its transaction logic - see that
//! file's module doc) while reusing every storage-engine-agnostic piece of
//! `s_ycsb_random` (key minting, hot-tail sampling, OLAP scan bounds) and
//! `ycsb_random`/`ycsb_schema` (row generation, config) as-is.
//!
//! Unlike BatStore's tree, libmdbx's `put(..., WriteFlags::UPSERT)` doesn't
//! distinguish "insert" from "update" at all - a late/duplicate arrival that
//! collides with an already-materialized key is handled by the exact same
//! call as a brand-new one, with no separate conflict path to reason about.
//! `new_arrival` vs `late_upsert` is still reported (an existence check
//! before the put) purely so the two engines' result CSVs stay
//! directly comparable, not because libmdbx needs the distinction.

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use libmdbx::{
    Database, DatabaseOptions, Mode, ReadWriteOptions, SyncMode, TableFlags, WriteFlags, WriteMap,
};

use crate::bat_bench::mem_stats::{DEFAULT_SAMPLE_INTERVAL, MemSampler};
use crate::bat_bench::s_ycsb_random::{
    HotTailSampler, SYcsbMix, SYcsbWriteOp, mint_arrival_key, olap_scan_bounds, pick_write_op,
};
use crate::bat_bench::ycsb_random::random_row;
use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbKey};
use crate::bat_wal::record::WalPayload;

pub struct MdbxSYcsbConfig {
    pub ycsb: YcsbConfig,
    pub num_write_threads: usize,
    pub num_olap_threads: usize,
    pub duration: Duration,
    pub mix: SYcsbMix,
    pub hot_window: u64,
    pub hot_theta: f64,
    pub max_lateness: u64,
    pub olap_lag: u64,
    pub olap_span: u64,
    pub read_payload: bool,
    /// See `MdbxYcsbConfig::db_path` in `mdbx_ycsb.rs` - same convention.
    pub db_path: PathBuf,
    pub output_dir: PathBuf,
}

pub struct MdbxSYcsbRunSummary {
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

fn open_db(path: &std::path::Path, num_threads: usize) -> Database<WriteMap> {
    fs::create_dir_all(path).unwrap_or_else(|e| {
        panic!(
            "mdbx_s_ycsb: failed to create db dir {}: {e}",
            path.display()
        )
    });
    // See mdbx_ycsb.rs::open_db's doc for why max_readers is sized off the
    // actual thread count rather than left at libmdbx's default of 61.
    let options = DatabaseOptions {
        max_readers: Some((num_threads as std::ffi::c_uint).saturating_add(8)),
        mode: Mode::ReadWrite(ReadWriteOptions {
            sync_mode: SyncMode::UtterlyNoSync,
            ..Default::default()
        }),
        ..Default::default()
    };
    let db = Database::<WriteMap>::open_with_options(path, options).unwrap_or_else(|e| {
        panic!(
            "mdbx_s_ycsb: failed to open database at {}: {e}",
            path.display()
        )
    });
    let txn = db
        .begin_rw_txn()
        .expect("mdbx_s_ycsb: begin_rw_txn (table creation)");
    txn.create_table(None, TableFlags::empty())
        .expect("mdbx_s_ycsb: create_table");
    txn.commit().expect("mdbx_s_ycsb: commit (table creation)");
    db
}

fn encode_row(cfg: &YcsbConfig) -> Vec<u8> {
    let row = random_row(cfg);
    let mut buf = Vec::new();
    row.wal_encode(&mut buf);
    buf
}

fn mdbx_arrival_upsert(db: &Database<WriteMap>, cfg: &YcsbConfig, key: YcsbKey) -> bool {
    let txn = db.begin_rw_txn().expect("mdbx_s_ycsb: begin_rw_txn");
    let table = txn.open_table(None).expect("mdbx_s_ycsb: open_table");
    let existed = txn
        .get::<Vec<u8>>(&table, &key.to_be_bytes())
        .expect("mdbx_s_ycsb: get")
        .is_some();
    let buf = encode_row(cfg);
    txn.put(&table, key.to_be_bytes(), &buf, WriteFlags::UPSERT)
        .expect("mdbx_s_ycsb: put");
    txn.commit().expect("mdbx_s_ycsb: commit");
    !existed
}

fn mdbx_hot_update(db: &Database<WriteMap>, cfg: &YcsbConfig, key: YcsbKey) -> bool {
    let txn = db.begin_rw_txn().expect("mdbx_s_ycsb: begin_rw_txn");
    let table = txn.open_table(None).expect("mdbx_s_ycsb: open_table");
    let exists = txn
        .get::<Vec<u8>>(&table, &key.to_be_bytes())
        .expect("mdbx_s_ycsb: get")
        .is_some();
    if exists {
        let buf = encode_row(cfg);
        txn.put(&table, key.to_be_bytes(), &buf, WriteFlags::UPSERT)
            .expect("mdbx_s_ycsb: put");
    }
    txn.commit().expect("mdbx_s_ycsb: commit");
    exists
}

fn mdbx_scan(
    db: &Database<WriteMap>,
    start_key: YcsbKey,
    len: u64,
    read_payload: bool,
) -> (usize, u64) {
    let txn = db.begin_ro_txn().expect("mdbx_s_ycsb: begin_ro_txn");
    let txn_id = txn.id();
    let table = txn.open_table(None).expect("mdbx_s_ycsb: open_table");
    let mut cursor = txn.cursor(&table).expect("mdbx_s_ycsb: cursor");
    let mut count = 0u64;
    let mut item = cursor
        .set_range::<Vec<u8>, Vec<u8>>(&start_key.to_be_bytes())
        .expect("mdbx_s_ycsb: cursor.set_range");
    while item.is_some() && count < len {
        if read_payload {
            if let Some((_, bytes)) = &item {
                std::hint::black_box(bytes.iter().fold(0u8, |a, b| a.wrapping_add(*b)));
            }
        }
        count += 1;
        if count >= len {
            break;
        }
        item = cursor
            .next::<Vec<u8>, Vec<u8>>()
            .expect("mdbx_s_ycsb: cursor.next");
    }
    (count as usize, txn_id)
}

const LOAD_BATCH_SIZE: u64 = 10_000;

fn populate(db: &Database<WriteMap>, cfg: &YcsbConfig) {
    let mut key = 1u64;
    while key <= cfg.record_count {
        let batch_end = (key + LOAD_BATCH_SIZE - 1).min(cfg.record_count);
        let txn = db.begin_rw_txn().expect("mdbx_s_ycsb: begin_rw_txn (load)");
        let table = txn
            .open_table(None)
            .expect("mdbx_s_ycsb: open_table (load)");
        for k in key..=batch_end {
            let buf = encode_row(cfg);
            txn.put(&table, k.to_be_bytes(), &buf, WriteFlags::UPSERT)
                .expect("mdbx_s_ycsb: put (load)");
        }
        txn.commit().expect("mdbx_s_ycsb: commit (load)");
        key = batch_end + 1;
    }
}

struct WriteWorkerStats {
    ops_per_sec: Vec<u64>,
    totals: [u64; NUM_WRITE_COUNTERS],
}

#[allow(clippy::too_many_arguments)]
fn write_worker_thread(
    db: Arc<Database<WriteMap>>,
    cfg: YcsbConfig,
    mix: SYcsbMix,
    hot_sampler: Arc<HotTailSampler>,
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
                let was_new = mdbx_arrival_upsert(&db, &cfg, key);
                totals[if was_new { NEW_ARRIVAL } else { LATE_UPSERT }] += 1;
            }
            SYcsbWriteOp::HotUpdate => {
                let max_key_now = current_max_key.load(Relaxed);
                let key = hot_sampler.sample(max_key_now);
                mdbx_hot_update(&db, &cfg, key);
                totals[HOT_UPDATE] += 1;
            }
        }

        let idx = (start.elapsed().as_secs() as usize).min(ops_per_sec.len() - 1);
        ops_per_sec[idx] += 1;
    }

    WriteWorkerStats {
        ops_per_sec,
        totals,
    }
}

struct OlapWorkerStats {
    scanned_tuples: u64,
    scans_completed: u64,
    scan_latencies_ns: Vec<u64>,
    staleness_txns: Vec<u64>,
}

fn olap_worker_thread(
    db: Arc<Database<WriteMap>>,
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
    let mut staleness_txns = Vec::new();

    while !stop.load(Relaxed) {
        let (lo, len) = olap_scan_bounds(current_max_key.load(Relaxed), olap_lag, olap_span);
        let scan_start = Instant::now();
        let (rows, txn_id_at_start) = mdbx_scan(&db, lo, len, read_payload);
        scanned_tuples += rows as u64;
        scan_latencies_ns.push(scan_start.elapsed().as_nanos() as u64);
        let txn_id_now = db
            .begin_ro_txn()
            .expect("mdbx_s_ycsb: begin_ro_txn (staleness probe)")
            .id();
        staleness_txns.push(txn_id_now.saturating_sub(txn_id_at_start));
        scans_completed += 1;
    }

    OlapWorkerStats {
        scanned_tuples,
        scans_completed,
        scan_latencies_ns,
        staleness_txns,
    }
}

pub fn run_mdbx_s_ycsb(cfg: MdbxSYcsbConfig) -> MdbxSYcsbRunSummary {
    assert!(
        cfg.ycsb.record_count >= 1,
        "mdbx_s_ycsb: record_count must be >= 1"
    );
    assert!(cfg.hot_window >= 1, "mdbx_s_ycsb: hot_window must be >= 1");
    assert!(cfg.olap_span >= 1, "mdbx_s_ycsb: olap_span must be >= 1");

    fs::create_dir_all(&cfg.output_dir).unwrap_or_else(|e| {
        panic!(
            "mdbx_s_ycsb: failed to create output_dir {}: {e}",
            cfg.output_dir.display()
        )
    });
    let mem_sampler = MemSampler::start(
        cfg.output_dir.join("mem_stats.csv"),
        DEFAULT_SAMPLE_INTERVAL,
    );

    let num_write_threads = cfg.num_write_threads.max(1);
    let num_olap_threads = cfg.num_olap_threads.max(1);
    let db = Arc::new(open_db(&cfg.db_path, num_write_threads + num_olap_threads));

    println!(
        "libmdbx S-YCSB benchmark\n\
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
         - db_path             = {}",
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
        cfg.db_path.display(),
    );

    println!("Loading {} cold records...", cfg.ycsb.record_count);
    let load_start = Instant::now();
    populate(&db, &cfg.ycsb);
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
    let read_payload = cfg.read_payload;
    let max_lateness = cfg.max_lateness;
    let olap_lag = cfg.olap_lag;
    let olap_span = cfg.olap_span;

    let write_handles: Vec<_> = (0..num_write_threads)
        .map(|_| {
            let db = db.clone();
            let cfg = ycsb_cfg;
            let hot_sampler = hot_sampler.clone();
            let current_max_key = current_max_key.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                write_worker_thread(
                    db,
                    cfg,
                    mix,
                    hot_sampler,
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
            let db = db.clone();
            let current_max_key = current_max_key.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                olap_worker_thread(
                    db,
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

    barrier.wait();
    let run_start = Instant::now();
    println!("Loading done. Running timed phase for {duration:?}...");
    thread::sleep(duration);
    stop.store(true, Relaxed);

    let write_stats: Vec<WriteWorkerStats> = write_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    let olap_stats: Vec<OlapWorkerStats> = olap_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    let actual_wall = run_start.elapsed();

    mem_sampler.stop();

    write_results(
        &write_stats,
        &olap_stats,
        duration,
        actual_wall,
        &cfg.output_dir,
    )
}

fn write_results(
    write_stats: &[WriteWorkerStats],
    olap_stats: &[OlapWorkerStats],
    requested_duration: Duration,
    actual_wall: Duration,
    out_dir: &std::path::Path,
) -> MdbxSYcsbRunSummary {
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
    let mut staleness_txns: Vec<u64> = Vec::new();
    for s in olap_stats {
        scanned_tuples += s.scanned_tuples;
        scans_completed += s.scans_completed;
        scan_latencies_ns.extend_from_slice(&s.scan_latencies_ns);
        staleness_txns.extend_from_slice(&s.staleness_txns);
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

    // Same format as BatStore's own s_ycsb_driver.rs::write_results - see
    // that function's comment.
    let pct = |samples: &[u64], p: f64| -> f64 {
        if samples.is_empty() {
            0.0
        } else {
            let idx = ((p * (samples.len() - 1) as f64).round() as usize).min(samples.len() - 1);
            samples[idx] as f64 / 1000.0
        }
    };

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

    staleness_txns.sort_unstable();
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
    let avg_staleness = if staleness_txns.is_empty() {
        0.0
    } else {
        staleness_txns.iter().sum::<u64>() as f64 / staleness_txns.len() as f64
    };
    staleness_file
        .write_all(
            format!(
                "{:.3},{:.3},{:.3},{},{:.3}\n",
                pct(&staleness_txns, 0.50),
                pct(&staleness_txns, 0.95),
                pct(&staleness_txns, 0.99),
                staleness_txns.len(),
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
    println!(
        "{:<20} {:.2}",
        "write throughput (ops/sec)", write_throughput
    );
    println!("{:<20} {}", "olap_scans", scans_completed);
    println!("{:<20} {}", "olap_scanned_tuples", scanned_tuples);
    println!(
        "Wrote {}, {} and {}",
        ts_path.display(),
        scan_latency_path.display(),
        staleness_path.display()
    );

    MdbxSYcsbRunSummary {
        write_throughput_ops_sec: write_throughput,
        totals,
        olap_scans_completed: scans_completed,
    }
}

pub fn main_mdbx_s_ycsb(parms: Vec<String>) {
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
    let read_payload: bool = arg(&parms, 14, true);
    let db_path: String = parms
        .get(15)
        .cloned()
        .unwrap_or_else(|| "mdbx_s_ycsb_db".to_string());

    run_mdbx_s_ycsb(MdbxSYcsbConfig {
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
        read_payload,
        db_path: PathBuf::from(db_path),
        output_dir: PathBuf::from("."),
    });
}
