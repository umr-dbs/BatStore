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
//! this codebase between BatStore's own tree and its transaction/business logic
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
//! Durability: `SyncMode::UtterlyNoSync` - no fsync/flush at all, matching
//! the in-memory-only guarantee already forced on every other engine here
//! (see `common.fresh_scratch_dir`, tmpfs-backed). `SyncMode::SafeNoSync`
//! was tried first, but per libmdbx's own docs it issues exactly the same
//! number/volume of disk IOPs as full `Durable` sync - it only removes the
//! *correctness* risk of those flushes, not the flushes themselves - so it
//! wasn't actually cheaper. `WriteMap` is used for the same reason: writes
//! go directly into the mmap'd region instead of through a `write()`
//! syscall into the page cache.

use std::borrow::Cow;
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
use crate::bat_bench::ycsb_random::{
    KeySampler, RequestDistribution, YcsbMix, YcsbOpType, pick_op, random_row, random_scan_length,
};
use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbKey};
use crate::bat_wal::record::WalPayload;

pub struct MdbxYcsbConfig {
    pub ycsb: YcsbConfig,
    pub num_threads: usize,
    pub duration: Duration,
    pub mix: YcsbMix,
    pub distribution: RequestDistribution,
    pub max_scan_length: u64,
    pub read_payload: bool,
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
// Keep per-second accounting from becoming part of the point-read benchmark. This
// matches ycsb_driver.rs: one clock read per 256 operations, rather than one per op.
const TIMESERIES_CLOCK_EVERY: u64 = 256;
// Storing/timing every operation is intrusive for an in-memory workload. Keep the same
// systematic sampling rate as BatStore's YCSB driver.
const OPERATION_LATENCY_SAMPLE_EVERY: u64 = 1024;
const COUNTER_NAMES: [&str; NUM_COUNTERS] =
    ["read", "update", "insert", "scan", "read_modify_write"];

fn open_db(path: &std::path::Path, num_threads: usize) -> Database<WriteMap> {
    fs::create_dir_all(path)
        .unwrap_or_else(|e| panic!("mdbx_ycsb: failed to create db dir {}: {e}", path.display()));
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
            "mdbx_ycsb: failed to open database at {}: {e}",
            path.display()
        )
    });
    // The unnamed/default table needs one RW transaction to exist before any reader can
    // open it.
    let txn = db
        .begin_rw_txn()
        .expect("mdbx_ycsb: begin_rw_txn (table creation)");
    txn.create_table(None, TableFlags::empty())
        .expect("mdbx_ycsb: create_table");
    txn.commit().expect("mdbx_ycsb: commit (table creation)");
    db
}

fn encode_row(cfg: &YcsbConfig) -> Vec<u8> {
    let row = random_row(cfg);
    let mut buf = Vec::new();
    row.wal_encode(&mut buf);
    buf
}

fn mdbx_read(db: &Database<WriteMap>, key: YcsbKey, read_payload: bool) -> bool {
    let txn = db.begin_ro_txn().expect("mdbx_ycsb: begin_ro_txn");
    let table = txn.open_table(None).expect("mdbx_ycsb: open_table");
    if read_payload {
        // A clean RO value lives in libmdbx's mmap and can be borrowed for the lifetime
        // of this transaction. Vec<u8> would allocate and copy the whole (normally 1 KiB)
        // YCSB row on every read, obscuring the storage engine's actual lookup cost.
        let value = txn
            .get::<Cow<'_, [u8]>>(&table, &key.to_be_bytes())
            .expect("mdbx_ycsb: get");
        if let Some(bytes) = value.as_deref() {
            std::hint::black_box(bytes.iter().fold(0u8, |a, b| a.wrapping_add(*b)));
        }
        value.is_some()
    } else {
        // Existence-only mode must not decode, allocate, or copy the value.
        txn.get::<()>(&table, &key.to_be_bytes())
            .expect("mdbx_ycsb: get")
            .is_some()
    }
}

fn mdbx_update(db: &Database<WriteMap>, cfg: &YcsbConfig, key: YcsbKey) -> bool {
    let txn = db.begin_rw_txn().expect("mdbx_ycsb: begin_rw_txn");
    let table = txn.open_table(None).expect("mdbx_ycsb: open_table");
    let exists = txn
        .get::<()>(&table, &key.to_be_bytes())
        .expect("mdbx_ycsb: get")
        .is_some();
    if exists {
        let buf = encode_row(cfg);
        txn.put(&table, key.to_be_bytes(), &buf, WriteFlags::UPSERT)
            .expect("mdbx_ycsb: put");
    }
    txn.commit().expect("mdbx_ycsb: commit");
    exists
}

fn mdbx_insert(db: &Database<WriteMap>, cfg: &YcsbConfig, key: YcsbKey) {
    let txn = db.begin_rw_txn().expect("mdbx_ycsb: begin_rw_txn");
    let table = txn.open_table(None).expect("mdbx_ycsb: open_table");
    let buf = encode_row(cfg);
    txn.put(&table, key.to_be_bytes(), &buf, WriteFlags::UPSERT)
        .expect("mdbx_ycsb: put");
    txn.commit().expect("mdbx_ycsb: commit");
}

/// Returns the number of rows actually scanned (can be `< len` near the end of the loaded
/// key range) - same contract as `ycsb_txn::scan`.
fn mdbx_scan(db: &Database<WriteMap>, start_key: YcsbKey, len: u64, read_payload: bool) -> usize {
    let txn = db.begin_ro_txn().expect("mdbx_ycsb: begin_ro_txn");
    let table = txn.open_table(None).expect("mdbx_ycsb: open_table");
    let mut cursor = txn.cursor(&table).expect("mdbx_ycsb: cursor");
    let mut count = 0u64;
    // Borrow values from the mmap in both modes. In key-only mode the returned Cow is
    // never inspected, so no payload bytes are copied or consumed.
    let mut item = cursor
        .set_range::<(), Cow<'_, [u8]>>(&start_key.to_be_bytes())
        .expect("mdbx_ycsb: cursor.set_range");
    while item.is_some() && count < len {
        if read_payload {
            if let Some((_, bytes)) = item.as_ref() {
                std::hint::black_box(bytes.iter().fold(0u8, |a, b| a.wrapping_add(*b)));
            }
        }
        count += 1;
        if count >= len {
            break;
        }
        item = cursor
            .next::<(), Cow<'_, [u8]>>()
            .expect("mdbx_ycsb: cursor.next");
    }
    count as usize
}

fn mdbx_read_modify_write(
    db: &Database<WriteMap>,
    cfg: &YcsbConfig,
    key: YcsbKey,
    read_payload: bool,
) -> bool {
    let _ = mdbx_read(db, key, read_payload);
    mdbx_update(db, cfg, key)
}

/// Records per load transaction. Unlike the timed phase's ops (each deliberately its own
/// transaction, to match the workload's per-op semantics), population isn't part of the
/// measured throughput - it was previously committing all 2M+ default records one at a
/// time, paying libmdbx's single-writer-lock/commit overhead 2M times over before the
/// timed phase even started. Batching amortizes that setup cost without touching how the
/// timed phase itself measures ops.
const LOAD_BATCH_SIZE: u64 = 10_000;

fn populate(db: &Database<WriteMap>, cfg: &YcsbConfig) {
    let mut key = 1u64;
    while key <= cfg.record_count {
        let batch_end = (key + LOAD_BATCH_SIZE - 1).min(cfg.record_count);
        let txn = db.begin_rw_txn().expect("mdbx_ycsb: begin_rw_txn (load)");
        let table = txn.open_table(None).expect("mdbx_ycsb: open_table (load)");
        for k in key..=batch_end {
            let buf = encode_row(cfg);
            txn.put(&table, k.to_be_bytes(), &buf, WriteFlags::UPSERT)
                .expect("mdbx_ycsb: put (load)");
        }
        txn.commit().expect("mdbx_ycsb: commit (load)");
        key = batch_end + 1;
    }
}

struct WorkerStats {
    ops_per_sec: Vec<u64>,
    totals: [u64; NUM_COUNTERS],
    scanned_tuples: u64,
    operation_latencies_ns: [Vec<u64>; NUM_COUNTERS],
}

#[allow(clippy::too_many_arguments)]
fn worker_thread(
    db: Arc<Database<WriteMap>>,
    cfg: YcsbConfig,
    mix: YcsbMix,
    sampler: Arc<KeySampler>,
    max_scan_length: u64,
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
                mdbx_read(&db, key, read_payload);
                if let Some(op_start) = op_start {
                    operation_latencies_ns[READ].push(op_start.elapsed().as_nanos() as u64);
                }
                totals[READ] += 1;
            }
            YcsbOpType::Update => {
                let key = sampler.sample(record_count, max_key_now);
                let op_start =
                    (totals[UPDATE] % OPERATION_LATENCY_SAMPLE_EVERY == 0).then(Instant::now);
                mdbx_update(&db, &cfg, key);
                if let Some(op_start) = op_start {
                    operation_latencies_ns[UPDATE].push(op_start.elapsed().as_nanos() as u64);
                }
                totals[UPDATE] += 1;
            }
            YcsbOpType::Insert => {
                let key = current_max_key.fetch_add(1, Relaxed) + 1;
                let op_start =
                    (totals[INSERT] % OPERATION_LATENCY_SAMPLE_EVERY == 0).then(Instant::now);
                mdbx_insert(&db, &cfg, key);
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
                    scanned_tuples += mdbx_scan(&db, key, len, read_payload) as u64;
                    operation_latencies_ns[SCAN].push(scan_start.elapsed().as_nanos() as u64);
                } else {
                    scanned_tuples += mdbx_scan(&db, key, len, read_payload) as u64;
                }
                totals[SCAN] += 1;
            }
            YcsbOpType::ReadModifyWrite => {
                let key = sampler.sample(record_count, max_key_now);
                let op_start =
                    (totals[RMW] % OPERATION_LATENCY_SAMPLE_EVERY == 0).then(Instant::now);
                mdbx_read_modify_write(&db, &cfg, key, read_payload);
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

pub fn run_mdbx_ycsb(cfg: MdbxYcsbConfig) -> MdbxYcsbRunSummary {
    assert!(
        cfg.ycsb.record_count >= 1,
        "mdbx_ycsb: record_count must be >= 1"
    );

    fs::create_dir_all(&cfg.output_dir).unwrap_or_else(|e| {
        panic!(
            "mdbx_ycsb: failed to create output_dir {}: {e}",
            cfg.output_dir.display()
        )
    });
    let mem_sampler = MemSampler::start(
        cfg.output_dir.join("mem_stats.csv"),
        DEFAULT_SAMPLE_INTERVAL,
    );

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
        cfg.ycsb.record_count,
        cfg.ycsb.field_count,
        cfg.ycsb.field_length,
        cfg.duration,
        cfg.mix,
        cfg.distribution,
        cfg.max_scan_length,
        cfg.db_path.display(),
    );

    println!("Loading {} records...", cfg.ycsb.record_count);
    let load_start = Instant::now();
    populate(&db, &cfg.ycsb);
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
    let read_payload = cfg.read_payload;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = db.clone();
            let cfg = ycsb_cfg;
            let sampler = sampler.clone();
            let current_max_key = current_max_key.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                worker_thread(
                    db,
                    cfg,
                    mix,
                    sampler,
                    max_scan_length,
                    read_payload,
                    current_max_key,
                    duration,
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

    let stats: Vec<WorkerStats> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let actual_wall = run_start.elapsed();

    mem_sampler.stop();

    write_results(&stats, duration, actual_wall, &cfg.output_dir)
}

fn write_results(
    stats: &[WorkerStats],
    requested_duration: Duration,
    actual_wall: Duration,
    out_dir: &std::path::Path,
) -> MdbxYcsbRunSummary {
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
        let avg = if samples.is_empty() {
            0.0
        } else {
            samples.iter().map(|&v| v as u128).sum::<u128>() as f64 / samples.len() as f64 / 1000.0
        };
        (pct(0.50), pct(0.95), pct(0.99), avg)
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

    // Backward-compatible scan-only summary for the existing manifest and plots.
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
    println!(
        "Wrote {}, {} and {}",
        ts_path.display(),
        operation_latency_path.display(),
        scan_latency_path.display()
    );

    MdbxYcsbRunSummary {
        throughput_ops_sec: throughput,
        totals,
    }
}

pub fn main_mdbx_ycsb(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms
            .get(idx)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }

    let workload: String = parms.get(2).cloned().unwrap_or_else(|| "a".to_string());
    let mix = YcsbMix::workload(&workload).unwrap_or_else(|| {
        panic!("mdbx_ycsb: unknown workload '{workload}' (expected one of a, b, c, d, e, f)")
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
    let db_path: String = parms
        .get(11)
        .cloned()
        .unwrap_or_else(|| "mdbx_ycsb_db".to_string());
    let read_payload: bool = arg(&parms, 12, true);

    run_mdbx_ycsb(MdbxYcsbConfig {
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
        read_payload,
        db_path: PathBuf::from(db_path),
        output_dir: PathBuf::from("."),
    });
}
