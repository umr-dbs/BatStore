//! Throughput/latency comparison: the existing `WalWriter` (every worker
//! thread hands its record to one channel; one dedicated background thread
//! coalesces whatever piled up into a single `write_all` + `sync_data` per
//! group-commit window) versus `LockFreeWalWriter` (every worker thread
//! reserves its own byte range via `fetch_add` and calls `pwrite` itself,
//! no channel, no dedicated writer thread — see `bat_wal::lockfree_writer`'s
//! doc for the full design and its correctness trade-off).
//!
//! This remains small enough to run in the normal suite (roughly two
//! seconds on the development machine) while still exercising real files.

use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_record_model::tx_stamp::TxStamp;
use crate::bat_sync::clock::GlobalClock;
use crate::bat_wal::lockfree_writer::{LocalBatch, LockFreeWalWriter};
use crate::bat_wal::writer::WalWriter;

const OPS_PER_THREAD: usize = 20_000;
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8, 16];
/// Local-batch sizes to sweep for `LockFreeWalWriter` — `1` is the
/// unbatched baseline (one `pwrite` per record, same as `enqueue` used
/// directly), included so the batched code path's own overhead is visible
/// against the plain one.
const BATCH_SIZES: &[usize] = &[1, 4, 16, 64, 256];
/// Matches `bat_wal::writer::GROUP_COMMIT_LINGER`/typical bench configs —
/// same fsync cadence for both writers so the comparison isolates the
/// append-path design, not a difference in how often either one fsyncs.
const FLUSH_INTERVAL: Duration = Duration::from_micros(200);

struct RunStats {
    threads: usize,
    total_ops: usize,
    wall: Duration,
    /// Per-call latency of the enqueue call itself (channel send for
    /// `WalWriter`, `fetch_add` + `pwrite` for `LockFreeWalWriter`) —
    /// what each worker thread actually pays before it can move on to its
    /// next unit of work. Collected from every op, sorted, for percentiles.
    latencies_ns: Vec<u64>,
}

impl RunStats {
    fn report(&self, label: &str) {
        let mut lat = self.latencies_ns.clone();
        lat.sort_unstable();
        let p50 = lat[lat.len() / 2];
        let p99 = lat[lat.len() * 99 / 100];
        let p999 = lat[lat.len() * 999 / 1000];
        let ops_per_sec = self.total_ops as f64 / self.wall.as_secs_f64();
        println!(
            "{label:<12} threads={:<3} total_ops={:<8} wall={:>8.3}ms  throughput={:>10.0} ops/s  \
             enqueue latency p50={:>6}ns p99={:>7}ns p99.9={:>8}ns",
            self.threads,
            self.total_ops,
            self.wall.as_secs_f64() * 1000.0,
            ops_per_sec,
            p50,
            p99,
            p999,
        );
    }
}

fn bench_wal_writer(threads: usize, path: &std::path::Path) -> RunStats {
    let _ = fs::remove_file(path);
    let writer: Arc<WalWriter<u64, u64>> = Arc::new(WalWriter::open(path, FLUSH_INTERVAL).unwrap());
    let clock = Arc::new(GlobalClock::new());

    let wall_start = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let writer = writer.clone();
            let clock = clock.clone();
            thread::spawn(move || {
                let mut latencies = Vec::with_capacity(OPS_PER_THREAD);
                for i in 0..OPS_PER_THREAD {
                    let key = (t * OPS_PER_THREAD + i) as u64;
                    let start = Instant::now();
                    let _stamp = writer.start_commit_logged(&clock, t as u16, move |_v| {
                        CRUDOperation::Insert(key, key)
                    });
                    latencies.push(start.elapsed().as_nanos() as u64);
                }
                latencies
            })
        })
        .collect();

    let mut latencies_ns = Vec::with_capacity(threads * OPS_PER_THREAD);
    for h in handles {
        latencies_ns.extend(h.join().unwrap());
    }
    let issue_done = wall_start.elapsed();

    // Drop drains+flushes+joins the background thread - the point at which
    // every issued record is actually durable on disk.
    drop(writer);
    let wall = wall_start.elapsed();
    let _ = issue_done; // kept for future drill-down; wall (incl. drain) is what's reported

    RunStats {
        threads,
        total_ops: threads * OPS_PER_THREAD,
        wall,
        latencies_ns,
    }
}

fn bench_lockfree_writer(threads: usize, path: &std::path::Path) -> RunStats {
    let _ = fs::remove_file(path);
    let writer: Arc<LockFreeWalWriter<u64, u64>> =
        Arc::new(LockFreeWalWriter::open(path, FLUSH_INTERVAL).unwrap());
    let clock = Arc::new(GlobalClock::new());
    let max_ts_seen = Arc::new(AtomicU64::new(0));

    let wall_start = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let writer = writer.clone();
            let clock = clock.clone();
            let max_ts_seen = max_ts_seen.clone();
            thread::spawn(move || {
                let mut latencies = Vec::with_capacity(OPS_PER_THREAD);
                for i in 0..OPS_PER_THREAD {
                    let key = (t * OPS_PER_THREAD + i) as u64;
                    let start = Instant::now();
                    let stamp = writer.start_commit_logged(&clock, t as u16, move |_v| {
                        CRUDOperation::Insert(key, key)
                    });
                    latencies.push(start.elapsed().as_nanos() as u64);
                    max_ts_seen.fetch_max(stamp.ts_start(), Ordering::Relaxed);
                }
                latencies
            })
        })
        .collect();

    let mut latencies_ns = Vec::with_capacity(threads * OPS_PER_THREAD);
    for h in handles {
        latencies_ns.extend(h.join().unwrap());
    }
    let issue_done = wall_start.elapsed();

    drop(writer);
    let wall = wall_start.elapsed();
    let _ = issue_done;

    RunStats {
        threads,
        total_ops: threads * OPS_PER_THREAD,
        wall,
        latencies_ns,
    }
}

/// Same shape as `bench_lockfree_writer`, but each thread groups its own
/// records into a `LocalBatch` of `batch_size` and flushes with one
/// `pwrite` per group instead of one per record (see
/// `bat_wal::lockfree_writer::LocalBatch`'s doc). Per-op latency is measured
/// around `push_write` + the conditional `flush_batch` — most ops just hit
/// the cheap local accumulate; every `batch_size`-th one also pays that
/// group's `pwrite`, so the latency distribution below has a "usually
/// cheap, occasionally pays for the whole group" shape by construction.
fn bench_lockfree_batched_writer(
    threads: usize,
    batch_size: usize,
    path: &std::path::Path,
) -> RunStats {
    let _ = fs::remove_file(path);
    let writer: Arc<LockFreeWalWriter<u64, u64>> =
        Arc::new(LockFreeWalWriter::open(path, FLUSH_INTERVAL).unwrap());
    let clock = Arc::new(GlobalClock::new());

    let wall_start = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let writer = writer.clone();
            let clock = clock.clone();
            thread::spawn(move || {
                let mut batch: LocalBatch<u64, u64> = LocalBatch::new();
                let mut latencies = Vec::with_capacity(OPS_PER_THREAD);
                for i in 0..OPS_PER_THREAD {
                    let key = (t * OPS_PER_THREAD + i) as u64;
                    let stamp = TxStamp::new(t as u16, clock.next_timestamp());
                    let start = Instant::now();
                    batch.push_write(stamp, move |_v| CRUDOperation::Insert(key, key));
                    if batch.len() >= batch_size {
                        writer.flush_batch(&mut batch);
                    }
                    latencies.push(start.elapsed().as_nanos() as u64);
                }
                writer.flush_batch(&mut batch); // flush this thread's remainder
                latencies
            })
        })
        .collect();

    let mut latencies_ns = Vec::with_capacity(threads * OPS_PER_THREAD);
    for h in handles {
        latencies_ns.extend(h.join().unwrap());
    }

    drop(writer);
    let wall = wall_start.elapsed();

    RunStats {
        threads,
        total_ops: threads * OPS_PER_THREAD,
        wall,
        latencies_ns,
    }
}

#[test]
fn compare_wal_writer_vs_lockfree_throughput() {
    let dir = std::env::temp_dir();
    let old_path = dir.join(format!("batstore_bench_wal_old_{}.log", std::process::id()));
    let new_path = dir.join(format!("batstore_bench_wal_new_{}.log", std::process::id()));

    println!();
    println!("=== WalWriter (channel + single background writer thread) ===");
    for &threads in THREAD_COUNTS {
        let stats = bench_wal_writer(threads, &old_path);
        stats.report("WalWriter");
    }

    println!();
    println!("=== LockFreeWalWriter (fetch_add tail + pwrite, no background writer) ===");
    for &threads in THREAD_COUNTS {
        let stats = bench_lockfree_writer(threads, &new_path);
        stats.report("LockFree");
    }

    println!();
    println!("=== LockFreeWalWriter + per-thread LocalBatch (group commit per thread) ===");
    for &batch_size in BATCH_SIZES {
        for &threads in THREAD_COUNTS {
            let stats = bench_lockfree_batched_writer(threads, batch_size, &new_path);
            stats.report(&format!("Batch={batch_size}"));
        }
    }

    let _ = fs::remove_file(&old_path);
    let _ = fs::remove_file(&new_path);
}
