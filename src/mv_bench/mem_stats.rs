//! Periodic memory-usage sampler used by the benchmark drivers/suite to log
//! how much memory a run actually uses over time — the piece that lets a
//! `benchmark_results/.../mem_stats.csv` show GC reclaiming memory over a
//! run vs. unbounded growth with GC off (see `mv_bench::suite`).
//!
//! Samples two independent sources every tick:
//! - `VmRSS` from `/proc/self/status` — the OS's own view of resident memory,
//!   Linux-only (this whole crate already only builds on Linux: `jemallocator`
//!   is a Linux-only target dependency and `main.rs` unconditionally installs
//!   it as `#[global_allocator]`, so no portability fallback is needed here).
//! - jemalloc's own `stats.allocated`/`active`/`resident`/`mapped` (via the
//!   `jemalloc_ctl` crate, sharing the same underlying jemalloc build as the
//!   `jemallocator` global allocator) — useful because it separates
//!   "logically allocated" from "resident" from "mapped-but-untouched",
//!   which `VmRSS` alone can't distinguish.
//!
//! The sampler thread never calls into any `MVBTSt`/`Database` tree, so it
//! does not consume a `WorkerId` slot and is exempt from the
//! `default_max_workers()` ceiling every terminal/OLAP thread is clamped
//! against elsewhere in `mv_bench`.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Default sampling period used by every benchmark driver — not exposed as a
/// CLI positional arg (see module docs on why); callers that need a
/// different cadence can add one directly via `MemSampler::start`.
pub const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_millis(500);

fn read_vm_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.trim().split_whitespace().next()?.parse().ok();
        }
    }
    None
}

struct JemallocStats {
    allocated: u64,
    active: u64,
    resident: u64,
    mapped: u64,
}

fn read_jemalloc_stats() -> Option<JemallocStats> {
    jemalloc_ctl::epoch::advance().ok()?;
    Some(JemallocStats {
        allocated: jemalloc_ctl::stats::allocated::read().ok()? as u64,
        active: jemalloc_ctl::stats::active::read().ok()? as u64,
        resident: jemalloc_ctl::stats::resident::read().ok()? as u64,
        mapped: jemalloc_ctl::stats::mapped::read().ok()? as u64,
    })
}

/// Background sampler: append one CSV row per tick to `csv_path` until
/// `stop()` is called. Header: `elapsed_sec,rss_kb,jemalloc_allocated_bytes,
/// jemalloc_active_bytes,jemalloc_resident_bytes,jemalloc_mapped_bytes`.
pub struct MemSampler {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl MemSampler {
    pub fn start(csv_path: PathBuf, interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();

        let handle = thread::spawn(move || {
            let _ = std::fs::remove_file(&csv_path);
            let mut file = OpenOptions::new().create(true).append(true).open(&csv_path)
                .unwrap_or_else(|e| panic!("mem_stats: failed to open {}: {e}", csv_path.display()));
            file.write_all(b"elapsed_sec,rss_kb,jemalloc_allocated_bytes,jemalloc_active_bytes,jemalloc_resident_bytes,jemalloc_mapped_bytes\n").unwrap();

            let start = Instant::now();
            while !thread_stop.load(Relaxed) {
                let elapsed = start.elapsed().as_secs_f64();
                let rss_kb = read_vm_rss_kb().unwrap_or(0);
                let j = read_jemalloc_stats();

                file.write_all(format!(
                    "{elapsed:.3},{rss_kb},{},{},{},{}\n",
                    j.as_ref().map(|s| s.allocated).unwrap_or(0),
                    j.as_ref().map(|s| s.active).unwrap_or(0),
                    j.as_ref().map(|s| s.resident).unwrap_or(0),
                    j.as_ref().map(|s| s.mapped).unwrap_or(0),
                ).as_bytes()).unwrap();

                thread::sleep(interval);
            }

            // One final sample right at stop time, so short runs (or runs
            // that finish between two ticks) still capture their end-state
            // memory footprint, not just whatever the last full tick saw.
            let elapsed = start.elapsed().as_secs_f64();
            let rss_kb = read_vm_rss_kb().unwrap_or(0);
            let j = read_jemalloc_stats();
            file.write_all(format!(
                "{elapsed:.3},{rss_kb},{},{},{},{}\n",
                j.as_ref().map(|s| s.allocated).unwrap_or(0),
                j.as_ref().map(|s| s.active).unwrap_or(0),
                j.as_ref().map(|s| s.resident).unwrap_or(0),
                j.as_ref().map(|s| s.mapped).unwrap_or(0),
            ).as_bytes()).unwrap();
        });

        Self { stop, handle: Some(handle) }
    }

    /// Signals the sampler thread to take one last sample and exit, then
    /// joins it — so the CSV at `csv_path` is complete and closed by the
    /// time this returns.
    pub fn stop(mut self) {
        self.stop.store(true, Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Re-reads a `mem_stats.csv` written by [`MemSampler`] and summarizes it —
/// used by `mv_bench::suite` to fold peak/avg memory into `manifest.csv`
/// without threading memory stats through every driver's return type.
pub struct MemSummary {
    pub peak_rss_mb: f64,
    pub avg_rss_mb: f64,
    pub peak_jemalloc_resident_mb: f64,
}

pub fn summarize(csv_path: &Path) -> Option<MemSummary> {
    let content = std::fs::read_to_string(csv_path).ok()?;
    let mut rss_samples = Vec::new();
    let mut peak_resident_bytes: u64 = 0;

    for line in content.lines().skip(1) {
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 5 { continue; }
        if let Ok(rss_kb) = cols[1].parse::<u64>() {
            rss_samples.push(rss_kb as f64 / 1024.0);
        }
        if let Ok(resident) = cols[4].parse::<u64>() {
            peak_resident_bytes = peak_resident_bytes.max(resident);
        }
    }

    if rss_samples.is_empty() {
        return None;
    }

    let peak_rss_mb = rss_samples.iter().cloned().fold(0.0f64, f64::max);
    let avg_rss_mb = rss_samples.iter().sum::<f64>() / rss_samples.len() as f64;

    Some(MemSummary {
        peak_rss_mb,
        avg_rss_mb,
        peak_jemalloc_resident_mb: peak_resident_bytes as f64 / (1024.0 * 1024.0),
    })
}
