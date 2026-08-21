//! YCSB counterpart to `tpcc_wal_backend_bench.rs` — see that file's doc for
//! why this drives the real benchmark end to end rather than a synthetic
//! WAL-only microbenchmark.
//!
//! The normal test uses a 20,000-record, one-second smoke configuration.
//! Set `BATSTORE_FULL_BENCH=1` to restore the original 200,000-record,
//! eight-second, 8/16-thread comparison.

use std::path::PathBuf;
use std::time::Duration;

use crate::bat_bench::ycsb_driver::{DriverConfig, run_ycsb};
use crate::bat_bench::ycsb_random::{RequestDistribution, YcsbMix};
use crate::bat_bench::ycsb_schema::YcsbConfig;
use crate::bat_root::index_root::RootIndexType;

const FULL_RECORD_COUNT: u64 = 200_000;
const FULL_DURATION_SECS: u64 = 8;
const FULL_THREAD_COUNTS: &[usize] = &[8, 16];
/// See `tpcc_wal_backend_bench.rs`'s `BACKENDS` doc.
const BACKENDS: &[(&str, Option<usize>)] = &[
    ("batched", None),
    ("lockfree-batch16", Some(16)),
    ("lockfree-batch64", Some(64)),
];

fn full_scale() -> bool {
    std::env::var_os("BATSTORE_FULL_BENCH").is_some()
}

fn config(
    num_threads: usize,
    wal_path: PathBuf,
    batch_size: Option<usize>,
    full: bool,
) -> DriverConfig {
    DriverConfig {
        ycsb: YcsbConfig {
            record_count: if full { FULL_RECORD_COUNT } else { 20_000 },
            field_count: 10,
            field_length: 100,
        },
        num_threads,
        duration: Duration::from_secs(if full { FULL_DURATION_SECS } else { 1 }),
        // Workload A (50% read / 50% update): the update half is what
        // actually exercises the WAL write path - a read-only mix
        // (workload C) would never touch it at all.
        mix: YcsbMix::workload("a").expect("workload 'a' must exist"),
        distribution: RequestDistribution::Zipfian { theta: 0.99 },
        max_scan_length: 100,
        write_all_fields: false,
        read_payload: true,
        execution_mode: crate::bat_bench::ycsb_txn::YcsbExecutionMode::Atomic,
        gc: true,
        update_in_place: false,
        root_star_index: RootIndexType::FrugalList,
        wal: Some((wal_path, Duration::from_millis(5))),
        wal_lockfree_batch_size: batch_size,
        output_dir: std::env::temp_dir().join("batstore_ycsb_wal_bench_out"),
        // Workload A never scans, so this would auto-decide to `None` anyway.
        scan_pool_workers: None,
    }
}

#[test]
fn compare_wal_backends_ycsb() {
    let full = full_scale();
    let record_count = if full { FULL_RECORD_COUNT } else { 20_000 };
    let duration = if full { FULL_DURATION_SECS } else { 1 };
    let thread_counts: &[usize] = if full { FULL_THREAD_COUNTS } else { &[2] };
    let dir = std::env::temp_dir();
    println!();
    println!(
        "=== YCSB workload A: WAL backend comparison ({record_count} records, {duration}s/run) ==="
    );
    for &threads in thread_counts {
        for &(label, batch_size) in BACKENDS {
            let wal_path = dir.join(format!(
                "batstore_ycsb_wal_bench_{label}_{threads}_{}.log",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&wal_path);

            let summary = run_ycsb(config(threads, wal_path.clone(), batch_size, full));

            println!(
                "YCSB  backend={label:<18} threads={threads:<3} ops/sec={:>10.1}",
                summary.throughput_ops_sec
            );

            let _ = std::fs::remove_file(&wal_path);
        }
    }
}
