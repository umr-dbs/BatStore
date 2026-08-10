//! YCSB counterpart to `tpcc_wal_backend_bench.rs` — see that file's doc for
//! why this drives the real benchmark end to end rather than a synthetic
//! WAL-only microbenchmark.
//!
//! `#[ignore]`d — see `tpcc_wal_backend_bench.rs`. Run with:
//! `cargo test --bin cMVBT compare_wal_backends_ycsb --release -- --ignored --nocapture --test-threads=1`

use std::path::PathBuf;
use std::time::Duration;

use crate::mv_bench::ycsb_random::{RequestDistribution, YcsbMix};
use crate::mv_bench::ycsb_driver::{run_ycsb, DriverConfig};
use crate::mv_bench::ycsb_schema::YcsbConfig;
use crate::mv_root::index_root::RootIndexType;

const RECORD_COUNT: u64 = 200_000;
const DURATION_SECS: u64 = 8;
const THREAD_COUNTS: &[usize] = &[8, 16];
/// See `tpcc_wal_backend_bench.rs`'s `BACKENDS` doc.
const BACKENDS: &[(&str, Option<usize>)] = &[
    ("batched", None),
    ("lockfree-batch16", Some(16)),
    ("lockfree-batch64", Some(64)),
];

fn config(num_threads: usize, wal_path: PathBuf, batch_size: Option<usize>) -> DriverConfig {
    DriverConfig {
        ycsb: YcsbConfig { record_count: RECORD_COUNT, field_count: 10, field_length: 100 },
        num_threads,
        duration: Duration::from_secs(DURATION_SECS),
        // Workload A (50% read / 50% update): the update half is what
        // actually exercises the WAL write path - a read-only mix
        // (workload C) would never touch it at all.
        mix: YcsbMix::workload("a").expect("workload 'a' must exist"),
        distribution: RequestDistribution::Zipfian { theta: 0.99 },
        max_scan_length: 100,
        gc: true,
        update_in_place: false,
        root_star_index: RootIndexType::FrugalList,
        wal: Some((wal_path, Duration::from_millis(5))),
        wal_lockfree_batch_size: batch_size,
        output_dir: std::env::temp_dir().join("cmvbt_ycsb_wal_bench_out"),
    }
}

#[test]
#[ignore]
fn compare_wal_backends_ycsb() {
    let dir = std::env::temp_dir();
    println!();
    println!("=== YCSB workload A: WAL backend comparison ({RECORD_COUNT} records, {DURATION_SECS}s/run) ===");
    for &threads in THREAD_COUNTS {
        for &(label, batch_size) in BACKENDS {
            let wal_path = dir.join(format!("cmvbt_ycsb_wal_bench_{label}_{threads}_{}.log", std::process::id()));
            let _ = std::fs::remove_file(&wal_path);

            let summary = run_ycsb(config(threads, wal_path.clone(), batch_size));

            println!("YCSB  backend={label:<18} threads={threads:<3} ops/sec={:>10.1}", summary.throughput_ops_sec);

            let _ = std::fs::remove_file(&wal_path);
        }
    }
}
