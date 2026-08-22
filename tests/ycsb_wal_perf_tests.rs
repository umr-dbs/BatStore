//! YCSB counterpart to `tpcc_wal_perf_tests.rs` — see that file's doc for
//! why this stays in the default `cargo test` run instead of being
//! `#[ignore]`d like `tests/ycsb_wal_backend_bench.rs`'s full-scale version.

use std::path::PathBuf;
use std::time::Duration;

use crate::bat_bench::mem_stats::read_vm_rss_kb;
use crate::bat_bench::ycsb_driver::{DriverConfig, run_ycsb};
use crate::bat_bench::ycsb_random::{RequestDistribution, YcsbMix};
use crate::bat_bench::ycsb_schema::YcsbConfig;
use crate::bat_root::index_root::RootIndexType;

const RECORD_COUNT: u64 = 2_000;
const DURATION: Duration = Duration::from_millis(300);
const THREAD_COUNTS: &[usize] = &[2, 4];
/// See `tests/tpcc_wal_backend_bench.rs`'s `BACKENDS` doc.
const BACKENDS: &[(&str, Option<usize>)] = &[
    ("batched", None),
    ("lockfree-batch16", Some(16)),
    ("lockfree-batch64", Some(64)),
];
/// 20 GB safety ceiling (this test's own ask) — see
/// `tpcc_wal_perf_tests.rs`'s `MAX_RSS_GROWTH_KB` doc.
const MAX_RSS_GROWTH_KB: u64 = 20_000_000;

fn config(num_threads: usize, wal_path: PathBuf, batch_size: Option<usize>) -> DriverConfig {
    DriverConfig {
        ycsb: YcsbConfig {
            record_count: RECORD_COUNT,
            field_count: 10,
            field_length: 100,
        },
        num_threads,
        duration: DURATION,
        // Workload A (50% read / 50% update): the update half is what
        // actually exercises the WAL write path.
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
        output_dir: std::env::temp_dir().join("batstore_ycsb_wal_perf_test_out"),
        // Workload A never scans, so this would auto-decide to `None` anyway.
        scan_pool_workers: None,
        idle_compaction: None,
    }
}

#[test]
fn wal_backend_perf_sweep_ycsb() {
    let dir = std::env::temp_dir();
    let rss_before = read_vm_rss_kb().unwrap_or(0);

    println!(
        "\n=== YCSB workload A: WAL backend perf sweep ({RECORD_COUNT} records, {DURATION:?}/run) ==="
    );
    for &threads in THREAD_COUNTS {
        for &(label, batch_size) in BACKENDS {
            let wal_path = dir.join(format!(
                "batstore_ycsb_wal_perf_{label}_{threads}_{}.log",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&wal_path);

            let summary = run_ycsb(config(threads, wal_path.clone(), batch_size));
            println!(
                "YCSB  backend={label:<18} threads={threads:<3} ops/sec={:>10.1}",
                summary.throughput_ops_sec
            );
            assert!(
                summary.totals.iter().sum::<u64>() > 0,
                "backend={label} threads={threads}: no ops completed at all"
            );

            let _ = std::fs::remove_file(&wal_path);
        }
    }

    let rss_after = read_vm_rss_kb().unwrap_or(0);
    let growth_kb = rss_after.saturating_sub(rss_before);
    println!(
        "RSS before={}MB after={}MB growth={}MB",
        rss_before / 1024,
        rss_after / 1024,
        growth_kb / 1024
    );
    assert!(
        growth_kb < MAX_RSS_GROWTH_KB,
        "YCSB WAL perf sweep grew RSS by {}MB (budget: {}MB)",
        growth_kb / 1024,
        MAX_RSS_GROWTH_KB / 1024,
    );
}
