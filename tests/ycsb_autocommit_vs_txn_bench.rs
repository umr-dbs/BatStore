//! Short sweep answering: for YCSB, is `Atomic` (auto-commit) faster than
//! `Transaction` (explicit begin/commit) mode because of the execution-mode
//! machinery itself, or does the gap actually come from payload size?
//!
//! Crosses `YcsbExecutionMode::{Atomic, Transaction}` with a few
//! `field_length` values on workload A (50% read / 50% update), single
//! thread, no WAL, so the only variables are execution mode and payload
//! size. See `ycsb_wal_perf_tests.rs` for the config-sweep pattern this
//! copies.

use std::time::Duration;

use crate::bat_bench::mem_stats::read_vm_rss_kb;
use crate::bat_bench::ycsb_driver::{DriverConfig, run_ycsb};
use crate::bat_bench::ycsb_random::{RequestDistribution, YcsbMix};
use crate::bat_bench::ycsb_schema::YcsbConfig;
use crate::bat_bench::ycsb_txn::YcsbExecutionMode;
use crate::bat_root::index_root::RootIndexType;

const RECORD_COUNT: u64 = 2_000;
const DURATION: Duration = Duration::from_millis(300);
const FIELD_LENGTHS: &[usize] = &[16, 100, 1_000, 4_000];
const MODES: &[(&str, YcsbExecutionMode)] = &[
    ("atomic", YcsbExecutionMode::Atomic),
    ("transaction", YcsbExecutionMode::Transaction),
];
/// 20 GB safety ceiling, same rationale as `ycsb_wal_perf_tests.rs`.
const MAX_RSS_GROWTH_KB: u64 = 20_000_000;

fn config(field_length: usize, execution_mode: YcsbExecutionMode) -> DriverConfig {
    DriverConfig {
        ycsb: YcsbConfig {
            record_count: RECORD_COUNT,
            field_count: 10,
            field_length,
        },
        num_threads: 1,
        duration: DURATION,
        mix: YcsbMix::workload("a").expect("workload 'a' must exist"),
        distribution: RequestDistribution::Zipfian { theta: 0.99 },
        max_scan_length: 100,
        write_all_fields: false,
        read_payload: true,
        execution_mode,
        gc: true,
        update_in_place: false,
        root_star_index: RootIndexType::FrugalList,
        wal: None,
        wal_lockfree_batch_size: None,
        output_dir: std::env::temp_dir().join("batstore_ycsb_autocommit_vs_txn_bench_out"),
        // Workload A never scans, so this would auto-decide to `None` anyway.
        scan_pool_workers: None,
        idle_compaction: None,
    }
}

#[test]
fn atomic_vs_transaction_across_payload_sizes() {
    let rss_before = read_vm_rss_kb().unwrap_or(0);

    println!(
        "\n=== YCSB workload A: atomic vs transaction across payload sizes ({RECORD_COUNT} records, {DURATION:?}/run, 1 thread) ==="
    );
    println!(
        "{:<12} {:>12} {:>16} {:>16} {:>10}",
        "mode", "field_len", "payload_bytes", "ops/sec", "vs atomic"
    );

    for &field_length in FIELD_LENGTHS {
        let mut atomic_throughput = None;
        for &(label, mode) in MODES {
            let summary = run_ycsb(config(field_length, mode));
            let ops = summary.throughput_ops_sec;
            if label == "atomic" {
                atomic_throughput = Some(ops);
            }
            let baseline = atomic_throughput.unwrap_or(ops);
            let ratio = if baseline > 0.0 { ops / baseline } else { 1.0 };
            println!(
                "{label:<12} {field_length:>12} {:>16} {ops:>16.1} {ratio:>9.2}x",
                field_length * 10, // field_count = 10
            );
            assert!(
                summary.totals.iter().sum::<u64>() > 0,
                "mode={label} field_length={field_length}: no ops completed at all"
            );
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
        "atomic-vs-transaction payload sweep grew RSS by {}MB (budget: {}MB)",
        growth_kb / 1024,
        MAX_RSS_GROWTH_KB / 1024,
    );
}
