//! Real-workload comparison (not a synthetic WAL-only microbenchmark like
//! `wal_writer_throughput_bench.rs`): runs the actual TPC-C driver end to
//! end — population, concurrent terminals doing the standard 5-txn mix,
//! real B-tree traversal/contention/GC — under the batched `WalWriter` vs.
//! the lock-free `LockFreeWalBackend` at a couple of local-batch sizes, to
//! see whether the WAL append-path difference actually shows up once it's
//! competing with everything else a real transaction does (tree traversal,
//! latch contention, GC), not just in isolation.
//!
//! The normal test uses a small smoke configuration. Set
//! `BATSTORE_FULL_BENCH=1` to restore the original 16-warehouse, 8-second,
//! 8/16-terminal comparison; use `--release -- --nocapture --test-threads=1`
//! for meaningful full-scale numbers.

use std::path::PathBuf;
use std::time::Duration;

use crate::mv_bench::olap_scan::OlapMode;
use crate::mv_bench::tpcc_driver::{DriverConfig, run_tpcc};
use crate::mv_bench::tpcc_schema::TpccConfig;
use crate::mv_root::index_root::RootIndexType;

// >= max(THREAD_COUNTS): `run_tpcc` clamps `num_terminals` down to
// `num_warehouses` under `affinity: true` (each terminal needs >= 1 owned
// warehouse), so a lower warehouse count would silently make the higher
// thread-count runs identical to the lower one instead of a real comparison.
const FULL_WAREHOUSES: u32 = 16;
const FULL_DURATION_SECS: u64 = 8;
const FULL_THREAD_COUNTS: &[usize] = &[8, 16];
/// `(label, wal_lockfree_batch_size)` — `None` is the existing batched
/// `WalWriter`; `Some(n)` is `LockFreeWalBackend` with that per-worker
/// local-batch size (see `mv_wal::lockfree_writer::LocalBatch`).
const BACKENDS: &[(&str, Option<usize>)] = &[
    ("batched", None),
    ("lockfree-batch16", Some(16)),
    ("lockfree-batch64", Some(64)),
];

fn full_scale() -> bool {
    std::env::var_os("BATSTORE_FULL_BENCH").is_some()
}

fn config(
    num_terminals: usize,
    wal_path: PathBuf,
    batch_size: Option<usize>,
    full: bool,
) -> DriverConfig {
    let tpcc = if full {
        TpccConfig {
            num_warehouses: FULL_WAREHOUSES,
            ..TpccConfig::default()
        }
    } else {
        TpccConfig {
            num_warehouses: 2,
            districts_per_warehouse: 2,
            customers_per_district: 100,
            num_items: 5_000,
            initial_orders_per_district: 100,
            initial_new_orders: 30,
            num_suppliers: 100,
        }
    };
    DriverConfig {
        tpcc,
        num_terminals,
        duration: Duration::from_secs(if full { FULL_DURATION_SECS } else { 1 }),
        affinity: true,
        gc: true,
        update_in_place: false,
        root_star_index: RootIndexType::FrugalList,
        big_tree_size: Default::default(),
        // OLAP off: isolates the OLTP/WAL-write comparison from OLAP scan
        // interference, which is an orthogonal axis already covered by
        // this driver's own `ch_benchmark`/`htap` experiments.
        olap_mode: OlapMode::RepeatedFreshFullScan,
        num_olap_threads: 0,
        wal: Some((wal_path, Duration::from_millis(5))),
        wal_lockfree_batch_size: batch_size,
        htap_baseline: None,
        output_dir: std::env::temp_dir().join("batstore_tpcc_wal_bench_out"),
    }
}

#[test]
fn compare_wal_backends_tpcc() {
    let full = full_scale();
    let thread_counts: &[usize] = if full { FULL_THREAD_COUNTS } else { &[2] };
    let warehouses = if full { FULL_WAREHOUSES } else { 2 };
    let duration = if full { FULL_DURATION_SECS } else { 1 };
    let dir = std::env::temp_dir();
    println!();
    println!("=== TPC-C: WAL backend comparison ({warehouses} warehouses, {duration}s/run) ===");
    for &threads in thread_counts {
        for &(label, batch_size) in BACKENDS {
            let wal_path = dir.join(format!(
                "batstore_tpcc_wal_bench_{label}_{threads}_{}.log",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&wal_path);

            let summary = run_tpcc(config(threads, wal_path.clone(), batch_size, full));

            println!(
                "TPCC  backend={label:<18} terminals={threads:<3} tpmC={:>10.1}  scans/sec={:>8.1}",
                summary.tpm_c, summary.avg_scan_tuples_per_sec,
            );

            let _ = std::fs::remove_file(&wal_path);
        }
    }
}
