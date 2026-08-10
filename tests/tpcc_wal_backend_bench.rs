//! Real-workload comparison (not a synthetic WAL-only microbenchmark like
//! `wal_writer_throughput_bench.rs`): runs the actual TPC-C driver end to
//! end — population, concurrent terminals doing the standard 5-txn mix,
//! real B-tree traversal/contention/GC — under the batched `WalWriter` vs.
//! the lock-free `LockFreeWalBackend` at a couple of local-batch sizes, to
//! see whether the WAL append-path difference actually shows up once it's
//! competing with everything else a real transaction does (tree traversal,
//! latch contention, GC), not just in isolation.
//!
//! `#[ignore]`d: takes real wall-clock time (population + several timed
//! runs) and writes real files to disk. Run with:
//! `cargo test --bin cMVBT compare_wal_backends_tpcc --release -- --ignored --nocapture --test-threads=1`

use std::path::PathBuf;
use std::time::Duration;

use crate::mv_bench::olap_scan::OlapMode;
use crate::mv_bench::tpcc_driver::{run_tpcc, DriverConfig};
use crate::mv_bench::tpcc_schema::TpccConfig;
use crate::mv_root::index_root::RootIndexType;

// >= max(THREAD_COUNTS): `run_tpcc` clamps `num_terminals` down to
// `num_warehouses` under `affinity: true` (each terminal needs >= 1 owned
// warehouse), so a lower warehouse count would silently make the higher
// thread-count runs identical to the lower one instead of a real comparison.
const WAREHOUSES: u32 = 16;
const DURATION_SECS: u64 = 8;
const THREAD_COUNTS: &[usize] = &[8, 16];
/// `(label, wal_lockfree_batch_size)` — `None` is the existing batched
/// `WalWriter`; `Some(n)` is `LockFreeWalBackend` with that per-worker
/// local-batch size (see `mv_wal::lockfree_writer::LocalBatch`).
const BACKENDS: &[(&str, Option<usize>)] = &[
    ("batched", None),
    ("lockfree-batch16", Some(16)),
    ("lockfree-batch64", Some(64)),
];

fn config(num_terminals: usize, wal_path: PathBuf, batch_size: Option<usize>) -> DriverConfig {
    DriverConfig {
        tpcc: TpccConfig { num_warehouses: WAREHOUSES, ..TpccConfig::default() },
        num_terminals,
        duration: Duration::from_secs(DURATION_SECS),
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
        output_dir: std::env::temp_dir().join("cmvbt_tpcc_wal_bench_out"),
    }
}

#[test]
#[ignore]
fn compare_wal_backends_tpcc() {
    let dir = std::env::temp_dir();
    println!();
    println!("=== TPC-C: WAL backend comparison ({WAREHOUSES} warehouses, {DURATION_SECS}s/run) ===");
    for &threads in THREAD_COUNTS {
        for &(label, batch_size) in BACKENDS {
            let wal_path = dir.join(format!("cmvbt_tpcc_wal_bench_{label}_{threads}_{}.log", std::process::id()));
            let _ = std::fs::remove_file(&wal_path);

            let summary = run_tpcc(config(threads, wal_path.clone(), batch_size));

            println!(
                "TPCC  backend={label:<18} terminals={threads:<3} tpmC={:>10.1}  scans/sec={:>8.1}",
                summary.tpm_c, summary.avg_scan_tuples_per_sec,
            );

            let _ = std::fs::remove_file(&wal_path);
        }
    }
}
