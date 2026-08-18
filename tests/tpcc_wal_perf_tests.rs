//! Fast, always-on performance smoke test for the WAL backend comparison
//! (see `tests/tpcc_wal_backend_bench.rs` for the full-scale, `#[ignore]`d
//! version meant for manual runs). This one stays in the default `cargo
//! test` run: a deliberately tiny data set and sub-second timed phases so
//! the whole sweep finishes in a few seconds, while still exercising the
//! real TPC-C driver (population, concurrent terminals, GC, WAL) against
//! both `WalWriter` and `LockFreeWalBackend` at a couple of thread
//! counts/batch sizes — enough to catch a WAL-backend regression (crash,
//! zero throughput, or unbounded memory growth) fast, without needing the
//! full-scale run to notice.
//!
//! Budget: wall time well under 20s; the `MAX_RSS_GROWTH_MB` assertion below
//! is a 20 GB safety ceiling on this whole sweep's own RSS growth — small
//! enough that a bug reintroducing runaway growth (like the `RESTART_TRACE`
//! leak that OOM'd `compare_wal_backends_tpcc` at full scale) gets caught
//! here, fast, instead of only in the full-scale manual run.

use std::path::PathBuf;
use std::time::Duration;

use crate::mv_bench::mem_stats::read_vm_rss_kb;
use crate::mv_bench::olap_scan::OlapMode;
use crate::mv_bench::tpcc_driver::{DriverConfig, run_tpcc};
use crate::mv_bench::tpcc_schema::TpccConfig;
use crate::mv_root::index_root::RootIndexType;

const WAREHOUSES: u32 = 4;
const DURATION: Duration = Duration::from_millis(300);
const THREAD_COUNTS: &[usize] = &[2, 4];
/// See `tests/tpcc_wal_backend_bench.rs`'s `BACKENDS` doc.
const BACKENDS: &[(&str, Option<usize>)] = &[
    ("batched", None),
    ("lockfree-batch16", Some(16)),
    ("lockfree-batch64", Some(64)),
];
/// 20 GB safety ceiling (this test's own ask), converted to the same units
/// `read_vm_rss_kb` returns.
const MAX_RSS_GROWTH_KB: u64 = 20_000_000;

fn config(num_terminals: usize, wal_path: PathBuf, batch_size: Option<usize>) -> DriverConfig {
    DriverConfig {
        // Tiny catalog: keeps population (which runs before the timed
        // phase, unavoidably counted against this test's wall-clock budget)
        // down to well under a second, unlike the full-scale bench's
        // defaults (100k items / 3k customers per district).
        tpcc: TpccConfig {
            num_warehouses: WAREHOUSES,
            customers_per_district: 10,
            num_items: 100,
            initial_orders_per_district: 10,
            initial_new_orders: 3,
            num_suppliers: 20,
            ..TpccConfig::default()
        },
        num_terminals,
        duration: DURATION,
        affinity: true,
        gc: true,
        update_in_place: false,
        root_star_index: RootIndexType::FrugalList,
        big_tree_size: Default::default(),
        olap_mode: OlapMode::RepeatedFreshFullScan,
        num_olap_threads: 0,
        wal: Some((wal_path, Duration::from_millis(5))),
        wal_lockfree_batch_size: batch_size,
        htap_baseline: None,
        output_dir: std::env::temp_dir().join("batstore_tpcc_wal_perf_test_out"),
    }
}

#[test]
fn wal_backend_perf_sweep_tpcc() {
    let dir = std::env::temp_dir();
    let rss_before = read_vm_rss_kb().unwrap_or(0);

    println!("\n=== TPC-C WAL backend perf sweep ({WAREHOUSES} warehouses, {DURATION:?}/run) ===");
    for &threads in THREAD_COUNTS {
        for &(label, batch_size) in BACKENDS {
            let wal_path = dir.join(format!(
                "batstore_tpcc_wal_perf_{label}_{threads}_{}.log",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&wal_path);

            let summary = run_tpcc(config(threads, wal_path.clone(), batch_size));
            println!(
                "TPCC  backend={label:<18} terminals={threads:<3} tpmC={:>10.1}",
                summary.tpm_c
            );
            assert!(
                summary.totals.iter().sum::<u64>() > 0,
                "backend={label} terminals={threads}: no transactions completed at all"
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
        "TPC-C WAL perf sweep grew RSS by {}MB (budget: {}MB)",
        growth_kb / 1024,
        MAX_RSS_GROWTH_KB / 1024,
    );
}
