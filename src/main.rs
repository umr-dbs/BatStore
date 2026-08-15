use crate::mv_block::block::Block;
use crate::mv_test::{main_append, main_generate, main_load, main_load_ycsb};
use chrono::{DateTime, Local};
use itertools::Itertools;
use std::{env, fs};

use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::{CRUDOperation, TxAtomicOperation};
use crate::mv_crud_model::crud_operation_result::{AtomicTxResult, CRUDOperationResult};
use crate::mv_tree::mvbt::Key;
use crate::mv_tree::mvbt::NUM_RECORDS;
use crate::mv_tree::mvbt::Payload;
use crate::mv_tree::mvbt::{FAN_OUT, MVBT};
use crate::mv_bench::tpcc_schema::TPCC_FAN_OUT;
use crate::mv_bench::tpcc_schema::TPCC_NUM_RECORDS;
use crate::mv_bench::ycsb_schema::{YcsbKey, YcsbRow, YCSB_FAN_OUT, YCSB_NUM_RECORDS};

mod mv_bench;
mod mv_block;
mod mv_crud_model;
mod mv_gc;
mod mv_page_model;
mod mv_query;
mod mv_record_model;
mod mv_test;
mod mv_tree;
mod mv_root;
mod mv_sync;
mod mv_wal;
mod mv_db;
#[cfg(feature = "tree-viz")]
mod mv_viz;

use crate::mv_sync::smart_cell::OptCell;
#[cfg(all(not(miri), not(feature = "mimalloc")))]
use jemallocator::Jemalloc;
#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use crate::mv_bench::tpcc_schema::{TpccKey, TpccRow};

// Miri interprets pure Rust/LLVM IR only — it can't run either allocator's
// FFI'd C, so this swaps in the default (System) allocator under `cargo
// miri`. The `mimalloc` feature (see `Cargo.toml`) swaps jemalloc for
// mimalloc as an A/B experiment - see that feature's doc for why.
#[cfg(all(not(miri), not(feature = "mimalloc")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(all(not(miri), feature = "mimalloc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    startup();

    let args = env::args();
    let parms = args.collect_vec();

    if parms.len() > 1  {
        match parms[1].as_str() {
            "" | "test" => test(),
            "minimal_repro" => minimal_repro(),
            "generate" => main_generate(parms),
            "append" => main_append(parms),
            "load" => main_load(parms),
            "load2" => main_load_ycsb(parms),
            "tpcc" => mv_bench::tpcc_driver::main_tpcc(parms),
            "tpch" => mv_bench::tpcc_driver::main_tpch(parms),
            "htap" => mv_bench::tpcc_driver::main_htap(parms),
            "ycsb" => mv_bench::ycsb_driver::main_ycsb(parms),
            "s_htap" => mv_bench::s_htap_driver::main_s_htap(parms),
            #[cfg(feature = "mdbx-backend")]
            "mdbx_ycsb" => mv_bench::mdbx_ycsb::main_mdbx_ycsb(parms),
            #[cfg(feature = "mdbx-backend")]
            "mdbx_tpcc" => mv_bench::mdbx_tpcc::main_mdbx_tpcc(parms),
            #[cfg(feature = "mdbx-backend")]
            "mdbx_s_htap" => mv_bench::mdbx_s_htap::main_mdbx_s_htap(parms),
            "benchmark" => mv_bench::suite::main_benchmark(parms),
            "_bench_one" => mv_bench::suite::main_bench_one(parms),
            #[cfg(feature = "tree-viz")]
            "viz_demo" => mv_test::main_viz_demo(parms),
            // "load_cc_new" => main_load_cc_new(parms),
            // "sorted_insert" => main_sorted_insert(parms),
            s => println!("Unknown Command '{s}'")
        }
    }
    else {
        println!("*********** Use a Command ***********")
    }

    // fs::write("restarts.csv", "\n").unwrap();
    //
    // let mut f = OpenOptions::new()
    //     .append(true)
    //     .create(true)
    //     .open("restarts.csv")
    //     .unwrap();
    //
    // f.write_all( unsafe { RESTARTS_COUNTER.as_ref() }
    //     .iter()
    //     .map(|a| a.load(SeqCst))
    //     .join(",")
    //     .as_bytes())
    //     .unwrap();
    //
    // println!("Restarts: {}", unsafe { RESTARTS_COUNTER.as_ref() }
    //     .iter()
    //     .enumerate()
    //     .map(|(i, count)| format!("{i}: {}", count.load(SeqCst)))
    //     .join("\n"))
}

/// Minimal, TPCC-free regression repro for the `RangeIterSi`
/// `register_reader_si` bug (see `dispatch.rs`'s `CRUDOperation::RangeIterSi`
/// arm): many concurrent inserts forcing heavy split churn on one plain
/// tree, racing concurrent deletes and concurrent `RangeSi` scans, while GC
/// block-reclaim is on. Before the fix this reliably crashed (a block a
/// scan was still traversing got reclaimed and repopulated mid-read,
/// exposing a torn/never-written record slot) within seconds; after the
/// fix it should run clean for the full duration.
fn minimal_repro() {
    use crate::mv_bench::tpcc_schema::{OrderLine, TpccRow, TpccTree};
    use crate::mv_crud_model::crud_operation::TxAtomicOperation;
    use crate::mv_crud_model::crud_operation_result::AtomicTxResult;
    use crate::mv_query::interval::Interval;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
    use std::sync::Arc;
    use std::time::Duration;

    fn line(i_id: u32) -> TpccRow {
        TpccRow::OrderLine(Box::new(OrderLine {
            ol_i_id: i_id,
            ol_supply_w_id: 1,
            ol_delivery_d: None,
            ol_quantity: 5,
            ol_amount: 3.14,
            ol_dist_info: "s".repeat(24),
        }))
    }

    let tree = Arc::new(TpccTree::default());
    tree.enable_gc(false);

    let stop = Arc::new(AtomicBool::new(false));
    let next_key = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for _ in 0..4 {
        let tree = tree.clone();
        let stop = stop.clone();
        let next_key = next_key.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Relaxed) {
                let key = next_key.fetch_add(1, Relaxed);
                let _ = tree.dispatch_atomic_transaction(TxAtomicOperation::Insert(key, line(key as u32)));
            }
        }));
    }

    for _ in 0..2 {
        let tree = tree.clone();
        let stop = stop.clone();
        handles.push(std::thread::spawn(move || {
            let mut k = 0u64;
            while !stop.load(Relaxed) {
                let _ = tree.dispatch_atomic_transaction(TxAtomicOperation::Delete(k));
                k = k.wrapping_add(1);
            }
        }));
    }

    for _ in 0..2 {
        let tree = tree.clone();
        let stop = stop.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Relaxed) {
                if let AtomicTxResult::MatchedRecords(records) =
                    tree.dispatch_atomic_transaction(TxAtomicOperation::RangeSi(Interval::new(0, u64::MAX)))
                {
                    let _ = records.len();
                }
            }
        }));
    }

    let secs: u64 = env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(30);
    std::thread::sleep(Duration::from_secs(secs));
    stop.store(true, Relaxed);
    for h in handles {
        let _ = h.join();
    }
    println!("minimal_repro: completed without corruption detected");
}

fn test() {
    let tree = MVBT::default();

    for key in 0..10_000_000 {
        let res
            = tree.dispatch_atomic_transaction(TxAtomicOperation::Insert(key, 0));

        if let AtomicTxResult::Inserted(..) = res {
        } else { panic!("Error") }
    }

    for v in (10..20).step_by(2) {
        let range
            = tree.dispatch_atomic_transaction(TxAtomicOperation::Range((0..Key::MAX).into(), v));

        match range {
            CRUDOperationResult::MatchedRecords(records) =>{
                let len = records.len();
                let str_re = records.iter().join("\n");

                println!("Len= {}\n{}", len, str_re);
            }
            s => println!("ERROR = {s}")
        }
    }

}
/// Essential function.
fn make_splash() {
    let datetime: DateTime<Local> = fs::metadata(env::current_exe().unwrap())
        .unwrap()
        .modified()
        .unwrap()
        .into();

    println!("                         _________________________");
    println!("                 _______/                         \\_______");
    println!("                /                                         \\");
    println!(" +-------------+                                           +-------------+");
    println!(" |                                                                       |");
    println!(" |               ------------------------------                          |");
    println!(
        " |               # Build:   {}                          |",
        datetime.format("%d-%m-%Y %T")
    );
    println!(
        " |               # Current version: {}                              |",
        env!("CARGO_PKG_VERSION")
    );
    println!(" |               --------------------------                              |");
    println!(
        " |               # HLE:   {}                                         |",
        hle()
    );
    // println!(" |               # RW-HLE:    AUTO                                       |");
    println!(" |               -----------------                                       |");
    println!(" |                                                                       |");
    println!(" |               ----------------------------------------------          |");
    println!(" |               # E-Mail: amir.tonta@mathematik.uni-marburg.de          |");
    println!(" |               # Written by: Amir Tonta                                |");
    println!(" |               # First released: 02-01-2024                            |");
    println!(" |               # Repository: https://github.com/umr-dbs/cMVBT          |");
    println!(" |               -----------------------------------------------------   |");
    println!(" |                                                                       |");
    println!(" |               ...cMVBT Application Launching...                       |");
    println!(" +-------------+                                           +-------------+");
    println!("                \\_______                           _______/");
    println!("                        \\_________________________/");

    println!();
    println!("--> System Log:");
}

fn startup() {
    make_splash();

    println!(">>HLE: \t\t\t\t{}", hle());
    println!("++++++++++++++++++++++++++++++++++++++++++++++++++");
    let block_size = size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>();
    let b_kb = block_size as f32 / 1024f32;

    let cell_sz = size_of::<OptCell<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>>();
    let cell_kb = cell_sz as f32 / 1024f32;
    println!(
        "\
           >>u64: FAN_OUT: \t\t{FAN_OUT}\n\
           >>u64: NUM_RECORDS: \t\t{NUM_RECORDS}\n\
           >>u64: size_of(BLOCK): \t\t{} bytes; {b_kb} kb\n\
           >>u64: size_of(CELL): \t\t{} bytes; {cell_kb} kb\n",
        block_size,
        cell_sz,
    );

    println!("*****************************************************");
    let block_size = size_of::<Block<TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>>();
    let b_kb = block_size as f32 / 1024f32;

    let cell_sz = size_of::<OptCell<Block<TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>>>();
    let cell_kb = cell_sz as f32 / 1024f32;
    println!(
        "\
           >>TPC-C: FAN_OUT: \t\t{TPCC_FAN_OUT}\n\
           >>TPC-C: NUM_RECORDS: \t\t{TPCC_NUM_RECORDS}\n\
           >>TPC-C: size_of(BLOCK): \t{} bytes; {b_kb} kb\n\
           >>TPC-C: size_of(CELL): \t{} bytes; {cell_kb} kb\n",
        block_size,
        cell_sz,
    );
    println!("*****************************************************");
    let block_size = size_of::<Block<YCSB_FAN_OUT, YCSB_NUM_RECORDS, YcsbKey, YcsbRow>>();
    let b_kb = block_size as f32 / 1024f32;

    let cell_sz = size_of::<OptCell<Block<YCSB_FAN_OUT, YCSB_NUM_RECORDS, YcsbKey, YcsbRow>>>();
    let cell_kb = cell_sz as f32 / 1024f32;
    println!(
        "\
           >>YCSB: FAN_OUT: \t\t{YCSB_FAN_OUT}\n\
           >>YCSB: NUM_RECORDS: \t\t{YCSB_NUM_RECORDS}\n\
           >>YCSB: size_of(BLOCK): \t{} bytes; {b_kb} kb\n\
           >>YCSB: size_of(CELL): \t\t{} bytes; {cell_kb} kb\n",
        block_size,
        cell_sz,
    );
    println!("*****************************************************");
    println!("*****************************************************");
}

pub fn hle() -> &'static str {
    if cfg!(feature = "hardware-lock-elision") {
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            "ON    "
        } else {
            "NO HLE"
        }
    } else {
        "OFF   "
    }
}

