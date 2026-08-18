//! libmdbx-backed TPC-C benchmark driver - the TPC-C counterpart to
//! `mdbx_ycsb.rs` (see that file's module docs for the general rationale:
//! path-copying/copy-on-write MVCC via libmdbx, compared against this
//! crate's own version-chain MVBTree, through the same standard workload).
//!
//! Reuses `tpcc_schema`/`tpcc_random`/`tpcc_wal_codec` entirely as-is - the
//! row structs, `TpccConfig`, every key-builder function (`k_warehouse`,
//! `k_customer`, ...), and `TpccRow`'s `WalPayload` byte codec are all
//! storage-engine-agnostic (confirmed: nothing in that layer touches
//! BatStore's own tree types). Only the transaction logic
//! (`new_order`/`payment`/`order_status`/`delivery`/`stock_level`, mirroring
//! `tpcc_txn.rs` function-for-function) and the driver loop are
//! reimplemented against libmdbx's `Transaction<RO|RW>` API - there's no
//! trait boundary in this codebase between BatStore's tree and its business
//! logic (see `mdbx_ycsb.rs`'s docs for why this is a parallel file, not a
//! generic backend swapped into `tpcc_driver.rs`).
//!
//! Only the 11 core TPC-C tables are used (not CH-benCHmark's SUPPLIER/
//! NATION/REGION addition) - so unlike `tpcc_driver.rs`'s full 4-query
//! `OlapMode::ChBenchmark` rotation (`mv_bench::tpch_queries`/`olap_scan.rs`),
//! only [`mdbx_q1`]/[`mdbx_q6`] are implemented here: CH-benCHmark's Q1
//! ("Pricing Summary Report") and Q6 ("Forecasting Revenue Change") are the
//! only 2 of its 22 queries that are pure aggregations over `ORDER_LINE`
//! alone, no joins against the missing dimension tables needed (see
//! `tpch_queries` module docs for why these two specifically were chosen as
//! the portable pair) - Q4/Q5 (which do need SUPPLIER/NATION/REGION) are not
//! ported here, matching every other engine's own htap_q1/htap_q6-only scope
//! (see scripts/engines/libmdbx.py and `common.HTAP_WORKLOADS`'s doc).
//!
//! Conflict handling differs fundamentally from BatStore's OSIC: libmdbx (like
//! LMDB) allows only one read-write transaction active process-wide at a
//! time, so two New-Order transactions can never race on the same write -
//! the writer lock itself is the concurrency control. There is no
//! `TxnOutcome::Conflict` case a concurrent writer can trigger here; it's
//! kept only for the same defensive "row unexpectedly missing" checks
//! `tpcc_txn.rs` has (which would indicate a genuine data/scale bug, not a
//! race), so the two drivers' output schemas match.

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use libmdbx::{Database, DatabaseOptions, Mode, ReadWriteOptions, SyncMode, Table as MdbxTable, TableFlags, Transaction, TransactionKind, WriteFlags, WriteMap, RO, RW};
use rand::prelude::*;

use crate::mv_bench::mem_stats::{MemSampler, DEFAULT_SAMPLE_INTERVAL};
use crate::mv_bench::tpcc_random::*;
use crate::mv_bench::tpcc_schema::*;
use crate::mv_wal::record::WalPayload;

pub struct MdbxTpccConfig {
    pub tpcc: TpccConfig,
    pub num_terminals: usize,
    pub duration: Duration,
    pub db_path: PathBuf,
    pub output_dir: PathBuf,
    /// Runs one extra read-only OLAP thread repeating the selected [`mdbx_q1`] or [`mdbx_q6`]
    /// concurrently with the OLTP terminals - htap_q1/htap_q6's "run
    /// CH-benCHmark queries alongside OLTP" mechanism (mirrors
    /// `tpcc_driver.rs`'s `OlapMode::ChBenchmark`, minus Q4/Q5 - see module
    /// docs). `false` for plain `tpcc`/`ycsb_*` runs, which skip the extra
    /// thread and the `tpcc_scan.csv` output entirely.
    pub htap_mode: MdbxHtapMode,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum MdbxHtapMode {
    None,
    Q1,
    Q6,
}

pub struct MdbxTpccRunSummary {
    pub tpm_c: f64,
    pub totals: [u64; NUM_COUNTERS],
}

const NO: usize = 0;
const PAY: usize = 3;
const OS: usize = 6;
const SL: usize = 9;
const DELIV_DISTRICTS: usize = 12;
const DELIV_CONFLICTS: usize = 13;
const NUM_COUNTERS: usize = 14;

const COUNTER_NAMES: [&str; NUM_COUNTERS] = [
    "new_order_committed", "new_order_conflict", "new_order_user_abort",
    "payment_committed", "payment_conflict", "payment_user_abort",
    "order_status_committed", "order_status_conflict", "order_status_user_abort",
    "stock_level_committed", "stock_level_conflict", "stock_level_user_abort",
    "delivery_districts_delivered", "delivery_conflicts",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnOutcome {
    Committed,
    Conflict,
    UserAbort,
}

#[inline]
fn record(totals: &mut [u64; NUM_COUNTERS], base: usize, outcome: TxnOutcome) {
    match outcome {
        TxnOutcome::Committed => totals[base] += 1,
        TxnOutcome::Conflict => totals[base + 1] += 1,
        TxnOutcome::UserAbort => totals[base + 2] += 1,
    }
}

// ---------------------------------------------------------------------
// libmdbx plumbing: one named table per TPC-C table (Table::as_str()),
// opened fresh in every transaction (a cheap DBI lookup, matching the
// existing code's own per-call `db.tree_for(table)` style rather than
// caching handles across transactions - Table<'txn> borrows from its own
// transaction's lifetime, so it can't be cached across them anyway).
// ---------------------------------------------------------------------

fn open_db(path: &std::path::Path, num_terminals: usize) -> Database<WriteMap> {
    fs::create_dir_all(path).unwrap_or_else(|e| panic!("mdbx_tpcc: failed to create db dir {}: {e}", path.display()));
    // libmdbx's reader-slot table defaults to 61 (MDBX_READERS_FULL beyond that) -
    // below our own terminal-count sweep, which was silently aborting/hanging
    // worker threads via the `.expect` calls below. Size it to the actual
    // terminal count plus headroom for the table-creation txn and any internal use.
    let options = DatabaseOptions {
        max_tables: Some(Table::ALL.len() as u64),
        max_readers: Some((num_terminals as std::ffi::c_uint).saturating_add(8)),
        mode: Mode::ReadWrite(ReadWriteOptions { sync_mode: SyncMode::UtterlyNoSync, ..Default::default() }),
        ..Default::default()
    };
    let db = Database::<WriteMap>::open_with_options(path, options)
        .unwrap_or_else(|e| panic!("mdbx_tpcc: failed to open database at {}: {e}", path.display()));
    let txn = db.begin_rw_txn().expect("mdbx_tpcc: begin_rw_txn (table creation)");
    for t in Table::ALL {
        txn.create_table(Some(t.as_str()), TableFlags::empty()).expect("mdbx_tpcc: create_table");
    }
    txn.commit().expect("mdbx_tpcc: commit (table creation)");
    db
}

fn tbl<'txn, K: TransactionKind>(txn: &'txn Transaction<'_, K, WriteMap>, table: Table) -> MdbxTable<'txn> {
    txn.open_table(Some(table.as_str())).expect("mdbx_tpcc: open_table")
}

fn get_row<K: TransactionKind>(txn: &Transaction<K, WriteMap>, table: Table, key: TpccKey) -> Option<TpccRow> {
    let t = tbl(txn, table);
    let bytes = txn.get::<Vec<u8>>(&t, &key.to_be_bytes()).expect("mdbx_tpcc: get")?;
    TpccRow::wal_decode(&bytes)
}

fn put_row(txn: &Transaction<RW, WriteMap>, table: Table, key: TpccKey, row: &TpccRow) {
    let t = tbl(txn, table);
    let mut buf = Vec::new();
    row.wal_encode(&mut buf);
    txn.put(&t, key.to_be_bytes(), &buf, WriteFlags::UPSERT).expect("mdbx_tpcc: put");
}

fn delete_row(txn: &Transaction<RW, WriteMap>, table: Table, key: TpccKey) -> bool {
    let t = tbl(txn, table);
    txn.del(&t, key.to_be_bytes(), None).expect("mdbx_tpcc: del")
}

/// Range scan `[lo, hi]` inclusive - mirrors `TpccTxn::range`'s eager-collect contract.
fn range_rows<K: TransactionKind>(txn: &Transaction<K, WriteMap>, table: Table, lo: TpccKey, hi: TpccKey) -> Vec<(TpccKey, TpccRow)> {
    let t = tbl(txn, table);
    let mut cursor = txn.cursor(&t).expect("mdbx_tpcc: cursor");
    let mut out = Vec::new();
    let mut item = cursor.set_range::<Vec<u8>, Vec<u8>>(&lo.to_be_bytes()).expect("mdbx_tpcc: cursor.set_range");
    while let Some((k, v)) = item {
        let key = TpccKey::from_be_bytes(k.as_slice().try_into().expect("mdbx_tpcc: malformed key"));
        if key > hi {
            break;
        }
        if let Some(row) = TpccRow::wal_decode(&v) {
            out.push((key, row));
        }
        item = cursor.next::<Vec<u8>, Vec<u8>>().expect("mdbx_tpcc: cursor.next");
    }
    out
}

fn pick_middle_by_name(matches: &[(TpccKey, TpccRow)]) -> u32 {
    let mid = (matches.len() + 1) / 2 - 1;
    decode_customer_name_idx_c_id(matches[mid].0)
}

// ---------------------------------------------------------------------
// CH-benCHmark Q1/Q6 (mirrors `mv_bench::tpch_queries::q1`/`q6` function-for-
// function, against libmdbx's `Transaction<RO>` instead of `TpccTxn` - see
// module docs on why only these 2 queries are ported here).
// ---------------------------------------------------------------------

/// Per-`ol_number` group produced by [`mdbx_q1`] - mirrors
/// `tpch_queries::OrderLineSummary`.
#[derive(Clone, Copy, Debug, Default)]
struct OrderLineSummary {
    ol_number: u8,
    count: u64,
    sum_qty: u64,
    sum_amount: f64,
}

/// CH-benCHmark Q1 ("Pricing Summary Report") - see
/// `tpch_queries::q1`'s doc for the full rationale (groups every delivered
/// order-line by `ol_number`, this schema's stand-in for `l_returnflag`/
/// `l_linestatus`). One full `ORDER_LINE` table scan under a read-only
/// snapshot; returns that snapshot's libmdbx transaction id alongside the
/// result (this engine's analogue of `tpch_queries::q1`'s `Version`, used the
/// same way - see `olap_thread`'s staleness computation).
fn mdbx_q1(db: &Database<WriteMap>, delivered_before: i64) -> (Vec<OrderLineSummary>, u64) {
    let txn = db.begin_ro_txn().expect("mdbx_tpcc: begin_ro_txn (q1)");
    let ts_start = txn.id();
    let lines = range_rows(&txn, Table::OrderLine, TpccKey::MIN, TpccKey::MAX);

    let mut groups: [OrderLineSummary; 16] =
        std::array::from_fn(|i| OrderLineSummary { ol_number: i as u8, ..Default::default() });

    for (key, row) in &lines {
        let ol = row.as_order_line();
        let Some(delivered) = ol.ol_delivery_d else { continue };
        if delivered > delivered_before {
            continue;
        }
        let g = &mut groups[decode_order_line_number(*key) as usize];
        g.count += 1;
        g.sum_qty += ol.ol_quantity as u64;
        g.sum_amount += ol.ol_amount;
    }

    let mut out: Vec<_> = groups.into_iter().filter(|g| g.count > 0).collect();
    out.sort_by_key(|g| g.ol_number);
    (out, ts_start)
}

/// CH-benCHmark Q6 ("Forecasting Revenue Change") - see `tpch_queries::q6`'s
/// doc (total revenue from order-lines delivered within `[date_lo, date_hi)`
/// with quantity below `max_qty`). One full `ORDER_LINE` table scan.
fn mdbx_q6(db: &Database<WriteMap>, date_lo: i64, date_hi: i64, max_qty: u8) -> (f64, u64) {
    let txn = db.begin_ro_txn().expect("mdbx_tpcc: begin_ro_txn (q6)");
    let ts_start = txn.id();
    let lines = range_rows(&txn, Table::OrderLine, TpccKey::MIN, TpccKey::MAX);

    let revenue = lines.iter()
        .filter_map(|(_, row)| {
            let ol = row.as_order_line();
            let delivered = ol.ol_delivery_d?;
            (delivered >= date_lo && delivered < date_hi && ol.ol_quantity < max_qty).then_some(ol.ol_amount)
        })
        .sum();
    (revenue, ts_start)
}

// ---------------------------------------------------------------------
// Population (mirrors tpcc_load.rs, minus CH-benCHmark's SUPPLIER/NATION/
// REGION - not needed here, see module docs).
// ---------------------------------------------------------------------

fn populate_warehouse(db: &Database<WriteMap>, cfg: &TpccConfig, w_id: u32, history_seq: &AtomicU64) {
    let txn = db.begin_rw_txn().expect("mdbx_tpcc: begin_rw_txn (load)");

    put_row(&txn, Table::Warehouse, k_warehouse(w_id), &TpccRow::Warehouse(Box::new(Warehouse {
        w_name: rnd_astring(6, 10),
        w_street_1: rnd_astring(10, 20),
        w_street_2: rnd_astring(10, 20),
        w_city: rnd_astring(10, 20),
        w_state: rnd_astring(2, 2),
        w_zip: rnd_zip(),
        w_tax: rand::rng().random_range(0..=2000) as f64 / 10000.0,
        w_ytd: 300_000.0,
    })));

    for d_id in 1..=cfg.districts_per_warehouse {
        put_row(&txn, Table::District, k_district(w_id, d_id), &TpccRow::District(Box::new(District {
            d_name: rnd_astring(6, 10),
            d_street_1: rnd_astring(10, 20),
            d_street_2: rnd_astring(10, 20),
            d_city: rnd_astring(10, 20),
            d_state: rnd_astring(2, 2),
            d_zip: rnd_zip(),
            d_tax: rand::rng().random_range(0..=2000) as f64 / 10000.0,
            d_ytd: 30_000.0,
            d_next_o_id: cfg.initial_orders_per_district + 1,
        })));

        for c_ord in 0..cfg.customers_per_district {
            let c_id = c_ord + 1;
            let last_code = c_last_code_for_load(c_ord);
            let c_last = gen_last_name(last_code);
            let c_first = rnd_astring(8, 16);
            let first_code_v = first_code(&c_first);
            let c_credit_bad = rand::rng().random_range(0..10) == 0;

            put_row(&txn, Table::Customer, k_customer(w_id, d_id, c_id), &TpccRow::Customer(Box::new(Customer {
                c_first,
                c_middle: "OE".to_string(),
                c_last,
                c_street_1: rnd_astring(10, 20),
                c_street_2: rnd_astring(10, 20),
                c_city: rnd_astring(10, 20),
                c_state: rnd_astring(2, 2),
                c_zip: rnd_zip(),
                c_phone: rnd_phone(),
                c_since: now_millis(),
                c_credit_bad,
                c_credit_lim: 50_000.0,
                c_discount: rand::rng().random_range(0..=5000) as f64 / 10000.0,
                c_balance: -10.0,
                c_ytd_payment: 10.0,
                c_payment_cnt: 1,
                c_delivery_cnt: 0,
                c_data: rnd_astring(300, 500),
            })));
            put_row(&txn, Table::CustomerNameIdx, k_customer_name_idx(w_id, d_id, last_code, first_code_v, c_id), &TpccRow::CustomerNameIdx);

            let h_key = k_history(history_seq.fetch_add(1, Relaxed));
            put_row(&txn, Table::History, h_key, &TpccRow::History(Box::new(History {
                h_c_id: c_id,
                h_c_d_id: d_id,
                h_c_w_id: w_id,
                h_d_id: d_id,
                h_w_id: w_id,
                h_date: now_millis(),
                h_amount: 10.0,
                h_data: rnd_astring(12, 24),
            })));
        }

        let mut c_ids: Vec<u32> = (1..=cfg.customers_per_district).collect();
        c_ids.shuffle(&mut rand::rng());

        let new_order_floor = cfg.initial_orders_per_district.saturating_sub(cfg.initial_new_orders);

        for o_ord in 0..cfg.initial_orders_per_district {
            let o_id = o_ord + 1;
            let c_id = c_ids[o_ord as usize];
            let ol_cnt = rand::rng().random_range(5..=15u8);
            let is_new = o_id > new_order_floor;
            let o_carrier_id = if is_new { None } else { Some(rand::rng().random_range(1..=10u32)) };

            put_row(&txn, Table::Orders, k_order(w_id, d_id, o_id), &TpccRow::Order(Box::new(Order {
                o_c_id: c_id,
                o_entry_d: now_millis(),
                o_carrier_id,
                o_ol_cnt: ol_cnt,
                o_all_local: true,
            })));
            put_row(&txn, Table::CustLastOrder, k_cust_last_order(w_id, d_id, c_id), &TpccRow::CustLastOrder(o_id));

            for ol_number in 1..=ol_cnt {
                let i_id = rand::rng().random_range(1..=cfg.num_items);
                let (ol_delivery_d, ol_amount) = if is_new {
                    (None, rand::rng().random_range(100..=999_999) as f64 / 100.0)
                } else {
                    (Some(now_millis()), 0.0)
                };

                put_row(&txn, Table::OrderLine, k_order_line(w_id, d_id, o_id, ol_number), &TpccRow::OrderLine(Box::new(OrderLine {
                    ol_i_id: i_id,
                    ol_supply_w_id: w_id,
                    ol_delivery_d,
                    ol_quantity: 5,
                    ol_amount,
                    ol_dist_info: rnd_astring(24, 24),
                })));
            }

            if is_new {
                put_row(&txn, Table::NewOrder, k_new_order(w_id, d_id, o_id), &TpccRow::NewOrder(NewOrderMarker { no_o_id: o_id }));
            }
        }
    }

    for i_id in 1..=cfg.num_items {
        put_row(&txn, Table::Stock, k_stock(w_id, i_id), &TpccRow::Stock(Box::new(Stock {
            s_quantity: rand::rng().random_range(10..=100),
            s_dist: std::array::from_fn(|_| rnd_astring(24, 24)),
            s_ytd: 0.0,
            s_order_cnt: 0,
            s_remote_cnt: 0,
            s_data: rnd_original_data(26, 50),
            s_su_suppkey: 0,
        })));
    }

    txn.commit().expect("mdbx_tpcc: commit (load warehouse)");
}

fn populate_items(db: &Database<WriteMap>, cfg: &TpccConfig) {
    let txn = db.begin_rw_txn().expect("mdbx_tpcc: begin_rw_txn (load items)");
    for i_id in 1..=cfg.num_items {
        put_row(&txn, Table::Item, k_item(i_id), &TpccRow::Item(Box::new(Item {
            i_im_id: rand::rng().random_range(1..=10_000),
            i_name: rnd_astring(14, 24),
            i_price: rand::rng().random_range(100..=10_000) as f64 / 100.0,
            i_data: rnd_original_data(26, 50),
        })));
    }
    txn.commit().expect("mdbx_tpcc: commit (load items)");
}

// ---------------------------------------------------------------------
// Transactions (mirrors tpcc_txn.rs function-for-function - see module docs
// on why Conflict can't actually happen here, kept only for defensive
// missing-row checks).
// ---------------------------------------------------------------------

fn new_order(db: &Database<WriteMap>, cfg: &TpccConfig, home_w_id: u32) -> TxnOutcome {
    let d_id = rand::rng().random_range(1..=cfg.districts_per_warehouse);
    let c_id = nu_rand_customer_id(cfg.customers_per_district);
    let ol_cnt = rand::rng().random_range(5..=15u8);
    let invalid_line = if rand::rng().random_range(1..=100) == 1 {
        Some(rand::rng().random_range(0..ol_cnt))
    } else {
        None
    };

    struct Line { i_id: u32, qty: u8 }
    let lines: Vec<Line> = (0..ol_cnt).map(|i| {
        let i_id = if Some(i) == invalid_line { cfg.num_items + 1 } else { nu_rand_item_id(cfg.num_items) };
        let qty = rand::rng().random_range(1..=10u8);
        Line { i_id, qty }
    }).collect();

    let txn = db.begin_rw_txn().expect("mdbx_tpcc: begin_rw_txn (new_order)");

    let Some(warehouse) = get_row(&txn, Table::Warehouse, k_warehouse(home_w_id)) else {
        return TxnOutcome::Conflict;
    };
    let w_tax = warehouse.as_warehouse().w_tax;

    let Some(district) = get_row(&txn, Table::District, k_district(home_w_id, d_id)) else {
        return TxnOutcome::Conflict;
    };
    let mut d_row = district.as_district().clone();
    let o_id = d_row.d_next_o_id;
    let d_tax = d_row.d_tax;

    let Some(customer) = get_row(&txn, Table::Customer, k_customer(home_w_id, d_id, c_id)) else {
        return TxnOutcome::Conflict;
    };
    let c_discount = customer.as_customer().c_discount;

    let mut priced = Vec::with_capacity(lines.len());
    for line in &lines {
        match get_row(&txn, Table::Item, k_item(line.i_id)) {
            Some(item) => priced.push((line, item.as_item().i_price)),
            None => return TxnOutcome::UserAbort,
        }
    }

    d_row.d_next_o_id = o_id + 1;
    put_row(&txn, Table::District, k_district(home_w_id, d_id), &TpccRow::District(Box::new(d_row)));

    for (ol_number, (line, i_price)) in priced.into_iter().enumerate() {
        let ol_number = (ol_number + 1) as u8;

        let Some(stock) = get_row(&txn, Table::Stock, k_stock(home_w_id, line.i_id)) else {
            return TxnOutcome::Conflict;
        };
        let mut s_row = stock.as_stock().clone();
        s_row.s_quantity = if s_row.s_quantity - line.qty as i32 >= 10 {
            s_row.s_quantity - line.qty as i32
        } else {
            s_row.s_quantity - line.qty as i32 + 91
        };
        s_row.s_ytd += line.qty as f64;
        s_row.s_order_cnt += 1;
        put_row(&txn, Table::Stock, k_stock(home_w_id, line.i_id), &TpccRow::Stock(Box::new(s_row)));

        let ol_amount = line.qty as f64 * i_price * (1.0 - c_discount) * (1.0 + w_tax + d_tax);
        put_row(&txn, Table::OrderLine, k_order_line(home_w_id, d_id, o_id, ol_number), &TpccRow::OrderLine(Box::new(OrderLine {
            ol_i_id: line.i_id,
            ol_supply_w_id: home_w_id,
            ol_delivery_d: None,
            ol_quantity: line.qty,
            ol_amount,
            ol_dist_info: rnd_astring(24, 24),
        })));
    }

    put_row(&txn, Table::Orders, k_order(home_w_id, d_id, o_id), &TpccRow::Order(Box::new(Order {
        o_c_id: c_id,
        o_entry_d: now_millis(),
        o_carrier_id: None,
        o_ol_cnt: ol_cnt,
        o_all_local: true,
    })));
    put_row(&txn, Table::NewOrder, k_new_order(home_w_id, d_id, o_id), &TpccRow::NewOrder(NewOrderMarker { no_o_id: o_id }));
    put_row(&txn, Table::CustLastOrder, k_cust_last_order(home_w_id, d_id, c_id), &TpccRow::CustLastOrder(o_id));

    txn.commit().expect("mdbx_tpcc: commit (new_order)");
    TxnOutcome::Committed
}

fn payment(db: &Database<WriteMap>, cfg: &TpccConfig, home_w_id: u32, history_seq: &AtomicU64) -> TxnOutcome {
    let d_id = rand::rng().random_range(1..=cfg.districts_per_warehouse);
    let amount = rand::rng().random_range(100..=500_000) as f64 / 100.0;
    let by_last_name = rand::rng().random_range(1..=100) <= 60;

    let txn = db.begin_rw_txn().expect("mdbx_tpcc: begin_rw_txn (payment)");

    let Some(warehouse) = get_row(&txn, Table::Warehouse, k_warehouse(home_w_id)) else {
        return TxnOutcome::Conflict;
    };
    let mut w_row = warehouse.as_warehouse().clone();
    w_row.w_ytd += amount;
    let w_name = w_row.w_name.clone();
    put_row(&txn, Table::Warehouse, k_warehouse(home_w_id), &TpccRow::Warehouse(Box::new(w_row)));

    let Some(district) = get_row(&txn, Table::District, k_district(home_w_id, d_id)) else {
        return TxnOutcome::Conflict;
    };
    let mut d_row = district.as_district().clone();
    d_row.d_ytd += amount;
    let d_name = d_row.d_name.clone();
    put_row(&txn, Table::District, k_district(home_w_id, d_id), &TpccRow::District(Box::new(d_row)));

    let c_id = if by_last_name {
        let last_code = c_last_code_for_run();
        let (lo, hi) = k_customer_name_idx_prefix_bounds(home_w_id, d_id, last_code);
        let mut matches = range_rows(&txn, Table::CustomerNameIdx, lo, hi);
        matches.sort_by_key(|r| r.0);
        if matches.is_empty() {
            return TxnOutcome::UserAbort;
        }
        pick_middle_by_name(&matches)
    } else {
        nu_rand_customer_id(cfg.customers_per_district)
    };

    let Some(customer) = get_row(&txn, Table::Customer, k_customer(home_w_id, d_id, c_id)) else {
        return TxnOutcome::Conflict;
    };
    let mut c_row = customer.as_customer().clone();
    c_row.c_balance -= amount;
    c_row.c_ytd_payment += amount;
    c_row.c_payment_cnt += 1;
    if c_row.c_credit_bad {
        let note = format!("{c_id} {d_id} {home_w_id} {d_id} {home_w_id} {amount:.2} | {}", c_row.c_data);
        c_row.c_data = note.chars().take(500).collect();
    }
    put_row(&txn, Table::Customer, k_customer(home_w_id, d_id, c_id), &TpccRow::Customer(Box::new(c_row)));

    let h_data = format!("{w_name}    {d_name}");
    let h_key = k_history(history_seq.fetch_add(1, Relaxed));
    put_row(&txn, Table::History, h_key, &TpccRow::History(Box::new(History {
        h_c_id: c_id,
        h_c_d_id: d_id,
        h_c_w_id: home_w_id,
        h_d_id: d_id,
        h_w_id: home_w_id,
        h_date: now_millis(),
        h_amount: amount,
        h_data,
    })));

    txn.commit().expect("mdbx_tpcc: commit (payment)");
    TxnOutcome::Committed
}

fn order_status(db: &Database<WriteMap>, cfg: &TpccConfig, home_w_id: u32) -> TxnOutcome {
    let d_id = rand::rng().random_range(1..=cfg.districts_per_warehouse);
    let by_last_name = rand::rng().random_range(1..=100) <= 60;

    let txn = db.begin_ro_txn().expect("mdbx_tpcc: begin_ro_txn (order_status)");

    let c_id = if by_last_name {
        let last_code = c_last_code_for_run();
        let (lo, hi) = k_customer_name_idx_prefix_bounds(home_w_id, d_id, last_code);
        let mut matches = range_rows(&txn, Table::CustomerNameIdx, lo, hi);
        matches.sort_by_key(|r| r.0);
        if matches.is_empty() {
            return TxnOutcome::UserAbort;
        }
        pick_middle_by_name(&matches)
    } else {
        nu_rand_customer_id(cfg.customers_per_district)
    };

    if get_row(&txn, Table::Customer, k_customer(home_w_id, d_id, c_id)).is_none() {
        return TxnOutcome::Conflict;
    }

    let Some(last_order) = get_row(&txn, Table::CustLastOrder, k_cust_last_order(home_w_id, d_id, c_id)) else {
        return TxnOutcome::Committed; // no order yet for this customer
    };
    let o_id = last_order.as_cust_last_order();

    let _order = get_row(&txn, Table::Orders, k_order(home_w_id, d_id, o_id));
    let (lo, hi) = k_order_line_bounds(home_w_id, d_id, o_id);
    let _lines = range_rows(&txn, Table::OrderLine, lo, hi);

    TxnOutcome::Committed
}

struct DeliveryOutcome {
    delivered_districts: u32,
    conflicts: u32,
}

fn delivery(db: &Database<WriteMap>, cfg: &TpccConfig, home_w_id: u32) -> DeliveryOutcome {
    let carrier_id = rand::rng().random_range(1..=10u32);
    let mut out = DeliveryOutcome { delivered_districts: 0, conflicts: 0 };
    let txn = db.begin_rw_txn().expect("mdbx_tpcc: begin_rw_txn (delivery)");

    for d_id in 1..=cfg.districts_per_warehouse {
        match deliver_one_district(&txn, home_w_id, d_id, carrier_id) {
            TxnOutcome::Committed => out.delivered_districts += 1,
            TxnOutcome::Conflict => {
                // Dropping the one shared transaction rolls back every district.
                out.delivered_districts = 0;
                out.conflicts = 1;
                return out;
            }
            TxnOutcome::UserAbort => {}
        }
    }
    txn.commit().expect("mdbx_tpcc: commit (delivery)");
    out
}

fn deliver_one_district(txn: &Transaction<RW, WriteMap>, w_id: u32, d_id: u8, carrier_id: u32) -> TxnOutcome {
    let (lo, hi) = k_new_order_district_bounds(w_id, d_id);
    let mut queued = range_rows(&txn, Table::NewOrder, lo, hi);
    if queued.is_empty() {
        return TxnOutcome::UserAbort;
    }
    queued.sort_by_key(|r| r.0);
    let (oldest_key, oldest_row) = &queued[0];
    let o_id = match oldest_row {
        TpccRow::NewOrder(m) => m.no_o_id,
        _ => unreachable!("NEW_ORDER-range scan returned a non-NewOrder row"),
    };

    if !delete_row(&txn, Table::NewOrder, *oldest_key) {
        return TxnOutcome::Conflict;
    }

    let order_key = k_order(w_id, d_id, o_id);
    let Some(order_row) = get_row(&txn, Table::Orders, order_key) else {
        return TxnOutcome::Conflict;
    };
    let mut order_row = order_row.as_order().clone();
    let c_id = order_row.o_c_id;
    order_row.o_carrier_id = Some(carrier_id);
    put_row(&txn, Table::Orders, order_key, &TpccRow::Order(Box::new(order_row)));

    let (ol_lo, ol_hi) = k_order_line_bounds(w_id, d_id, o_id);
    let lines = range_rows(&txn, Table::OrderLine, ol_lo, ol_hi);
    let mut total = 0.0f64;
    let now = now_millis();
    for (key, row) in &lines {
        let mut ol = row.as_order_line().clone();
        total += ol.ol_amount;
        ol.ol_delivery_d = Some(now);
        put_row(&txn, Table::OrderLine, *key, &TpccRow::OrderLine(Box::new(ol)));
    }

    let cust_key = k_customer(w_id, d_id, c_id);
    let Some(cust_row) = get_row(&txn, Table::Customer, cust_key) else {
        return TxnOutcome::Conflict;
    };
    let mut c_row = cust_row.as_customer().clone();
    c_row.c_balance += total;
    c_row.c_delivery_cnt += 1;
    put_row(&txn, Table::Customer, cust_key, &TpccRow::Customer(Box::new(c_row)));

    TxnOutcome::Committed
}

fn stock_level(db: &Database<WriteMap>, cfg: &TpccConfig, home_w_id: u32, threshold: i32) -> TxnOutcome {
    let d_id = rand::rng().random_range(1..=cfg.districts_per_warehouse);
    let txn = db.begin_ro_txn().expect("mdbx_tpcc: begin_ro_txn (stock_level)");

    let Some(district) = get_row(&txn, Table::District, k_district(home_w_id, d_id)) else {
        return TxnOutcome::Conflict;
    };
    let next_o_id = district.as_district().d_next_o_id;
    let hi_o = next_o_id.saturating_sub(1);
    let lo_o = hi_o.saturating_sub(19).max(1);

    let mut item_ids = std::collections::HashSet::new();
    for o_id in lo_o..=hi_o {
        let (lo, hi) = k_order_line_bounds(home_w_id, d_id, o_id);
        for (_, row) in range_rows(&txn, Table::OrderLine, lo, hi) {
            item_ids.insert(row.as_order_line().ol_i_id);
        }
    }

    let mut low_stock = 0u32;
    for i_id in item_ids {
        if let Some(s) = get_row(&txn, Table::Stock, k_stock(home_w_id, i_id)) {
            if s.as_stock().s_quantity < threshold {
                low_stock += 1;
            }
        }
    }
    let _ = low_stock;

    TxnOutcome::Committed
}

// ---------------------------------------------------------------------
// Driver (mirrors tpcc_driver.rs, minus WAL/affinity/the full multi-query
// OlapMode sweep - see module docs on scope; htap_ch_benchmark is the one
// piece of tpcc_driver.rs's OLAP/HTAP machinery this file does port).
// ---------------------------------------------------------------------

/// One row of `tpcc_scan.csv` - same column shape as `olap_scan::ScanResult`
/// (see `tpcc_driver.rs::write_results`) so `scripts/engines/libmdbx.py`
/// reads it exactly the way `scripts/engines/batstore.py` already reads
/// BatStore's own. `snapshot`/`staleness_versions` use libmdbx's own
/// transaction id (`Transaction::id()`, a real MDBX-internal monotonic
/// counter) in place of BatStore's logical `Version` - same idea (a
/// snapshot's position on the timeline of committed writes), different
/// engine's native counter.
struct MdbxScanResult {
    mode: &'static str,
    elapsed_secs: f64,
    snapshot: u64,
    scanned_tuples: usize,
    latency_ns: u128,
    summary: Option<f64>,
    staleness_versions: u64,
}

/// Runs one selected analytical query and reports an [`MdbxScanResult`]. `staleness`
/// is computed the same way `olap_scan.rs` computes it for BatStore: a fresh
/// read-only transaction opened immediately after the query finishes, diffed
/// against the query's own transaction id - "how many committed writer
/// transactions happened while this analytical answer was being computed."
fn ch_q1_once(db: &Database<WriteMap>, date_hi: i64, run_start: Instant) -> MdbxScanResult {
    let staleness = |ts_start: u64| {
        let fresh = db.begin_ro_txn().expect("mdbx_tpcc: begin_ro_txn (staleness probe)");
        fresh.id().saturating_sub(ts_start)
    };

    let start = Instant::now();
    let (q1, ts_start) = mdbx_q1(db, date_hi);
    MdbxScanResult {
        mode: "ch_q1_pricing_summary",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        snapshot: ts_start,
        scanned_tuples: q1.len(),
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q1.iter().map(|g| g.sum_amount).sum()),
        staleness_versions: staleness(ts_start),
    }
}

fn ch_q6_once(db: &Database<WriteMap>, date_lo: i64, date_hi: i64, run_start: Instant) -> MdbxScanResult {
    let staleness = |ts_start: u64| {
        let fresh = db.begin_ro_txn().expect("mdbx_tpcc: begin_ro_txn (staleness probe)");
        fresh.id().saturating_sub(ts_start)
    };
    let start = Instant::now();
    let (q6, ts_start) = mdbx_q6(db, date_lo, date_hi, 24);
    MdbxScanResult {
        mode: "ch_q6_forecast_revenue",
        elapsed_secs: run_start.elapsed().as_secs_f64(),
        snapshot: ts_start,
        scanned_tuples: 1,
        latency_ns: start.elapsed().as_nanos(),
        summary: Some(q6),
        staleness_versions: staleness(ts_start),
    }
}

/// The one HTAP OLAP thread's whole run - repeats the selected query until `stop`,
/// streaming every completed query's result into the returned `Vec`
/// (joined back in `run_mdbx_tpcc`, mirrors `terminal_thread`'s
/// join-and-collect shape rather than `tpcc_driver.rs`'s channel-based
/// multi-thread fan-in, since there's always exactly one of these).
fn olap_thread(
    db: Arc<Database<WriteMap>>,
    date_lo: i64,
    date_hi: i64,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    mode: MdbxHtapMode,
) -> Vec<MdbxScanResult> {
    barrier.wait();
    let run_start = Instant::now();
    let mut out = Vec::new();

    while !stop.load(Relaxed) {
        out.push(match mode {
            MdbxHtapMode::Q1 => ch_q1_once(&db, date_hi, run_start),
            MdbxHtapMode::Q6 => ch_q6_once(&db, date_lo, date_hi, run_start),
            MdbxHtapMode::None => break,
        });
    }

    out
}

struct TerminalStats {
    new_order_committed_per_sec: Vec<u64>,
    totals: [u64; NUM_COUNTERS],
}

fn terminal_thread(
    db: Arc<Database<WriteMap>>,
    cfg: TpccConfig,
    duration: Duration,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    history_seq: Arc<AtomicU64>,
) -> TerminalStats {
    barrier.wait();

    let mut new_order_committed_per_sec = vec![0u64; duration.as_secs() as usize + 2];
    let mut totals = [0u64; NUM_COUNTERS];
    let start = Instant::now();

    while !stop.load(Relaxed) {
        let home_w = rand::rng().random_range(1..=cfg.num_warehouses);

        match rand::rng().random_range(1..=100u32) {
            1..=45 => {
                let outcome = new_order(&db, &cfg, home_w);
                record(&mut totals, NO, outcome);
                if outcome == TxnOutcome::Committed {
                    let idx = (start.elapsed().as_secs() as usize).min(new_order_committed_per_sec.len() - 1);
                    new_order_committed_per_sec[idx] += 1;
                }
            }
            46..=88 => record(&mut totals, PAY, payment(&db, &cfg, home_w, &history_seq)),
            89..=92 => record(&mut totals, OS, order_status(&db, &cfg, home_w)),
            93..=96 => {
                let d = delivery(&db, &cfg, home_w);
                totals[DELIV_DISTRICTS] += d.delivered_districts as u64;
                totals[DELIV_CONFLICTS] += d.conflicts as u64;
            }
            _ => record(&mut totals, SL, stock_level(&db, &cfg, home_w, 15)),
        }
    }

    TerminalStats { new_order_committed_per_sec, totals }
}

pub fn run_mdbx_tpcc(cfg: MdbxTpccConfig) -> MdbxTpccRunSummary {
    assert!(cfg.tpcc.num_warehouses >= 1, "mdbx_tpcc: num_warehouses must be >= 1");

    fs::create_dir_all(&cfg.output_dir)
        .unwrap_or_else(|e| panic!("mdbx_tpcc: failed to create output_dir {}: {e}", cfg.output_dir.display()));
    let mem_sampler = MemSampler::start(cfg.output_dir.join("mem_stats.csv"), DEFAULT_SAMPLE_INTERVAL);

    let num_terminals = cfg.num_terminals.max(1);
    let db = Arc::new(open_db(&cfg.db_path, num_terminals));

    println!(
        "libmdbx TPC-C benchmark\n\
         - warehouses            = {}\n\
         - terminals (OLTP)      = {num_terminals}\n\
         - duration              = {:?}\n\
         - db_path               = {}\n\
         - items/customers/orders per district = {}/{}/{}",
        cfg.tpcc.num_warehouses, cfg.duration, cfg.db_path.display(),
        cfg.tpcc.num_items, cfg.tpcc.customers_per_district, cfg.tpcc.initial_orders_per_district,
    );

    println!("Loading TPC-C data set...");
    let load_start = Instant::now();
    let history_seq = Arc::new(AtomicU64::new(0));
    populate_items(&db, &cfg.tpcc);
    for w_id in 1..=cfg.tpcc.num_warehouses {
        populate_warehouse(&db, &cfg.tpcc, w_id, &history_seq);
    }
    println!("Loaded {} warehouse(s) in {:?}.", cfg.tpcc.num_warehouses, load_start.elapsed());

    // +1 OLAP thread when htap_ch_benchmark is set - same "every distinct thread
    // permanently owns a barrier slot" shape as tpcc_driver.rs's num_terminals +
    // num_olap_threads (see that module's threading-constraint doc), just fixed at
    // exactly 0 or 1 OLAP threads here (see module docs on scope).
    let has_olap = cfg.htap_mode != MdbxHtapMode::None;
    let barrier = Arc::new(Barrier::new(num_terminals + 1 + has_olap as usize));
    let stop = Arc::new(AtomicBool::new(false));
    let duration = cfg.duration;

    let handles: Vec<_> = (0..num_terminals).map(|_| {
        let db = db.clone();
        let cfg = cfg.tpcc;
        let stop = stop.clone();
        let barrier = barrier.clone();
        let history_seq = history_seq.clone();
        thread::spawn(move || terminal_thread(db, cfg, duration, stop, barrier, history_seq))
    }).collect();

    // Wide-open by default, same as tpcc_driver.rs's "ch" olap_mode: every row
    // loaded gets stamped with the load's own real wall-clock time (see
    // tpcc_random::now_millis), not a simulated TPC-H date range, so an
    // unrestricted [MIN, MAX) filter is what makes Q1/Q6 see the whole loaded
    // data set.
    let olap_handle = has_olap.then(|| {
        let db = db.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        thread::spawn(move || olap_thread(db, i64::MIN, i64::MAX, stop, barrier, cfg.htap_mode))
    });

    barrier.wait();
    let run_start = Instant::now();
    println!("Loading done. Running timed phase for {duration:?}...");
    thread::sleep(duration);
    stop.store(true, Relaxed);

    let stats: Vec<TerminalStats> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let scan_results = olap_handle.map(|h| h.join().unwrap()).unwrap_or_default();
    let actual_wall = run_start.elapsed();

    mem_sampler.stop();

    write_results(&stats, &scan_results, duration, actual_wall, &cfg.output_dir)
}

fn write_results(stats: &[TerminalStats], scan_results: &[MdbxScanResult], requested_duration: Duration, actual_wall: Duration, out_dir: &std::path::Path) -> MdbxTpccRunSummary {
    let series_len = requested_duration.as_secs() as usize + 2;
    let mut per_sec = vec![0u64; series_len];
    let mut totals = [0u64; NUM_COUNTERS];
    for t in stats {
        for (i, v) in t.new_order_committed_per_sec.iter().enumerate() {
            per_sec[i] += v;
        }
        for i in 0..NUM_COUNTERS {
            totals[i] += t.totals[i];
        }
    }

    let oltp_ts_path = out_dir.join("tpcc_oltp_timeseries.csv");
    let _ = fs::remove_file(&oltp_ts_path);
    let mut ts_file = OpenOptions::new().create(true).append(true).open(&oltp_ts_path).unwrap();
    ts_file.write_all(b"elapsed_sec,new_order_committed\n").unwrap();
    for (sec, count) in per_sec.iter().enumerate() {
        ts_file.write_all(format!("{sec},{count}\n").as_bytes()).unwrap();
    }

    // Same column shape as tpcc_driver.rs::write_results's tpcc_scan.csv (see
    // MdbxScanResult's doc) - only written when the OLAP thread actually ran, so plain
    // tpcc/ycsb_* runs don't leave a stale/empty file behind from a previous htap run
    // reusing the same output_dir.
    if !scan_results.is_empty() {
        let scan_path = out_dir.join("tpcc_scan.csv");
        let _ = fs::remove_file(&scan_path);
        let mut scan_file = OpenOptions::new().create(true).append(true).open(&scan_path).unwrap();
        scan_file.write_all(b"mode,elapsed_secs,delay_secs,snapshot,scanned_tuples,latency_ns,tuples_per_sec,summary,staleness_versions\n").unwrap();
        for r in scan_results {
            let tuples_per_sec = if r.latency_ns == 0 { 0.0 } else { r.scanned_tuples as f64 / (r.latency_ns as f64 / 1e9) };
            scan_file.write_all(format!(
                "{},{:.3},{},{},{},{},{:.2},{},{}\n",
                r.mode, r.elapsed_secs, 0.0, r.snapshot, r.scanned_tuples, r.latency_ns, tuples_per_sec,
                r.summary.map(|s| format!("{s:.2}")).unwrap_or_default(),
                r.staleness_versions,
            ).as_bytes()).unwrap();
        }
        println!("Wrote {}", scan_path.display());
    }

    let new_order_total = totals[NO];
    let tpm_c = new_order_total as f64 / (actual_wall.as_secs_f64() / 60.0);

    println!("\n===== Results (timed phase: {actual_wall:?}) =====");
    for i in 0..NUM_COUNTERS {
        println!("{:<32} {}", COUNTER_NAMES[i], totals[i]);
    }
    println!("{:<32} {:.2}", "tpmC (New-Order/min)", tpm_c);
    println!("Wrote {}", oltp_ts_path.display());

    MdbxTpccRunSummary { tpm_c, totals }
}

pub fn main_mdbx_tpcc(parms: Vec<String>) {
    fn arg<T: std::str::FromStr>(parms: &[String], idx: usize, default: T) -> T {
        parms.get(idx).and_then(|s| s.parse().ok()).unwrap_or(default)
    }

    // Positional order mirrors the existing `tpcc` subcommand (main_tpcc) wherever the
    // concept overlaps, dropping every knob that has no libmdbx equivalent (affinity, gc,
    // update_in_place, root_star_index, WAL) - see mdbx_ycsb.rs/
    // scripts/engines/libmdbx.py for the same convention. `htap_mode` (position 9) is the
    // one piece of tpcc_driver.rs's OLAP/HTAP surface this file does port - "none"
    // (default), "ch_q1", or "ch_q6" - dropping
    // tpcc_driver.rs's other olap_mode_str variants (sleep/fresh/scan-delay-sweep) and
    // ch's own region_name/num_suppliers knobs, neither of which apply to Q1/Q6.
    let num_warehouses: u32 = arg(&parms, 2, 4);
    let num_terminals: usize = arg(&parms, 3, num_cpus::get());
    let duration_secs: u64 = arg(&parms, 4, 30);
    let num_items: u32 = arg(&parms, 5, 100_000);
    let customers_per_district: u32 = arg(&parms, 6, 3_000);
    let initial_orders_per_district: u32 = arg(&parms, 7, 3_000);
    let db_path: String = parms.get(8).cloned().unwrap_or_else(|| "mdbx_tpcc_db".to_string());
    let htap_mode = match parms.get(9).map(|s| s.as_str()).unwrap_or("none") {
        "ch_q1" => MdbxHtapMode::Q1,
        "ch_q6" => MdbxHtapMode::Q6,
        _ => MdbxHtapMode::None,
    };

    run_mdbx_tpcc(MdbxTpccConfig {
        tpcc: TpccConfig {
            num_warehouses,
            districts_per_warehouse: 10,
            customers_per_district,
            num_items,
            initial_orders_per_district,
            initial_new_orders: (initial_orders_per_district * 3 / 10).max(1),
            num_suppliers: 0,
        },
        htap_mode,
        num_terminals,
        duration: Duration::from_secs(duration_secs),
        db_path: PathBuf::from(db_path),
        output_dir: PathBuf::from("."),
    });
}
