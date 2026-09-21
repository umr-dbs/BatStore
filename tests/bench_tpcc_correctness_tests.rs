//! End-to-end correctness checks for the TPC-C benchmark harness
//! (`bat_bench::tpcc_load`/`tpcc_txn`): load a tiny (single-warehouse,
//! single-district) data set, run a handful of the standard transaction
//! profiles through the same driver functions the real benchmark uses, and
//! verify the resulting data is actually correct — not just "didn't panic".
//!
//! Every one of `tpcc_txn`'s five profiles picks its target row(s) with
//! internal, unseeded randomness (`rand::rng()`), so these tests can't
//! predict *which* row a given call touches. Instead they check invariants
//! that must hold regardless of which row was picked: e.g. every committed
//! Payment must move `c_balance` and `c_ytd_payment` by exactly opposite
//! amounts, and the warehouse's `w_ytd` growth must equal the sum of those
//! amounts. Tying two or more tables together this way catches the same
//! class of bug (an amount/counter applied to the wrong row, or only half
//! of a cross-table update landing) that a hard-coded expected value would,
//! without needing to fix the RNG.
//!
//! Single warehouse/district and no remote ops (`allow_remote: false`) so
//! every call in a given test targets the same, known warehouse/district —
//! and, since there's exactly one thread here, no call can ever lose a
//! first-writer-wins race (`TxnOutcome::Conflict` is always a bug in these
//! tests, not an expected outcome).

use std::sync::atomic::AtomicU64;

use crate::bat_bench::tpcc_load::{populate_items, populate_warehouse};
use crate::bat_bench::tpcc_schema::{
    Table, TpccConfig, TpccDatabase, TpccKey, TpccRow, k_customer, k_district,
    k_new_order_district_bounds, k_order, k_order_line_bounds, k_warehouse,
};
use crate::bat_bench::tpcc_txn::{self, TpccTxn, TxnOutcome, many, one};
use crate::bat_query::interval::Interval;
use crate::bat_record_model::record_point::RecordPointResult;
use crate::bat_root::index_root::RootIndexType;

/// Small enough that loading + a dozen transactions finishes in well under a
/// second, large enough that every transaction profile has real rows to
/// work with (a handful of districts'/customers' worth).
fn tiny_cfg() -> TpccConfig {
    TpccConfig {
        num_warehouses: 1,
        districts_per_warehouse: 1,
        customers_per_district: 20,
        num_items: 30,
        initial_orders_per_district: 15,
        initial_new_orders: 5,
        num_suppliers: 1,
    }
}

fn full_range() -> Interval<TpccKey> {
    Interval::new(TpccKey::MIN, TpccKey::MAX)
}

fn scan_all(db: &TpccDatabase, table: Table) -> Vec<RecordPointResult<TpccKey, TpccRow>> {
    let mut tx = TpccTxn::begin(db);
    let rows = many(tx.range(table, full_range(), true));
    tx.commit();
    rows
}

fn district_next_o_id(db: &TpccDatabase) -> u32 {
    let mut tx = TpccTxn::begin(db);
    let v = one(tx.point(Table::District, k_district(1, 1)))
        .expect("district exists")
        .payload
        .as_district()
        .d_next_o_id;
    tx.commit();
    v
}

fn warehouse_ytd(db: &TpccDatabase) -> f64 {
    let mut tx = TpccTxn::begin(db);
    let v = one(tx.point(Table::Warehouse, k_warehouse(1)))
        .expect("warehouse exists")
        .payload
        .as_warehouse()
        .w_ytd;
    tx.commit();
    v
}

fn customer_balance_and_ytd(db: &TpccDatabase, c_id: u32) -> (f64, f64) {
    let mut tx = TpccTxn::begin(db);
    let c = one(tx.point(Table::Customer, k_customer(1, 1, c_id))).expect("customer exists");
    let v = (
        c.payload.as_customer().c_balance,
        c.payload.as_customer().c_ytd_payment,
    );
    tx.commit();
    v
}

fn setup() -> (TpccConfig, TpccDatabase, AtomicU64) {
    let cfg = tiny_cfg();
    let db = TpccDatabase::new(RootIndexType::default());
    populate_items(&db, &cfg);
    let history_seq = AtomicU64::new(0);
    populate_warehouse(&db, &cfg, 1, &history_seq);
    (cfg, db, history_seq)
}

#[test]
fn new_order_keeps_district_counter_stock_and_order_lines_consistent() {
    let (cfg, db, _history_seq) = setup();

    let d_next_o_id_before = district_next_o_id(&db);
    let orders_before = scan_all(&db, Table::Orders).len();
    let new_order_before = scan_all(&db, Table::NewOrder).len();

    let order_lines_before = scan_all(&db, Table::OrderLine);
    let qty_before: u64 = order_lines_before
        .iter()
        .map(|r| r.payload.as_order_line().ol_quantity as u64)
        .sum();
    let ol_count_before = order_lines_before.len();

    let stock_before = scan_all(&db, Table::Stock);
    let ytd_before: f64 = stock_before
        .iter()
        .map(|r| r.payload.as_stock().s_ytd)
        .sum();
    let order_cnt_before: u64 = stock_before
        .iter()
        .map(|r| r.payload.as_stock().s_order_cnt as u64)
        .sum();

    let mut committed: u32 = 0;
    for _ in 0..8 {
        match tpcc_txn::new_order(&db, &cfg, 1, false) {
            TxnOutcome::Committed => committed += 1,
            // Spec's own ~1% intentional rollback on an invalid item id.
            TxnOutcome::UserAbort => {}
            TxnOutcome::Conflict => {
                panic!("single-threaded run: nothing to lose a first-writer-wins race against")
            }
        }
    }

    assert_eq!(
        district_next_o_id(&db) - d_next_o_id_before,
        committed,
        "one district-counter bump per committed New-Order"
    );
    assert_eq!(
        scan_all(&db, Table::Orders).len() - orders_before,
        committed as usize,
        "one Orders row per committed New-Order"
    );
    assert_eq!(
        scan_all(&db, Table::NewOrder).len() - new_order_before,
        committed as usize,
        "one NewOrder row per committed New-Order"
    );

    let order_lines_after = scan_all(&db, Table::OrderLine);
    let qty_after: u64 = order_lines_after
        .iter()
        .map(|r| r.payload.as_order_line().ol_quantity as u64)
        .sum();
    let stock_after = scan_all(&db, Table::Stock);
    let ytd_after: f64 = stock_after.iter().map(|r| r.payload.as_stock().s_ytd).sum();
    let order_cnt_after: u64 = stock_after
        .iter()
        .map(|r| r.payload.as_stock().s_order_cnt as u64)
        .sum();

    assert!(
        (ytd_after - ytd_before - (qty_after - qty_before) as f64).abs() < 1e-9,
        "total stock s_ytd growth must equal the total order-line quantity just inserted"
    );
    assert_eq!(
        order_cnt_after - order_cnt_before,
        (order_lines_after.len() - ol_count_before) as u64,
        "every new order-line must bump its stock row's s_order_cnt by exactly 1"
    );
}

#[test]
fn payment_moves_matching_amounts_across_customer_history_and_warehouse() {
    let (cfg, db, history_seq) = setup();

    let balances_before: Vec<(f64, f64)> = (1..=cfg.customers_per_district)
        .map(|c| customer_balance_and_ytd(&db, c))
        .collect();
    let w_ytd_before = warehouse_ytd(&db);
    let history_before = scan_all(&db, Table::History).len();

    let mut committed: u32 = 0;
    for i in 0..12 {
        match tpcc_txn::payment(&db, &cfg, 1, false, &history_seq) {
            TxnOutcome::Committed => committed += 1,
            TxnOutcome::UserAbort => {}
            TxnOutcome::Conflict => panic!(
                "iteration {i}: single-threaded run: nothing to lose a first-writer-wins race against"
            ),
        }
    }

    assert_eq!(
        scan_all(&db, Table::History).len() - history_before,
        committed as usize,
        "one History row per committed Payment, none for an aborted one"
    );

    let balances_after: Vec<(f64, f64)> = (1..=cfg.customers_per_district)
        .map(|c| customer_balance_and_ytd(&db, c))
        .collect();
    let mut total_amount_applied = 0.0f64;
    for ((b0, y0), (b1, y1)) in balances_before.iter().zip(&balances_after) {
        let balance_delta = b1 - b0;
        let ytd_delta = y1 - y0;
        assert!(
            (balance_delta + ytd_delta).abs() < 1e-9,
            "c_balance must move by exactly -1x whatever c_ytd_payment moved by"
        );
        total_amount_applied += ytd_delta;
    }

    assert!(
        (warehouse_ytd(&db) - w_ytd_before - total_amount_applied).abs() < 1e-6,
        "warehouse w_ytd growth must equal the total payment amount actually applied to customers"
    );
}

#[test]
fn delivery_dequeues_oldest_new_order_and_credits_the_right_customer_by_the_right_amount() {
    let (cfg, db, _history_seq) = setup();

    let (lo, hi) = k_new_order_district_bounds(1, 1);
    let oldest = {
        let mut tx = TpccTxn::begin(&db);
        let r = tx
            .range_min(Table::NewOrder, Interval::new(lo, hi))
            .expect("tiny cfg always seeds at least one queued new-order");
        tx.commit();
        r
    };
    let o_id = match &*oldest.payload {
        TpccRow::NewOrder(m) => m.no_o_id,
        _ => unreachable!("NewOrder-table scan returned a non-NewOrder row"),
    };

    let (c_id, expected_credit) = {
        let mut tx = TpccTxn::begin(&db);
        let order = one(tx.point(Table::Orders, k_order(1, 1, o_id)))
            .expect("order exists for its own queued new-order");
        let c_id = order.payload.as_order().o_c_id;
        let (ol_lo, ol_hi) = k_order_line_bounds(1, 1, o_id);
        let total: f64 = many(tx.range(Table::OrderLine, Interval::new(ol_lo, ol_hi), true))
            .iter()
            .map(|l| l.payload.as_order_line().ol_amount)
            .sum();
        tx.commit();
        (c_id, total)
    };
    let (balance_before, _) = customer_balance_and_ytd(&db, c_id);

    let new_order_before = scan_all(&db, Table::NewOrder).len();
    let out = tpcc_txn::delivery(&db, &cfg, 1);
    assert_eq!(
        (out.delivered_districts, out.empty_districts, out.conflicts),
        (1, 0, 0),
        "single district, a new-order was queued, single-threaded: must deliver cleanly"
    );
    assert_eq!(
        new_order_before - scan_all(&db, Table::NewOrder).len(),
        1,
        "exactly the delivered order's NewOrder row must be gone"
    );

    let mut tx = TpccTxn::begin(&db);
    let order = one(tx.point(Table::Orders, k_order(1, 1, o_id))).unwrap();
    assert!(
        order.payload.as_order().o_carrier_id.is_some(),
        "delivered order must have a carrier assigned"
    );
    let (ol_lo, ol_hi) = k_order_line_bounds(1, 1, o_id);
    for line in many(tx.range(Table::OrderLine, Interval::new(ol_lo, ol_hi), true)) {
        assert!(
            line.payload.as_order_line().ol_delivery_d.is_some(),
            "every order-line of a delivered order must be stamped delivered"
        );
    }
    tx.commit();

    let (balance_after, _) = customer_balance_and_ytd(&db, c_id);
    assert!(
        (balance_after - balance_before - expected_credit).abs() < 1e-9,
        "customer balance must grow by exactly the sum of the delivered order's own order-line amounts"
    );
}

/// Order-Status and Stock-Level (spec §2.6/§2.8) are documented read-only —
/// running several of each must not change the row count of any of the 14
/// tables.
#[test]
fn order_status_and_stock_level_never_mutate_any_table() {
    let (cfg, db, _history_seq) = setup();

    let before: Vec<usize> = Table::ALL.iter().map(|&t| scan_all(&db, t).len()).collect();

    for _ in 0..5 {
        assert_ne!(
            tpcc_txn::order_status(&db, &cfg, 1),
            TxnOutcome::Conflict,
            "single-threaded run: nothing to conflict with"
        );
    }
    for _ in 0..5 {
        assert_eq!(
            tpcc_txn::stock_level(&db, &cfg, 1, 50),
            TxnOutcome::Committed
        );
    }

    let after: Vec<usize> = Table::ALL.iter().map(|&t| scan_all(&db, t).len()).collect();
    assert_eq!(
        before, after,
        "read-only transactions must not add or remove rows in any table"
    );
}
