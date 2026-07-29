//! Correctness checks for the CH-benCHmark-style analytical queries in
//! `mv_bench::tpch_queries` (q1/q4/q5/q6).
//!
//! Unlike the transaction-profile tests (`bench_tpcc_correctness_tests.rs`),
//! these queries have no internal randomness at all — they're pure scans
//! plus in-memory grouping over whatever rows are in the tables. So instead
//! of checking cross-table invariants, each test here inserts a small,
//! fully-known fixture directly (bypassing the randomized `tpcc_load`
//! population) and asserts on the exact, hand-computed expected result —
//! the strongest correctness check available when the input is fully under
//! the test's control.

use crate::mv_bench::tpcc_load::populate_regions_and_nations;
use crate::mv_bench::tpcc_schema::{
    k_order, k_order_line, k_stock, Order, OrderLine, Stock, Supplier, Table, TpccDatabase, TpccRow,
};
use crate::mv_bench::tpch_queries::{q1, q4, q5, q6};
use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_root::index_root::RootIndexType;

fn insert(db: &TpccDatabase, table: Table, key: u64, row: TpccRow) {
    match db.tree_for(table).dispatch_crud(CRUDOperation::Insert(key, row)) {
        CRUDOperationResult::Inserted(_) => {}
        other => panic!("tpch fixture: unexpected insert result for key {key}: {other}"),
    }
}

fn order_line(ol_i_id: u32, ol_supply_w_id: u32, ol_delivery_d: Option<i64>, ol_quantity: u8, ol_amount: f64) -> TpccRow {
    TpccRow::OrderLine(Box::new(OrderLine { ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info: String::new() }))
}

fn order(o_c_id: u32, o_entry_d: i64, o_ol_cnt: u8) -> TpccRow {
    TpccRow::Order(Box::new(Order { o_c_id, o_entry_d, o_carrier_id: None, o_ol_cnt, o_all_local: true }))
}

/// q1 groups delivered-before-cutoff order-lines by their `ol_number`
/// position within the order, summing count/quantity/amount.
#[test]
fn q1_groups_and_sums_delivered_order_lines_by_ol_number() {
    let db = TpccDatabase::new(RootIndexType::default());

    insert(&db, Table::OrderLine, k_order_line(1, 1, 1, 1), order_line(1, 1, Some(50), 5, 100.0));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 1, 2), order_line(2, 1, Some(50), 3, 60.0));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 2, 1), order_line(1, 1, Some(50), 7, 140.0));
    // Delivered after the cutoff: excluded even though it's ol_number 2.
    insert(&db, Table::OrderLine, k_order_line(1, 1, 2, 2), order_line(2, 1, Some(150), 2, 999.0));
    // Never delivered: excluded regardless of cutoff.
    insert(&db, Table::OrderLine, k_order_line(1, 1, 3, 1), order_line(1, 1, None, 1, 10.0));

    let (groups, _ts) = q1(&db, 100);

    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].ol_number, 1);
    assert_eq!(groups[0].count, 2);
    assert_eq!(groups[0].sum_qty, 12);
    assert!((groups[0].sum_amount - 240.0).abs() < 1e-9);
    assert!((groups[0].avg_qty() - 6.0).abs() < 1e-9);
    assert!((groups[0].avg_amount() - 120.0).abs() < 1e-9);

    assert_eq!(groups[1].ol_number, 2);
    assert_eq!(groups[1].count, 1);
    assert_eq!(groups[1].sum_qty, 3);
    assert!((groups[1].sum_amount - 60.0).abs() < 1e-9);
}

/// q6 sums the amount of delivered-in-range order-lines below a quantity
/// threshold.
#[test]
fn q6_sums_revenue_for_delivered_low_quantity_lines_in_date_range() {
    let db = TpccDatabase::new(RootIndexType::default());

    insert(&db, Table::OrderLine, k_order_line(1, 1, 1, 1), order_line(1, 1, Some(50), 5, 100.0));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 1, 2), order_line(2, 1, Some(50), 3, 60.0));
    // Quantity 7 >= max_qty(6): excluded despite being in range.
    insert(&db, Table::OrderLine, k_order_line(1, 1, 2, 1), order_line(1, 1, Some(50), 7, 140.0));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 2, 2), order_line(2, 1, Some(150), 2, 999.0));
    // Never delivered: excluded regardless of date/quantity.
    insert(&db, Table::OrderLine, k_order_line(1, 1, 3, 1), order_line(1, 1, None, 1, 10.0));

    let (revenue, _ts) = q6(&db, 0, 200, 6);
    assert!((revenue - 1159.0).abs() < 1e-9, "expected 100 + 60 + 999, got {revenue}");
}

/// q4 counts, grouped by `o_ol_cnt`, orders entered in range that have at
/// least one order-line either delivered later than `o_entry_d +
/// late_slack_millis` or never delivered at all.
#[test]
fn q4_counts_late_orders_in_date_range_grouped_by_line_count() {
    let db = TpccDatabase::new(RootIndexType::default());

    // Order A: entry 100, one line delivered far past its slack -> late.
    insert(&db, Table::Orders, k_order(1, 1, 10), order(1, 100, 2));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 10, 1), order_line(1, 1, Some(5_100), 5, 1.0));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 10, 2), order_line(2, 1, Some(600), 5, 1.0));

    // Order B: entry 150, both lines well within slack -> not late.
    insert(&db, Table::Orders, k_order(1, 1, 11), order(2, 150, 2));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 11, 1), order_line(1, 1, Some(650), 5, 1.0));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 11, 2), order_line(2, 1, Some(650), 5, 1.0));

    // Order C: entry 9999, outside [date_lo, date_hi) -> excluded regardless of its lines.
    insert(&db, Table::Orders, k_order(1, 1, 12), order(3, 9_999, 2));

    // Order D: entry 120, one line never delivered -> counts as late.
    insert(&db, Table::Orders, k_order(1, 1, 13), order(4, 120, 2));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 13, 1), order_line(1, 1, None, 5, 1.0));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 13, 2), order_line(2, 1, Some(1_000), 5, 1.0));

    let (counts, _ts) = q4(&db, 0, 200, 1_000);

    assert_eq!(counts.len(), 1, "only orders A and D are late, both with o_ol_cnt=2; B isn't late, C is out of range");
    assert_eq!(counts[0].o_ol_cnt, 2);
    assert_eq!(counts[0].order_count, 2);
}

/// q5 joins ORDERS -> ORDER_LINE -> STOCK -> SUPPLIER -> NATION -> REGION,
/// summing revenue per supplying nation for orders in range, filtered to
/// suppliers within the requested region.
#[test]
fn q5_attributes_revenue_to_the_supplying_nation_within_the_requested_region() {
    let db = TpccDatabase::new(RootIndexType::default());
    // Fixed, deterministic REGION/NATION reference data (see
    // `tpcc_load::populate_regions_and_nations`'s doc): index 6 is FRANCE
    // (regionkey 3 = EUROPE), index 8 is INDIA (regionkey 2 = ASIA).
    populate_regions_and_nations(&db);

    insert(&db, Table::Supplier, 0, TpccRow::Supplier(Box::new(Supplier {
        s_name: "Supplier#FR".into(), s_address: String::new(), s_nationkey: 6,
        s_phone: String::new(), s_acctbal: 0.0, s_comment: String::new(),
    })));
    insert(&db, Table::Supplier, 1, TpccRow::Supplier(Box::new(Supplier {
        s_name: "Supplier#IN".into(), s_address: String::new(), s_nationkey: 8,
        s_phone: String::new(), s_acctbal: 0.0, s_comment: String::new(),
    })));

    insert(&db, Table::Stock, k_stock(1, 1), TpccRow::Stock(Box::new(Stock {
        s_quantity: 50, s_dist: std::array::from_fn(|_| String::new()), s_ytd: 0.0,
        s_order_cnt: 0, s_remote_cnt: 0, s_data: String::new(), s_su_suppkey: 0, // French supplier
    })));
    insert(&db, Table::Stock, k_stock(1, 2), TpccRow::Stock(Box::new(Stock {
        s_quantity: 50, s_dist: std::array::from_fn(|_| String::new()), s_ytd: 0.0,
        s_order_cnt: 0, s_remote_cnt: 0, s_data: String::new(), s_su_suppkey: 1, // Indian supplier
    })));

    insert(&db, Table::Orders, k_order(1, 1, 1), order(1, 100, 2));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 1, 1), order_line(1, 1, Some(100), 1, 50.0));
    insert(&db, Table::OrderLine, k_order_line(1, 1, 1, 2), order_line(2, 1, Some(100), 1, 30.0));

    let (europe, _ts) = q5(&db, "EUROPE", 0, 200);
    assert_eq!(europe.len(), 1, "only the French-supplied line should count towards EUROPE");
    assert_eq!(europe[0].n_name, "FRANCE");
    assert!((europe[0].revenue - 50.0).abs() < 1e-9);

    let (asia, _ts) = q5(&db, "ASIA", 0, 200);
    assert_eq!(asia.len(), 1);
    assert_eq!(asia[0].n_name, "INDIA");
    assert!((asia[0].revenue - 30.0).abs() < 1e-9);

    let (unknown_region, _ts) = q5(&db, "NO SUCH REGION", 0, 200);
    assert!(unknown_region.is_empty());
}

// Sanity-check the fixed REGION/NATION table itself, since q5's expected
// values above depend on FRANCE/INDIA's exact regionkeys staying put.
#[test]
fn region_and_nation_reference_data_has_the_expected_fixed_mapping() {
    let db = TpccDatabase::new(RootIndexType::default());
    populate_regions_and_nations(&db);

    let tx = crate::mv_bench::tpcc_txn::TpccTxn::begin(&db);
    let regions = crate::mv_bench::tpcc_txn::many(tx.range(Table::Region, crate::mv_bench::tpcc_schema::region_table_range(), true));
    let nations = crate::mv_bench::tpcc_txn::many(tx.range(Table::Nation, crate::mv_bench::tpcc_schema::nation_table_range(), true));
    tx.commit();

    assert_eq!(regions.len(), 5);
    assert_eq!(nations.len(), 25);

    let region_name_at = |id: u8| -> String {
        regions.iter().find(|r| crate::mv_bench::tpcc_schema::decode_region_id(r.key) == id).unwrap().payload.as_region().r_name.clone()
    };
    let nation_at = |id: u8| -> (String, u8) {
        let n = nations.iter().find(|n| crate::mv_bench::tpcc_schema::decode_nation_id(n.key) == id).unwrap();
        (n.payload.as_nation().n_name.clone(), n.payload.as_nation().n_regionkey)
    };

    assert_eq!(region_name_at(3), "EUROPE");
    assert_eq!(region_name_at(2), "ASIA");
    assert_eq!(nation_at(6), ("FRANCE".to_string(), 3));
    assert_eq!(nation_at(8), ("INDIA".to_string(), 2));
}
