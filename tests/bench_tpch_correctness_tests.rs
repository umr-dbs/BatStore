//! Correctness checks for the CH-benCHmark-style analytical queries in
//! `bat_bench::tpch_queries` (q1/q4/q5/q6).
//!
//! Unlike the transaction-profile tests (`bench_tpcc_correctness_tests.rs`),
//! these queries have no internal randomness at all — they're pure scans
//! plus in-memory grouping over whatever rows are in the tables. So instead
//! of checking cross-table invariants, each test here inserts a small,
//! fully-known fixture directly (bypassing the randomized `tpcc_load`
//! population) and asserts on the exact, hand-computed expected result —
//! the strongest correctness check available when the input is fully under
//! the test's control.

use crate::bat_bench::parallel_scan::{q1_parallel, q6_parallel};
use crate::bat_bench::tpcc_load::populate_regions_and_nations;
use crate::bat_bench::tpcc_schema::TpccScanWorkerPool as ScanWorkerPool;
use crate::bat_bench::tpcc_schema::{
    BENCHBASE_Q1_DELIVERY_AFTER_MILLIS, BENCHBASE_Q6_DATE_HI_MILLIS, BENCHBASE_Q6_DATE_LO_MILLIS,
    BigTreeSize, Order, OrderLine, Stock, Supplier, Table, TpccDatabase, TpccKey, TpccRow,
    TpccTree, k_order, k_order_line, k_stock, order_line_table_range,
};
use crate::bat_bench::tpcc_txn::TpccTxn;
use crate::bat_bench::tpch_queries::{q1, q1_benchbase, q4, q5, q6, q6_benchbase};
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_sync::worker::READ_ONLY_SCAN_WORKER_ID;
use crate::bat_tree::scan_pool::DEFAULT_QUERY_FANOUT;
use std::time::{Duration, Instant};

fn insert(db: &TpccDatabase, table: Table, key: u64, row: TpccRow) {
    match db
        .tree_for(table)
        .dispatch_crud(CRUDOperation::Insert(key, row))
    {
        CRUDOperationResult::Inserted(_) => {}
        other => panic!("tpch fixture: unexpected insert result for key {key}: {other}"),
    }
}

fn order_line(
    ol_i_id: u32,
    ol_supply_w_id: u32,
    ol_delivery_d: Option<i64>,
    ol_quantity: u8,
    ol_amount: f64,
) -> TpccRow {
    TpccRow::OrderLine(Box::new(OrderLine {
        ol_i_id,
        ol_supply_w_id,
        ol_delivery_d,
        ol_quantity,
        ol_amount,
        ol_dist_info: [0u8; 24],
    }))
}

fn order(o_c_id: u32, o_entry_d: i64, o_ol_cnt: u8) -> TpccRow {
    TpccRow::Order(Box::new(Order {
        o_c_id,
        o_entry_d,
        o_carrier_id: None,
        o_ol_cnt,
        o_all_local: true,
    }))
}

/// q1 groups delivered-before-cutoff order-lines by their `ol_number`
/// position within the order, summing count/quantity/amount.
#[test]
fn q1_groups_and_sums_delivered_order_lines_by_ol_number() {
    let db = TpccDatabase::new(RootIndexType::default());

    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 1, 1),
        order_line(1, 1, Some(50), 5, 100.0),
    );
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 1, 2),
        order_line(2, 1, Some(50), 3, 60.0),
    );
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 2, 1),
        order_line(1, 1, Some(50), 7, 140.0),
    );
    // Delivered after the cutoff: excluded even though it's ol_number 2.
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 2, 2),
        order_line(2, 1, Some(150), 2, 999.0),
    );
    // Never delivered: excluded regardless of cutoff.
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 3, 1),
        order_line(1, 1, None, 1, 10.0),
    );

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

#[test]
fn q1_includes_a_row_delivered_exactly_on_the_inclusive_cutoff() {
    let db = TpccDatabase::new(RootIndexType::default());

    // The earliest representable date: must not be excluded by the
    // encoded lower bound (`encode_signed_zone_value(i64::MIN)`).
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 1, 1),
        order_line(1, 1, Some(i64::MIN), 1, 1.0),
    );
    // Exactly at the cutoff: included (`<=`, not `<`).
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 2, 1),
        order_line(1, 1, Some(100), 1, 10.0),
    );
    // One past the cutoff: excluded.
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 3, 1),
        order_line(1, 1, Some(101), 1, 1_000_000.0),
    );

    let (groups, _ts) = q1(&db, 100);
    let total_count: u64 = groups.iter().map(|g| g.count).sum();
    let total_amount: f64 = groups.iter().map(|g| g.sum_amount).sum();

    assert_eq!(
        total_count, 2,
        "expected exactly the MIN-date and on-cutoff rows"
    );
    assert!(
        (total_amount - 11.0).abs() < 1e-9,
        "expected 1.0 + 10.0 = 11.0, got {total_amount}"
    );

    db.enable_scan_pool(Table::OrderLine, 2, None);
    let pool = db.scan_pool(Table::OrderLine).unwrap();
    let (groups_parallel, _ts) = q1_parallel(&db, &pool, 1, 100);
    let total_count_parallel: u64 = groups_parallel.iter().map(|g| g.count).sum();
    assert_eq!(
        total_count_parallel, 2,
        "q1_parallel disagreed with q1 on the inclusive cutoff"
    );
    db.disable_scan_pool(Table::OrderLine);
}

#[test]
fn benchbase_q1_uses_the_strict_fixed_cutoff_with_zone_pruning() {
    let db = TpccDatabase::new(RootIndexType::default());
    for (order_id, delivered, amount) in [
        (1, BENCHBASE_Q1_DELIVERY_AFTER_MILLIS, 1_000_000.0),
        (2, BENCHBASE_Q1_DELIVERY_AFTER_MILLIS + 1, 7.0),
    ] {
        insert(
            &db,
            Table::OrderLine,
            k_order_line(1, 1, order_id, 1),
            order_line(1, 1, Some(delivered), 3, amount),
        );
    }

    let (groups, _) = q1_benchbase(&db);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].count, 1);
    assert!((groups[0].sum_amount - 7.0).abs() < 1e-9);
}

/// q6 sums the amount of delivered-in-range order-lines below a quantity
/// threshold.
#[test]
fn q6_sums_revenue_for_delivered_low_quantity_lines_in_date_range() {
    let db = TpccDatabase::new(RootIndexType::default());

    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 1, 1),
        order_line(1, 1, Some(50), 5, 100.0),
    );
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 1, 2),
        order_line(2, 1, Some(50), 3, 60.0),
    );
    // Quantity 7 >= max_qty(6): excluded despite being in range.
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 2, 1),
        order_line(1, 1, Some(50), 7, 140.0),
    );
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 2, 2),
        order_line(2, 1, Some(150), 2, 999.0),
    );
    // Never delivered: excluded regardless of date/quantity.
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 3, 1),
        order_line(1, 1, None, 1, 10.0),
    );

    let (revenue, _ts) = q6(&db, 0, 200, 6);
    assert!(
        (revenue - 1159.0).abs() < 1e-9,
        "expected 100 + 60 + 999, got {revenue}"
    );
}

#[test]
fn q6_excludes_a_row_delivered_exactly_on_the_exclusive_upper_bound() {
    let db = TpccDatabase::new(RootIndexType::default());

    // Exactly at date_lo: included.
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 1, 1),
        order_line(1, 1, Some(0), 5, 7.0),
    );
    // Exactly at date_hi - 1: included (last valid instant).
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 2, 1),
        order_line(1, 1, Some(99), 5, 11.0),
    );
    // Exactly at date_hi: excluded (upper bound is exclusive).
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 3, 1),
        order_line(1, 1, Some(100), 5, 1_000_000.0),
    );

    let (revenue, _ts) = q6(&db, 0, 100, 6);
    assert!(
        (revenue - 18.0).abs() < 1e-9,
        "expected exactly 7 + 11 = 18 (the date_hi row must be excluded), got {revenue}"
    );

    db.enable_scan_pool(Table::OrderLine, 2, None);
    let pool = db.scan_pool(Table::OrderLine).unwrap();
    let (revenue_parallel, _ts) = q6_parallel(&db, &pool, 1, 0, 100, 6);
    assert!(
        (revenue_parallel - 18.0).abs() < 1e-9,
        "q6_parallel disagreed with q6 on the exclusive upper bound: got {revenue_parallel}"
    );
    db.disable_scan_pool(Table::OrderLine);
}

#[test]
fn benchbase_q6_uses_fixed_half_open_dates_and_inclusive_quantity() {
    let db = TpccDatabase::new(RootIndexType::default());
    for (order_id, delivered, quantity, amount) in [
        (1, BENCHBASE_Q6_DATE_LO_MILLIS, 1, 5.0),
        (2, BENCHBASE_Q6_DATE_HI_MILLIS - 1, 100, 7.0),
        (3, BENCHBASE_Q6_DATE_HI_MILLIS, 1, 1_000_000.0),
    ] {
        insert(
            &db,
            Table::OrderLine,
            k_order_line(1, 1, order_id, 1),
            order_line(1, 1, Some(delivered), quantity, amount),
        );
    }

    let (revenue, _) = q6_benchbase(&db);
    assert!((revenue - 12.0).abs() < 1e-9);
}

/// q4 counts, grouped by `o_ol_cnt`, orders entered in range that have at
/// least one order-line either delivered later than `o_entry_d +
/// late_slack_millis` or never delivered at all.
#[test]
fn q4_counts_late_orders_in_date_range_grouped_by_line_count() {
    let db = TpccDatabase::new(RootIndexType::default());

    // Order A: entry 100, one line delivered far past its slack -> late.
    insert(&db, Table::Orders, k_order(1, 1, 10), order(1, 100, 2));
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 10, 1),
        order_line(1, 1, Some(5_100), 5, 1.0),
    );
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 10, 2),
        order_line(2, 1, Some(600), 5, 1.0),
    );

    // Order B: entry 150, both lines well within slack -> not late.
    insert(&db, Table::Orders, k_order(1, 1, 11), order(2, 150, 2));
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 11, 1),
        order_line(1, 1, Some(650), 5, 1.0),
    );
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 11, 2),
        order_line(2, 1, Some(650), 5, 1.0),
    );

    // Order C: entry 9999, outside [date_lo, date_hi) -> excluded regardless of its lines.
    insert(&db, Table::Orders, k_order(1, 1, 12), order(3, 9_999, 2));

    // Order D: entry 120, one line never delivered -> counts as late.
    insert(&db, Table::Orders, k_order(1, 1, 13), order(4, 120, 2));
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 13, 1),
        order_line(1, 1, None, 5, 1.0),
    );
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 13, 2),
        order_line(2, 1, Some(1_000), 5, 1.0),
    );

    let (counts, _ts) = q4(&db, 0, 200, 1_000);

    assert_eq!(
        counts.len(),
        1,
        "only orders A and D are late, both with o_ol_cnt=2; B isn't late, C is out of range"
    );
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

    insert(
        &db,
        Table::Supplier,
        0,
        TpccRow::Supplier(Box::new(Supplier {
            s_name: "Supplier#FR".into(),
            s_address: String::new(),
            s_nationkey: 6,
            s_phone: String::new(),
            s_acctbal: 0.0,
            s_comment: String::new(),
        })),
    );
    insert(
        &db,
        Table::Supplier,
        1,
        TpccRow::Supplier(Box::new(Supplier {
            s_name: "Supplier#IN".into(),
            s_address: String::new(),
            s_nationkey: 8,
            s_phone: String::new(),
            s_acctbal: 0.0,
            s_comment: String::new(),
        })),
    );

    insert(
        &db,
        Table::Stock,
        k_stock(1, 1),
        TpccRow::Stock(Box::new(Stock {
            s_quantity: 50,
            s_dist: [[0u8; 24]; 10],
            s_ytd: 0.0,
            s_order_cnt: 0,
            s_remote_cnt: 0,
            s_data: String::new(),
            s_su_suppkey: 0, // French supplier
        })),
    );
    insert(
        &db,
        Table::Stock,
        k_stock(1, 2),
        TpccRow::Stock(Box::new(Stock {
            s_quantity: 50,
            s_dist: [[0u8; 24]; 10],
            s_ytd: 0.0,
            s_order_cnt: 0,
            s_remote_cnt: 0,
            s_data: String::new(),
            s_su_suppkey: 1, // Indian supplier
        })),
    );

    insert(&db, Table::Orders, k_order(1, 1, 1), order(1, 100, 2));
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 1, 1),
        order_line(1, 1, Some(100), 1, 50.0),
    );
    insert(
        &db,
        Table::OrderLine,
        k_order_line(1, 1, 1, 2),
        order_line(2, 1, Some(100), 1, 30.0),
    );

    let (europe, _ts) = q5(&db, "EUROPE", 0, 200);
    assert_eq!(
        europe.len(),
        1,
        "only the French-supplied line should count towards EUROPE"
    );
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

    let mut tx = crate::bat_bench::tpcc_txn::TpccTxn::begin(&db);
    let regions = crate::bat_bench::tpcc_txn::many(tx.range(
        Table::Region,
        crate::bat_bench::tpcc_schema::region_table_range(),
        true,
    ));
    let nations = crate::bat_bench::tpcc_txn::many(tx.range(
        Table::Nation,
        crate::bat_bench::tpcc_schema::nation_table_range(),
        true,
    ));
    tx.commit();

    assert_eq!(regions.len(), 5);
    assert_eq!(nations.len(), 25);

    let region_name_at = |id: u8| -> String {
        regions
            .iter()
            .find(|r| crate::bat_bench::tpcc_schema::decode_region_id(r.key) == id)
            .unwrap()
            .payload
            .as_region()
            .r_name
            .clone()
    };
    let nation_at = |id: u8| -> (String, u8) {
        let n = nations
            .iter()
            .find(|n| crate::bat_bench::tpcc_schema::decode_nation_id(n.key) == id)
            .unwrap();
        (
            n.payload.as_nation().n_name.clone(),
            n.payload.as_nation().n_regionkey,
        )
    };

    assert_eq!(region_name_at(3), "EUROPE");
    assert_eq!(region_name_at(2), "ASIA");
    assert_eq!(nation_at(6), ("FRANCE".to_string(), 3));
    assert_eq!(nation_at(8), ("INDIA".to_string(), 2));
}

fn populate_multi_warehouse_order_lines(db: &TpccDatabase, num_warehouses: u32) {
    for w in 1..=num_warehouses {
        insert(
            db,
            Table::OrderLine,
            k_order_line(w, 1, 1, 1),
            order_line(1, w, Some(50), w as u8, 10.0 * w as f64),
        );
        insert(
            db,
            Table::OrderLine,
            k_order_line(w, 1, 1, 2),
            order_line(2, w, Some(50), w as u8, 5.0 * w as f64),
        );
    }
}

#[test]
fn q1_parallel_matches_sequential_q1_across_fanouts() {
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);

    let (expected, _) = q1(&db, 100);
    assert_eq!(
        expected.len(),
        2,
        "ol_number 1 and 2 across all 5 warehouses"
    );

    for fanout in [1, 2, 3, 8] {
        let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), fanout, Some(1));
        // `ScanWorkerPool::spawn` floors every request at 2 workers (a
        // "pool" of 1 buys no parallelism over the sequential path) — see
        // that method's doc.
        assert_eq!(pool.num_workers(), fanout.max(2));
        let (actual, _) = q1_parallel(&db, &pool, num_warehouses, 100);

        assert_eq!(actual.len(), expected.len(), "fanout={fanout}");
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.ol_number, e.ol_number, "fanout={fanout}");
            assert_eq!(a.count, e.count, "fanout={fanout}");
            assert_eq!(a.sum_qty, e.sum_qty, "fanout={fanout}");
            assert!(
                (a.sum_amount - e.sum_amount).abs() < 1e-9,
                "fanout={fanout}: expected {}, got {}",
                e.sum_amount,
                a.sum_amount
            );
        }
    }
}

/// Same cross-check as above, for `q6_parallel` vs. the sequential `q6`.
#[test]
fn q6_parallel_matches_sequential_q6_across_fanouts() {
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);

    let (expected, _) = q6(&db, 0, 200, 250);
    assert!(
        expected > 0.0,
        "sanity: fixture should have matching revenue"
    );

    for fanout in [1, 2, 3, 8] {
        let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), fanout, Some(1));
        let (actual, _) = q6_parallel(&db, &pool, num_warehouses, 0, 200, 250);
        assert!(
            (actual - expected).abs() < 1e-9,
            "fanout={fanout}: expected {expected}, got {actual}"
        );
    }
}

#[test]
fn enable_scan_pool_assigns_a_working_pool_and_disable_scan_pool_removes_it() {
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);

    let (expected, _) = q1(&db, 100);
    assert!(
        db.scan_pool(Table::OrderLine).is_none(),
        "no pool assigned yet"
    );

    db.enable_scan_pool(Table::OrderLine, 3, None);
    let pool = db
        .scan_pool(Table::OrderLine)
        .expect("enable_scan_pool should have assigned one");
    assert_eq!(pool.num_workers(), 3);

    let (actual, _) = q1_parallel(&db, &pool, num_warehouses, 100);
    assert_eq!(actual.len(), expected.len());
    for (a, e) in actual.iter().zip(expected.iter()) {
        assert_eq!(a.ol_number, e.ol_number);
        assert_eq!(a.count, e.count);
        assert_eq!(a.sum_qty, e.sum_qty);
        assert!((a.sum_amount - e.sum_amount).abs() < 1e-9);
    }

    db.disable_scan_pool(Table::OrderLine);
    assert!(
        db.scan_pool(Table::OrderLine).is_none(),
        "disable_scan_pool should have cleared it"
    );
}

#[test]
fn tpcc_txn_range_and_range_count_transparently_use_the_order_line_scan_pool_when_one_is_assigned()
{
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);
    let full_range = order_line_table_range();

    let mut tx_before = TpccTxn::begin(&db);
    let expected_count = tx_before.range_count(Table::OrderLine, full_range);
    let expected_rows = match tx_before.range(Table::OrderLine, full_range, true) {
        CRUDOperationResult::MatchedRecords(v) => v.len(),
        other => panic!("unexpected range result: {other}"),
    };
    tx_before.commit();
    assert_eq!(
        expected_count,
        2 * num_warehouses as usize,
        "sanity: 2 order-lines inserted per warehouse"
    );
    assert_eq!(expected_rows, expected_count);

    db.enable_scan_pool(Table::OrderLine, 4, None);

    let mut tx_after = TpccTxn::begin(&db);
    let actual_count = tx_after.range_count(Table::OrderLine, full_range);
    let actual_rows = match tx_after.range(Table::OrderLine, full_range, true) {
        CRUDOperationResult::MatchedRecords(v) => v.len(),
        other => panic!("unexpected range result: {other}"),
    };
    tx_after.commit();

    assert_eq!(
        actual_count, expected_count,
        "range_count must agree whether or not a pool is assigned"
    );
    assert_eq!(
        actual_rows, expected_rows,
        "range must agree whether or not a pool is assigned"
    );

    db.disable_scan_pool(Table::OrderLine);
}

#[test]
fn shared_scan_pool_serves_concurrent_callers_correctly() {
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);

    let (expected_q1, _) = q1(&db, 100);
    let (expected_q6, _) = q6(&db, 0, 200, 250);
    db.enable_scan_pool(Table::OrderLine, 2, None);

    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let db = &db;
                let expected_q1 = &expected_q1;
                scope.spawn(move || {
                    let pool = db
                        .scan_pool(Table::OrderLine)
                        .expect("pool was assigned above");
                    if i % 2 == 0 {
                        let (actual, _) = q1_parallel(db, &pool, num_warehouses, 100);
                        assert_eq!(actual.len(), expected_q1.len());
                        for (a, e) in actual.iter().zip(expected_q1.iter()) {
                            assert_eq!(a.count, e.count);
                            assert_eq!(a.sum_qty, e.sum_qty);
                            assert!((a.sum_amount - e.sum_amount).abs() < 1e-9);
                        }
                    } else {
                        let (actual, _) = q6_parallel(db, &pool, num_warehouses, 0, 200, 250);
                        assert!((actual - expected_q6).abs() < 1e-9);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    });

    db.disable_scan_pool(Table::OrderLine);
}

#[test]
fn try_dispatch_runs_inline_when_the_pool_has_no_spare_capacity() {
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);
    let (expected, _) = q1(&db, 100);

    let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), 2, None);
    assert_eq!(pool.num_workers(), 2);
    let hold = Duration::from_millis(300);

    std::thread::scope(|scope| {
        // Occupy both workers with a slow job each, so the pool has zero
        // spare capacity for `hold`'s whole duration.
        for _ in 0..pool.num_workers() {
            scope.spawn(|| {
                pool.dispatch(vec![order_line_table_range()], move |_tree, _range| {
                    std::thread::sleep(hold);
                    0
                });
            });
        }
        // Give both workers a moment to actually pick their job up
        // (`in_flight` only increments once a worker's `recv()` returns).
        std::thread::sleep(hold / 4);
        assert!(
            !pool.has_spare_capacity(),
            "both workers should be busy with the slow filler jobs"
        );

        let start = Instant::now();
        let (actual, _) = q1_parallel(&db, &pool, num_warehouses, 100);
        let elapsed = start.elapsed();
        assert!(
            elapsed < hold / 2,
            "try_dispatch should have run inline instead of waiting behind the busy pool, took {elapsed:?}"
        );

        assert_eq!(actual.len(), expected.len());
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.ol_number, e.ol_number);
            assert_eq!(a.count, e.count);
            assert_eq!(a.sum_qty, e.sum_qty);
            assert!((a.sum_amount - e.sum_amount).abs() < 1e-9);
        }
    });
}

#[test]
fn scan_pool_can_oversubscribe_past_max_workers_without_registering() {
    let db = TpccDatabase::new_with_big_tree_size_and_max_workers(
        RootIndexType::default(),
        BigTreeSize::default(),
        2,
    );
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);
    let (expected, _) = q1(&db, 100);

    // `Some(1)` makes `fair_query_fanout` hand the whole 10-worker pool to
    // this test's one caller, so the query below actually exercises all 10
    // pool threads rather than just its `Some(4)` "unknown" default.
    db.enable_scan_pool(Table::OrderLine, 10, Some(1));
    let pool = db
        .scan_pool(Table::OrderLine)
        .expect("enable_scan_pool should have assigned one");
    assert_eq!(
        pool.num_workers(),
        10,
        "far more workers than max_workers=2"
    );

    let (actual, _) = q1_parallel(&db, &pool, num_warehouses, 100);
    assert_eq!(actual.len(), expected.len());
    for (a, e) in actual.iter().zip(expected.iter()) {
        assert_eq!(a.ol_number, e.ol_number);
        assert_eq!(a.count, e.count);
        assert_eq!(a.sum_qty, e.sum_qty);
        assert!((a.sum_amount - e.sum_amount).abs() < 1e-9);
    }

    db.disable_scan_pool(Table::OrderLine);
}

#[test]
fn scan_pool_serves_multiple_query_sized_slices_concurrently() {
    let db = TpccDatabase::new(RootIndexType::default());
    let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), 6, None);
    assert_eq!(pool.num_workers(), 6);
    let hold = Duration::from_millis(200);

    let start = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..3 {
            scope.spawn(|| {
                pool.dispatch(
                    vec![order_line_table_range(), order_line_table_range()],
                    move |_tree, _range| {
                        std::thread::sleep(hold);
                        0
                    },
                );
            });
        }
    });
    let elapsed = start.elapsed();
    assert!(
        elapsed < hold * 2,
        "3 concurrent 2-worker slices on a 6-worker pool should run at the same time \
         (~{hold:?} total), not serialize (~3x{hold:?}) — took {elapsed:?}"
    );
}

#[test]
fn scan_pool_vs_sequential_crossover() {
    let num_warehouses = 8u32;
    let repeats = 5usize;

    println!(
        "{:>12} {:>10} {:>14} {:>14} {:>8}",
        "rows/wh", "total", "seq_us", "pool_us", "pool/seq"
    );
    for &rows_per_warehouse in &[100usize, 2_000, 8_192, 20_000] {
        let db = TpccDatabase::new(RootIndexType::default());
        for w in 1..=num_warehouses {
            for i in 0..rows_per_warehouse {
                insert(
                    &db,
                    Table::OrderLine,
                    k_order_line(w, 1, i as u32, ((i % 15) + 1) as u8),
                    order_line(1, w, Some(50), 5, 10.0),
                );
            }
        }

        db.enable_scan_pool(Table::OrderLine, 2, None);
        let pool = db.scan_pool(Table::OrderLine).unwrap();

        // Warm up both paths once (first call pays one-time costs: root
        // resolution, thread-local snapshot cache allocation, etc.) before
        // timing either.
        let _ = q1(&db, 100);
        let _ = q1_parallel(&db, &pool, num_warehouses, 100);

        let seq_start = Instant::now();
        for _ in 0..repeats {
            let _ = q1(&db, 100);
        }
        let seq_us = seq_start.elapsed().as_micros() as f64 / repeats as f64;

        let pool_start = Instant::now();
        for _ in 0..repeats {
            let _ = q1_parallel(&db, &pool, num_warehouses, 100);
        }
        let pool_us = pool_start.elapsed().as_micros() as f64 / repeats as f64;

        println!(
            "{:>12} {:>10} {:>14.1} {:>14.1} {:>8.2}",
            rows_per_warehouse,
            rows_per_warehouse * num_warehouses as usize,
            seq_us,
            pool_us,
            pool_us / seq_us,
        );

        db.disable_scan_pool(Table::OrderLine);
    }
}

#[test]
fn zone_map_pruning_speeds_up_a_narrow_q1_predicate() {
    let num_warehouses = 8u32;
    let rows_per_warehouse = 20_000usize;
    let repeats = 20usize;

    let db = TpccDatabase::new(RootIndexType::default());
    for w in 1..=num_warehouses {
        for i in 0..rows_per_warehouse {
            insert(
                &db,
                Table::OrderLine,
                k_order_line(w, 1, i as u32, ((i % 15) + 1) as u8),
                order_line(1, w, Some(i as i64), 5, 10.0),
            );
        }
    }

    let full_cutoff = rows_per_warehouse as i64;
    let narrow_cutoff = rows_per_warehouse as i64 / 50;

    // Warm up both paths once before timing either.
    let (full_result, _) = q1(&db, full_cutoff);
    let (narrow_result, _) = q1(&db, narrow_cutoff);

    let full_start = Instant::now();
    for _ in 0..repeats {
        let _ = q1(&db, full_cutoff);
    }
    let full_us = full_start.elapsed().as_micros() as f64 / repeats as f64;

    let narrow_start = Instant::now();
    for _ in 0..repeats {
        let _ = q1(&db, narrow_cutoff);
    }
    let narrow_us = narrow_start.elapsed().as_micros() as f64 / repeats as f64;

    println!(
        "\n=== zone-map pruning: q1 over {} rows, cutoff covering 100% vs ~2% of dates ===",
        rows_per_warehouse * num_warehouses as usize
    );
    println!(
        "full_cutoff={full_cutoff} -> {full_us:.1}us/call, {} matched rows",
        full_result.iter().map(|g| g.count).sum::<u64>()
    );
    println!(
        "narrow_cutoff={narrow_cutoff} -> {narrow_us:.1}us/call, {} matched rows",
        narrow_result.iter().map(|g| g.count).sum::<u64>()
    );
    println!("narrow/full time ratio: {:.3}", narrow_us / full_us);

    assert!(
        narrow_result.iter().map(|g| g.count).sum::<u64>()
            < full_result.iter().map(|g| g.count).sum::<u64>(),
        "the narrow cutoff should match strictly fewer rows than the full one"
    );
}

fn partition_warehouses(num_warehouses: u32, fanout: usize) -> Vec<Interval<TpccKey>> {
    let fanout = fanout.max(1);
    let n = num_warehouses as usize;
    let base = n / fanout;
    let rem = n % fanout;
    let full = order_line_table_range();
    let mut ranges = Vec::with_capacity(fanout);
    let mut next_w_id = 1u32;
    for i in 0..fanout {
        let count = base + if i < rem { 1 } else { 0 };
        if count == 0 {
            ranges.push(Interval::new(full.upper, full.lower));
            continue;
        }
        let lo_w = next_w_id;
        let hi_w = next_w_id + count as u32 - 1;
        next_w_id = hi_w + 1;
        let lower = if i == 0 {
            full.lower
        } else {
            k_order_line(lo_w, 0, 0, 0)
        };
        let upper = if i == fanout - 1 {
            full.upper
        } else {
            k_order_line(hi_w + 1, 0, 0, 0) - 1
        };
        ranges.push(Interval::new(lower, upper));
    }
    ranges
}

fn count_delivered(
    tree: &TpccTree,
    range: Interval<TpccKey>,
    ts_start: u64,
    delivered_before: i64,
) -> u64 {
    let mut count = 0u64;
    RangeQueryIter::new(tree, ts_start, range, false, READ_ONLY_SCAN_WORKER_ID).for_each_ref(
        |_, row| {
            let ol = row.as_order_line();
            if ol.ol_delivery_d.is_some_and(|d| d <= delivered_before) {
                count += 1;
            }
        },
    );
    count
}

#[test]
#[ignore]
fn scan_pool_fanout_scaling() {
    let num_warehouses = 8u32;
    let rows_per_warehouse = 100_000usize;
    let repeats = 5usize;

    let db = TpccDatabase::new(RootIndexType::default());
    for w in 1..=num_warehouses {
        for i in 0..rows_per_warehouse {
            insert(
                &db,
                Table::OrderLine,
                k_order_line(w, 1, i as u32, ((i % 15) + 1) as u8),
                order_line(1, w, Some(50), 5, 10.0),
            );
        }
    }

    let tx = TpccTxn::begin(&db);
    let ts_start = tx.ts_start();
    let tree = db.tree_for(Table::OrderLine);

    let seq_start = Instant::now();
    let mut seq_total = 0u64;
    for _ in 0..repeats {
        seq_total = count_delivered(&tree, order_line_table_range(), ts_start, 100);
    }
    let seq_us = seq_start.elapsed().as_micros() as f64 / repeats as f64;
    println!("{:>8} {:>14} {:>8}", "fanout", "latency_us", "speedup");
    println!("{:>8} {:>14.1} {:>8}", "seq", seq_us, "1.00x");

    for &fanout in &[2usize, 4, 8, 16, 32, 64] {
        let pool = ScanWorkerPool::spawn(tree.clone(), fanout, None);
        let ranges = partition_warehouses(num_warehouses, fanout);

        // Warm up (thread spin-up, first-touch costs) before timing.
        let _ = pool.dispatch(ranges.clone(), move |t, r| {
            count_delivered(t, r, ts_start, 100)
        });

        let start = Instant::now();
        let mut total = 0u64;
        for _ in 0..repeats {
            total = pool
                .dispatch(ranges.clone(), move |t, r| {
                    count_delivered(t, r, ts_start, 100)
                })
                .into_iter()
                .sum();
        }
        let us = start.elapsed().as_micros() as f64 / repeats as f64;
        assert_eq!(
            total, seq_total,
            "fanout={fanout} must count the same rows as sequential"
        );
        println!("{:>8} {:>14.1} {:>7.2}x", fanout, us, seq_us / us);
    }

    db.enable_scan_pool(Table::OrderLine, 2, Some(1));
    let pool = db.scan_pool(Table::OrderLine).unwrap();
    let _ = q1_parallel(&db, &pool, num_warehouses, 100);
    let start = Instant::now();
    for _ in 0..repeats {
        let _ = q1_parallel(&db, &pool, num_warehouses, 100);
    }
    let q1_parallel_us = start.elapsed().as_micros() as f64 / repeats as f64;
    println!(
        "{:>8} {:>14.1} {:>7.2}x  (real q1_parallel, try_dispatch, pool=2)",
        "n/a",
        q1_parallel_us,
        seq_us / q1_parallel_us
    );
    db.disable_scan_pool(Table::OrderLine);

    tx.commit();
}

#[test]
fn fair_query_fanout_uses_the_default_when_expected_concurrent_queries_is_unknown() {
    let db = TpccDatabase::new(RootIndexType::default());
    let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), 10, None);
    assert_eq!(pool.fair_query_fanout(), Some(DEFAULT_QUERY_FANOUT));
}

#[test]
fn fair_query_fanout_treats_zero_expected_callers_as_unknown() {
    let db = TpccDatabase::new(RootIndexType::default());
    let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), 10, Some(0));
    assert_eq!(pool.fair_query_fanout(), Some(DEFAULT_QUERY_FANOUT));
}

#[test]
fn fair_query_fanout_caps_the_unknown_default_at_pool_capacity() {
    let db = TpccDatabase::new(RootIndexType::default());
    let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), 2, None);
    assert_eq!(pool.fair_query_fanout(), Some(2));
}

#[test]
fn fair_query_fanout_divides_pool_capacity_evenly_across_expected_callers() {
    let db = TpccDatabase::new(RootIndexType::default());
    for &(num_workers, expected_callers, want) in &[
        (32usize, 4usize, 8usize),
        (16, 2, 8),
        (100, 10, 10),
        (9, 3, 3),
    ] {
        let pool = ScanWorkerPool::spawn(
            db.tree_for(Table::OrderLine),
            num_workers,
            Some(expected_callers),
        );
        assert_eq!(
            pool.fair_query_fanout(),
            Some(want),
            "num_workers={num_workers} expected_callers={expected_callers}"
        );
    }
}

#[test]
fn fair_query_fanout_returns_none_when_too_many_expected_callers_to_share_fairly() {
    let db = TpccDatabase::new(RootIndexType::default());
    // share = 10/10 = 1 < 2
    let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), 10, Some(10));
    assert_eq!(pool.fair_query_fanout(), None);
    // share = 10/20 = 0 < 2
    let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), 10, Some(20));
    assert_eq!(pool.fair_query_fanout(), None);
}

#[test]
#[should_panic(expected = "is a TreeClass::Big table")]
fn enable_scan_pool_panics_for_warehouse_a_big_class_table() {
    let db = TpccDatabase::new(RootIndexType::default());
    db.enable_scan_pool(Table::Warehouse, 4, None);
}

#[test]
#[should_panic(expected = "is a TreeClass::Big table")]
fn disable_scan_pool_panics_for_district_a_big_class_table() {
    let db = TpccDatabase::new(RootIndexType::default());
    db.disable_scan_pool(Table::District);
}

#[test]
#[should_panic(expected = "is a TreeClass::Big table")]
fn scan_pool_getter_panics_for_warehouse_a_big_class_table() {
    let db = TpccDatabase::new(RootIndexType::default());
    let _ = db.scan_pool(Table::Warehouse);
}

#[test]
fn multiple_tables_can_have_independent_scan_pools_at_once() {
    let db = TpccDatabase::new(RootIndexType::default());
    db.enable_scan_pool(Table::OrderLine, 4, Some(2));
    db.enable_scan_pool(Table::Item, 10, Some(2));

    let order_line_pool = db
        .scan_pool(Table::OrderLine)
        .expect("OrderLine pool should be assigned");
    let item_pool = db
        .scan_pool(Table::Item)
        .expect("Item pool should be assigned");
    assert_eq!(order_line_pool.num_workers(), 4);
    assert_eq!(item_pool.num_workers(), 10);
    assert_eq!(order_line_pool.fair_query_fanout(), Some(2));
    assert_eq!(item_pool.fair_query_fanout(), Some(5));

    db.disable_scan_pool(Table::OrderLine);
    assert!(
        db.scan_pool(Table::OrderLine).is_none(),
        "disabling OrderLine's pool must not touch Item's"
    );
    assert!(
        db.scan_pool(Table::Item).is_some(),
        "Item's own pool must still be assigned"
    );

    db.disable_scan_pool(Table::Item);
    assert!(db.scan_pool(Table::Item).is_none());
}

#[test]
fn replacing_a_scan_pool_does_not_break_a_caller_still_holding_the_old_one() {
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);
    let (expected, _) = q1(&db, 100);

    db.enable_scan_pool(Table::OrderLine, 3, None);
    let old_pool = db.scan_pool(Table::OrderLine).unwrap();
    assert_eq!(old_pool.num_workers(), 3);

    db.enable_scan_pool(Table::OrderLine, 6, None);
    let new_pool = db.scan_pool(Table::OrderLine).unwrap();
    assert_eq!(
        new_pool.num_workers(),
        6,
        "re-enabling must replace, not add to, the old pool"
    );

    let (actual, _) = q1_parallel(&db, &old_pool, num_warehouses, 100);
    assert_eq!(actual.len(), expected.len());
    for (a, e) in actual.iter().zip(expected.iter()) {
        assert_eq!(a.count, e.count);
        assert_eq!(a.sum_qty, e.sum_qty);
        assert!((a.sum_amount - e.sum_amount).abs() < 1e-9);
    }

    db.disable_scan_pool(Table::OrderLine);
}

#[test]
fn shared_scan_pool_stays_correct_under_heavy_concurrent_load() {
    let db = TpccDatabase::new_with_big_tree_size_and_max_workers(
        RootIndexType::default(),
        BigTreeSize::default(),
        32,
    );
    let num_warehouses = 6;
    populate_multi_warehouse_order_lines(&db, num_warehouses);

    let (expected_q1, _) = q1(&db, 100);
    let (expected_q6, _) = q6(&db, 0, 200, 250);
    db.enable_scan_pool(Table::OrderLine, 4, Some(8));

    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..20)
            .map(|i| {
                let db = &db;
                let expected_q1 = &expected_q1;
                scope.spawn(move || {
                    let pool = db.scan_pool(Table::OrderLine).unwrap();
                    for _ in 0..10 {
                        if i % 2 == 0 {
                            let (actual, _) = q1_parallel(db, &pool, num_warehouses, 100);
                            assert_eq!(actual.len(), expected_q1.len());
                            for (a, e) in actual.iter().zip(expected_q1.iter()) {
                                assert_eq!(a.count, e.count);
                                assert_eq!(a.sum_qty, e.sum_qty);
                                assert!((a.sum_amount - e.sum_amount).abs() < 1e-9);
                            }
                        } else {
                            let (actual, _) = q6_parallel(db, &pool, num_warehouses, 0, 200, 250);
                            assert!((actual - expected_q6).abs() < 1e-9);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    });

    db.disable_scan_pool(Table::OrderLine);
}
