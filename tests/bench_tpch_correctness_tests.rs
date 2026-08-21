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

use crate::bat_bench::tpcc_load::populate_regions_and_nations;
use crate::bat_bench::tpcc_schema::{
    BigTreeSize, Order, OrderLine, Stock, Supplier, Table, TpccDatabase, TpccKey, TpccRow, TpccTree, k_order,
    k_order_line, k_stock, order_line_table_range,
};
use crate::bat_bench::parallel_scan::{q1_parallel, q6_parallel};
use crate::bat_bench::tpcc_schema::TpccScanWorkerPool as ScanWorkerPool;
use crate::bat_bench::tpcc_txn::TpccTxn;
use crate::bat_bench::tpch_queries::{q1, q4, q5, q6};
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_sync::worker::READ_ONLY_SCAN_WORKER_ID;
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
        ol_dist_info: String::new(),
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
            s_dist: std::array::from_fn(|_| String::new()),
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
            s_dist: std::array::from_fn(|_| String::new()),
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

/// Populates `Table::OrderLine` with 2 lines per warehouse across
/// `1..=num_warehouses`, each warehouse's data shifted so the queries below
/// have warehouse-distinguishable, hand-predictable totals — used to check
/// `parallel_scan::q1_parallel`/`q6_parallel` (which split the scan by
/// warehouse, see that module's doc) against the sequential `q1`/`q6` on
/// exactly the same data.
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

/// `q1_parallel` must agree with the sequential `q1` byte-for-byte on the
/// same data, including when `fanout` doesn't evenly divide
/// `num_warehouses` (uneven blocks) and when it exceeds it (some workers get
/// a genuinely empty range) — see `partition_order_line_range`'s doc for
/// both cases.
#[test]
fn q1_parallel_matches_sequential_q1_across_fanouts() {
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);

    let (expected, _) = q1(&db, 100);
    assert_eq!(expected.len(), 2, "ol_number 1 and 2 across all 5 warehouses");

    for fanout in [1, 2, 3, 8] {
        // `expected_concurrent_queries: Some(1)` makes `fair_query_fanout`
        // hand back this whole pool (`pool.num_workers()`) to the one
        // caller here, reproducing the pre-`fair_query_fanout` sweep this
        // test was written for.
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
    assert!(expected > 0.0, "sanity: fixture should have matching revenue");

    for fanout in [1, 2, 3, 8] {
        let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), fanout, Some(1));
        let (actual, _) = q6_parallel(&db, &pool, num_warehouses, 0, 200, 250);
        assert!(
            (actual - expected).abs() < 1e-9,
            "fanout={fanout}: expected {expected}, got {actual}"
        );
    }
}

/// Exercises the actual database-assignment path (`TpccDatabase::
/// enable_scan_pool`/`scan_pool`/`disable_scan_pool`) rather than a
/// hand-spawned `ScanWorkerPool` — this is what `OlapMode::ChQ1`/`ChQ6`
/// actually call through in `olap_scan::run_olap_worker`. Confirms the
/// assigned pool produces the exact same result as the sequential query,
/// and that `scan_pool` correctly reports `None` before assignment and
/// after `disable_scan_pool`.
#[test]
fn enable_scan_pool_assigns_a_working_pool_and_disable_scan_pool_removes_it() {
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);

    let (expected, _) = q1(&db, 100);
    assert!(db.scan_pool(Table::OrderLine).is_none(), "no pool assigned yet");

    db.enable_scan_pool(Table::OrderLine, 3, None);
    let pool = db.scan_pool(Table::OrderLine).expect("enable_scan_pool should have assigned one");
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
    assert!(db.scan_pool(Table::OrderLine).is_none(), "disable_scan_pool should have cleared it");
}

/// The whole point of the redesigned pool (see `scan_pool::ScanWorkerPool`'s
/// doc) is that several concurrent callers can share one pool without their
/// jobs' results getting mixed up — the old per-query-exclusive design
/// could only ever have one caller holding it at a time. Runs several
/// `q1_parallel`/`q6_parallel` calls concurrently against one shared,
/// database-assigned pool and checks every single one still gets the exact
/// right answer.
#[test]
fn shared_scan_pool_serves_concurrent_callers_correctly() {
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);

    let (expected_q1, _) = q1(&db, 100);
    let (expected_q6, _) = q6(&db, 0, 200, 250);
    // `None`: keep the pool's whole 2-worker capacity available to each of
    // the 4 concurrent callers below via `fair_query_fanout`'s "unknown"
    // default (`Some(2)`), so this test still exercises real dispatch/
    // sharing through the pool rather than every caller sharing a fair
    // slice too thin to bother with.
    db.enable_scan_pool(Table::OrderLine, 2, None);

    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let db = &db;
                let expected_q1 = &expected_q1;
                scope.spawn(move || {
                    let pool = db.scan_pool(Table::OrderLine).expect("pool was assigned above");
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

/// `q1_parallel`/`q6_parallel` go through `ScanWorkerPool::try_dispatch`,
/// not `dispatch` — when the pool has no spare capacity, a query must run
/// on the calling thread instead of queuing behind whatever else is
/// keeping every worker busy. Occupies both of a 2-worker pool's workers
/// with artificially slow jobs, then checks `q1_parallel` still returns the
/// exact right answer, and returns almost immediately rather than waiting
/// out the slow jobs' `hold` duration.
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
        assert!(!pool.has_spare_capacity(), "both workers should be busy with the slow filler jobs");

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

/// The whole point of `bat_sync::worker::READ_ONLY_SCAN_WORKER_ID` (see its
/// doc) is that a `ScanWorkerPool`'s own worker threads never draw from the
/// tree's fixed `WorkerRegistry` — so a pool can be sized past
/// `max_workers` entirely safely. Builds a database whose registry only
/// has room for 2 real workers, then assigns `ORDER_LINE` a 10-worker
/// pool: if any pool worker thread ever called `tree.worker_id()`, the
/// registry would panic (`WorkerRegistry::acquire`'s `assert!(id <
/// max_workers)`) well before all 10 could register, which would surface
/// here as `q1_parallel` never getting a result back for at least one of
/// its jobs (a panicking worker drops its result sender without sending,
/// so `ScanWorkerPool::dispatch`'s `rx.recv().expect(..)` panics too).
#[test]
fn scan_pool_can_oversubscribe_past_max_workers_without_registering() {
    let db = TpccDatabase::new_with_big_tree_size_and_max_workers(RootIndexType::default(), BigTreeSize::default(), 2);
    let num_warehouses = 5;
    populate_multi_warehouse_order_lines(&db, num_warehouses);
    let (expected, _) = q1(&db, 100);

    // `Some(1)` makes `fair_query_fanout` hand the whole 10-worker pool to
    // this test's one caller, so the query below actually exercises all 10
    // pool threads rather than just its `Some(2)` "unknown" default.
    db.enable_scan_pool(Table::OrderLine, 10, Some(1));
    let pool = db.scan_pool(Table::OrderLine).expect("enable_scan_pool should have assigned one");
    assert_eq!(pool.num_workers(), 10, "far more workers than max_workers=2");

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

/// Proves the pool genuinely *shares* capacity across concurrently
/// querying callers, not just "doesn't halt anyone" — see `parallel_scan`'s
/// "Partitioning" doc for why a query asks for a fair share of the pool
/// (`ScanWorkerPool::fair_query_fanout`) rather than its entire capacity:
/// with a 6-worker pool, 3 query-sized (2-worker) slices should all get
/// serviced by the pool's workers at the same time, not one after another.
/// Uses the same slow-filler-job technique as
/// `try_dispatch_runs_inline_when_the_pool_has_no_spare_capacity`
/// (`pool.dispatch` directly, bypassing q1/q6-specific logic, and so also
/// bypassing `fair_query_fanout` — this test picks the 2-per-batch slice
/// size itself) to make "did this run concurrently" a simple wall-clock
/// check: 3 batches of 2 jobs each, each job sleeping `hold`, all submitted
/// at once — if the pool actually shares its 6 workers across all 3
/// batches, total wall time is ~1 `hold`; if it silently serialized them,
/// ~3.
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
                pool.dispatch(vec![order_line_table_range(), order_line_table_range()], move |_tree, _range| {
                    std::thread::sleep(hold);
                    0
                });
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

/// Not a correctness check but kept as a permanent perf probe (same
/// convention as this codebase's other `compare_*`/`*_bench` tests, e.g.
/// `bat_test::tpcc_wal_backend_bench`): tracks where the scan-size
/// crossover actually sits between `q1_parallel`'s pool overhead (channel
/// send/recv + oneshot result channel per sub-range job) and sequential
/// `q1` just walking the whole range itself — i.e. gives an early signal if
/// a future change moves `parallel_scan::MIN_ROWS_FOR_SCAN_POOL`'s
/// calibration (currently 65,536) out of date. Sweeps `ORDER_LINE` row
/// counts with a fixed 2-worker pool (matching the driver's own floor/
/// default) and a fixed 8 warehouses — one point sits exactly at
/// `MIN_ROWS_FOR_SCAN_POOL` — timing several repeats of each at each size.
/// Prints a table (`--nocapture` to see it) rather than asserting on exact
/// timings, which would be flaky under CI/machine noise; the crossover
/// itself was established once, deliberately, via a much finer sweep run
/// by hand (not kept here) — see this constant's own doc for that data.
#[test]
fn scan_pool_vs_sequential_crossover() {
    let num_warehouses = 8u32;
    let repeats = 5usize;

    println!("{:>12} {:>10} {:>14} {:>14} {:>8}", "rows/wh", "total", "seq_us", "pool_us", "pool/seq");
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

/// Splits `order_line_table_range()` into `fanout` contiguous warehouse
/// blocks — a copy of `parallel_scan::partition_order_line_range`'s logic
/// (private to that module) so this probe can sweep fanout independently
/// of whatever `ScanWorkerPool::fair_query_fanout` would actually compute
/// for a given pool/OLAP-thread-count combination.
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
        let lower = if i == 0 { full.lower } else { k_order_line(lo_w, 0, 0, 0) };
        let upper = if i == fanout - 1 { full.upper } else { k_order_line(hi_w + 1, 0, 0, 0) - 1 };
        ranges.push(Interval::new(lower, upper));
    }
    ranges
}

fn count_delivered(tree: &TpccTree, range: Interval<TpccKey>, ts_start: u64, delivered_before: i64) -> u64 {
    let mut count = 0u64;
    RangeQueryIter::new(tree, ts_start, range, false, READ_ONLY_SCAN_WORKER_ID).for_each_ref(|_, row| {
        let ol = row.as_order_line();
        if ol.ol_delivery_d.is_some_and(|d| d <= delivered_before) {
            count += 1;
        }
    });
    count
}

/// Investigative probe (`#[ignore]`d, not a correctness check): how much
/// does one query's *own* latency actually improve as its fan-out grows?
/// Times the same full-table scan split into 2/4/8/16/32/64 pool workers
/// directly (bypassing `q1_parallel`'s `fair_query_fanout`-based
/// partitioning), on one large, fixed-size fixture, to see where the
/// per-query speedup curve actually saturates. This is the measurement
/// that motivated `ScanWorkerPool::fair_query_fanout` in the first place:
/// a fixed fan-out of 2 (this module's old, now-removed `QUERY_FANOUT`)
/// only captured the first small step of a curve that keeps improving up
/// to ~8 workers (~2.4x) before flattening out — so a query's fair share
/// of a large, `num_cpus`-sized pool captures far more of that ceiling
/// than a fixed small constant ever could, whenever few enough OLAP
/// threads are sharing the pool to afford it.
/// Run with: `cargo test --release --bin batstore -- --ignored --nocapture scan_pool_fanout_scaling`
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
        let _ = pool.dispatch(ranges.clone(), move |t, r| count_delivered(t, r, ts_start, 100));

        let start = Instant::now();
        let mut total = 0u64;
        for _ in 0..repeats {
            total = pool
                .dispatch(ranges.clone(), move |t, r| count_delivered(t, r, ts_start, 100))
                .into_iter()
                .sum();
        }
        let us = start.elapsed().as_micros() as f64 / repeats as f64;
        assert_eq!(total, seq_total, "fanout={fanout} must count the same rows as sequential");
        println!("{:>8} {:>14.1} {:>7.2}x", fanout, us, seq_us / us);
    }

    // Compare against the *real* production path (q1_parallel, going
    // through try_dispatch's busy check) on the exact same data, to see
    // whether try_dispatch's has_spare_capacity() check is itself costing
    // speedup in a tight back-to-back single-thread loop (each call's own
    // last-received job may not have had its `in_flight` decrement land
    // yet when the very next call checks capacity).
    // `Some(1)`: this one caller should get the whole 2-worker pool
    // (matching "pool=2" below), same as `fair_query_fanout`'s "unknown"
    // default would give anyway at this size.
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
        "n/a", q1_parallel_us, seq_us / q1_parallel_us
    );
    db.disable_scan_pool(Table::OrderLine);

    tx.commit();
}

/// `ScanWorkerPool::fair_query_fanout` unit coverage — the core arithmetic
/// behind `DriverConfig::scan_pool_workers`'s whole sizing design (see that
/// field's doc) — checked directly rather than only indirectly through
/// `q1_parallel`/`q6_parallel`'s own use of it.
#[test]
fn fair_query_fanout_returns_two_when_expected_concurrent_queries_is_unknown() {
    let db = TpccDatabase::new(RootIndexType::default());
    let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), 10, None);
    assert_eq!(pool.fair_query_fanout(), Some(2));
}

#[test]
fn fair_query_fanout_treats_zero_expected_callers_as_unknown() {
    let db = TpccDatabase::new(RootIndexType::default());
    let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), 10, Some(0));
    assert_eq!(pool.fair_query_fanout(), Some(2));
}

#[test]
fn fair_query_fanout_divides_pool_capacity_evenly_across_expected_callers() {
    let db = TpccDatabase::new(RootIndexType::default());
    for &(num_workers, expected_callers, want) in &[(32usize, 4usize, 8usize), (16, 2, 8), (100, 10, 10), (9, 3, 3)] {
        let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), num_workers, Some(expected_callers));
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

/// `TpccDatabase::enable_scan_pool`'s doc says this panics for
/// `Warehouse`/`District` (they live outside `db`'s own table list — see
/// `TreeClass`'s doc) rather than silently misbehaving through
/// `table_ids`' meaningless `0` for those two — never actually exercised
/// by any other test.
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

/// Two different tables' pools are entirely independent state — assigning
/// or dropping one must never touch the other, since they're separate
/// slots in `bat_db::Database`'s own `scan_pools` vector (indexed by
/// `TableId`), not a single shared "the" pool.
#[test]
fn multiple_tables_can_have_independent_scan_pools_at_once() {
    let db = TpccDatabase::new(RootIndexType::default());
    db.enable_scan_pool(Table::OrderLine, 4, Some(2));
    db.enable_scan_pool(Table::Item, 10, Some(2));

    let order_line_pool = db.scan_pool(Table::OrderLine).expect("OrderLine pool should be assigned");
    let item_pool = db.scan_pool(Table::Item).expect("Item pool should be assigned");
    assert_eq!(order_line_pool.num_workers(), 4);
    assert_eq!(item_pool.num_workers(), 10);
    assert_eq!(order_line_pool.fair_query_fanout(), Some(2));
    assert_eq!(item_pool.fair_query_fanout(), Some(5));

    db.disable_scan_pool(Table::OrderLine);
    assert!(db.scan_pool(Table::OrderLine).is_none(), "disabling OrderLine's pool must not touch Item's");
    assert!(db.scan_pool(Table::Item).is_some(), "Item's own pool must still be assigned");

    db.disable_scan_pool(Table::Item);
    assert!(db.scan_pool(Table::Item).is_none());
}

/// Re-`enable_scan_pool`-ing a table replaces its pool outright rather
/// than layering a second one on top — and a caller that already grabbed
/// the *old* `Arc<ScanWorkerPool>` before the replacement must keep
/// working correctly regardless: its worker threads only exit once every
/// last `Arc` clone (including one a caller is still holding) is dropped,
/// not the instant a different pool object takes its place in `db`'s own
/// slot.
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
    assert_eq!(new_pool.num_workers(), 6, "re-enabling must replace, not add to, the old pool");

    let (actual, _) = q1_parallel(&db, &old_pool, num_warehouses, 100);
    assert_eq!(actual.len(), expected.len());
    for (a, e) in actual.iter().zip(expected.iter()) {
        assert_eq!(a.count, e.count);
        assert_eq!(a.sum_qty, e.sum_qty);
        assert!((a.sum_amount - e.sum_amount).abs() < 1e-9);
    }

    db.disable_scan_pool(Table::OrderLine);
}

/// A heavier version of `shared_scan_pool_serves_concurrent_callers_correctly`:
/// 20 threads hammering one shared, modestly-sized pool concurrently for
/// several iterations each, mixing `q1_parallel`/`q6_parallel`, every
/// single result checked against the sequential answer — a stronger
/// correctness signal than a handful of threads/iterations before
/// committing this feature.
#[test]
fn shared_scan_pool_stays_correct_under_heavy_concurrent_load() {
    // 20 test threads each act as their own OLAP thread (each permanently
    // registers its own real `WorkerId` via `q1_parallel`/`q6_parallel`'s
    // own `TpccTxn::begin`, unlike the pool's own worker threads — see
    // `bat_sync::worker::READ_ONLY_SCAN_WORKER_ID`'s doc) — `TpccDatabase::
    // new`'s default `max_workers` (`num_cpus`) isn't guaranteed to have
    // room for 20 + this test's own thread on every machine this runs on,
    // so size it explicitly rather than relying on the host having enough
    // cores.
    let db = TpccDatabase::new_with_big_tree_size_and_max_workers(RootIndexType::default(), BigTreeSize::default(), 32);
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
