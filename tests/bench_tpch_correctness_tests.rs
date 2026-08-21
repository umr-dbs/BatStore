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
    BigTreeSize, Order, OrderLine, Stock, Supplier, Table, TpccDatabase, TpccRow, k_order, k_order_line, k_stock,
    order_line_table_range,
};
use crate::bat_bench::parallel_scan::{q1_parallel, q6_parallel};
use crate::bat_bench::tpcc_schema::TpccScanWorkerPool as ScanWorkerPool;
use crate::bat_bench::tpch_queries::{q1, q4, q5, q6};
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_root::index_root::RootIndexType;
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
        let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), fanout);
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
        let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), fanout);
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

    db.enable_scan_pool(Table::OrderLine, 3);
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
    db.enable_scan_pool(Table::OrderLine, 2);

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

    let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), 2);
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

    db.enable_scan_pool(Table::OrderLine, 10);
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
/// "Partitioning" doc for why a query only ever asks for `QUERY_FANOUT`
/// (2) workers rather than the pool's entire capacity: with a 6-worker
/// pool, 3 query-sized (2-worker) slices should all get serviced by the
/// pool's workers at the same time, not one after another. Uses the same
/// slow-filler-job technique as `try_dispatch_runs_inline_when_the_pool_has_
/// no_spare_capacity` (`pool.dispatch` directly, bypassing q1/q6-specific
/// logic) to make "did this run concurrently" a simple wall-clock check:
/// 3 batches of 2 jobs each, each job sleeping `hold`, all submitted at
/// once — if the pool actually shares its 6 workers across all 3 batches,
/// total wall time is ~1 `hold`; if it silently serialized them, ~3.
#[test]
fn scan_pool_serves_multiple_query_sized_slices_concurrently() {
    let db = TpccDatabase::new(RootIndexType::default());
    let pool = ScanWorkerPool::spawn(db.tree_for(Table::OrderLine), 6);
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
