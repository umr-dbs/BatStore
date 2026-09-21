//! Correctness (and, deliberately, an *imbalance* demonstration) for
//! `bat_query::interval::RangeSplit`'s `u64` implementation and
//! `bat_tree::scan_pool::ScanWorkerPool::dispatch_evenly`, the generic
//! "split this range and run it through the pool" entry point built on
//! top of it. See `RangeSplit`'s own doc for the hazard these last two
//! tests exist to make concrete rather than just assert in prose: numeric
//! bisection is safe over a *tight* range, but not over a bit-packed
//! key's full type-level span.

use crate::bat_bench::tpcc_schema::{
    OrderLine, Table, TpccDatabase, TpccRow, k_order_line, order_line_table_range,
};
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_query::interval::{Interval, RangeSplit};
use crate::bat_root::index_root::RootIndexType;
use crate::bat_sync::worker::READ_ONLY_SCAN_WORKER_ID;

fn insert(db: &TpccDatabase, table: Table, key: u64, row: TpccRow) {
    match db
        .tree_for(table)
        .dispatch_crud(CRUDOperation::Insert(key, row))
    {
        CRUDOperationResult::Inserted(_) => {}
        other => panic!(
            "interval_range_split_tests fixture: unexpected insert result for key {key}: {other}"
        ),
    }
}

fn order_line_row() -> TpccRow {
    TpccRow::OrderLine(Box::new(OrderLine {
        ol_i_id: 1,
        ol_supply_w_id: 1,
        ol_delivery_d: Some(1),
        ol_quantity: 1,
        ol_amount: 1.0,
        ol_dist_info: [0u8; 24],
    }))
}

/// Every value in `range` must end up in exactly one of the returned
/// sub-intervals (a genuinely empty/inverted one contributes nothing),
/// contiguous and in order, with no gaps or overlaps.
fn assert_partitions_range_exactly(range: Interval<u64>, parts: &[Interval<u64>]) {
    let mut expected_next = range.lower;
    for (i, part) in parts.iter().enumerate() {
        if part.lower > part.upper {
            continue; // genuinely empty slot - contributes nothing, skip
        }
        assert_eq!(
            part.lower, expected_next,
            "part {i} doesn't start where the previous one ended"
        );
        expected_next = part.upper.checked_add(1).unwrap_or(u64::MAX);
    }
    assert_eq!(
        parts
            .last()
            .unwrap()
            .upper
            .max(range.lower.saturating_sub(1)),
        range.upper,
        "last non-empty part must reach range.upper"
    );
}

#[test]
fn split_evenly_covers_the_full_range_with_no_gaps_or_overlaps() {
    let range = Interval::new(0u64, 999u64);
    let parts = u64::split_evenly(range, 7).expect("a non-empty range should always split");
    assert_eq!(parts.len(), 7, "must always return exactly `fanout` pieces");
    assert_partitions_range_exactly(range, &parts);

    let total: u64 = parts
        .iter()
        .filter(|p| p.lower <= p.upper)
        .map(|p| p.upper - p.lower + 1)
        .sum();
    assert_eq!(
        total, 1000,
        "every one of the 1000 values must land in exactly one piece"
    );
}

#[test]
fn split_evenly_pads_with_empty_intervals_when_fanout_exceeds_the_span() {
    let range = Interval::new(10u64, 12u64); // 3 values
    let parts = u64::split_evenly(range, 10).unwrap();
    assert_eq!(
        parts.len(),
        10,
        "still exactly `fanout` pieces, most of them empty"
    );

    let non_empty: Vec<_> = parts.iter().filter(|p| p.lower <= p.upper).collect();
    assert_eq!(
        non_empty.len(),
        3,
        "only as many non-empty pieces as there are values"
    );
    let total: u64 = non_empty.iter().map(|p| p.upper - p.lower + 1).sum();
    assert_eq!(total, 3);
}

#[test]
fn split_evenly_handles_the_full_u64_domain_without_overflow() {
    let range = Interval::new(u64::MIN, u64::MAX);
    let parts =
        u64::split_evenly(range, 4).expect("must not panic/overflow on the widest possible range");
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0].lower, u64::MIN);
    assert_eq!(parts[3].upper, u64::MAX);
    assert_partitions_range_exactly(range, &parts);
}

#[test]
fn split_evenly_returns_none_for_an_already_empty_range() {
    let inverted = Interval::new(5u64, 1u64); // lower > upper: this codebase's "empty" convention
    assert!(u64::split_evenly(inverted, 4).is_none());
}

#[test]
fn split_evenly_at_fanout_one_returns_the_whole_range_unsplit() {
    let range = Interval::new(3u64, 300u64);
    let parts = u64::split_evenly(range, 1).unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].lower, range.lower);
    assert_eq!(parts[0].upper, range.upper);
}

#[test]
fn dispatch_evenly_is_correct_but_badly_imbalanced_over_order_lines_full_sentinel_range() {
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 3u32;
    let rows_per_warehouse = 20u32;
    for w in 1..=num_warehouses {
        for i in 0..rows_per_warehouse {
            insert(
                &db,
                Table::OrderLine,
                k_order_line(w, 1, i, 1),
                order_line_row(),
            );
        }
    }

    db.enable_scan_pool(Table::OrderLine, 8, Some(1));
    let pool = db.scan_pool(Table::OrderLine).unwrap();
    let ts_start = db.current_version();

    let counts = pool
        .dispatch_evenly(order_line_table_range(), move |tree, range| {
            let mut count = 0u64;
            crate::bat_query::iter_query::RangeQueryIter::new(
                tree,
                ts_start,
                range,
                false,
                READ_ONLY_SCAN_WORKER_ID,
            )
            .for_each_ref(|_, _| count += 1);
            count
        })
        .expect("a non-empty pool with a non-empty range must always dispatch");

    assert_eq!(counts.len(), 8);
    let total: u64 = counts.iter().sum();
    assert_eq!(
        total,
        (num_warehouses * rows_per_warehouse) as u64,
        "every row must still be counted exactly once"
    );

    assert_eq!(
        counts[0], total,
        "all real data lands in the first numeric bucket"
    );
    assert!(
        counts[1..].iter().all(|&c| c == 0),
        "every other bucket gets nothing to do: {counts:?}"
    );

    db.disable_scan_pool(Table::OrderLine);
}

#[test]
fn dispatch_evenly_is_balanced_when_given_a_tight_real_range() {
    let db = TpccDatabase::new(RootIndexType::default());
    let num_warehouses = 4u32;
    let rows_per_warehouse = 20u32;
    for w in 1..=num_warehouses {
        for i in 0..rows_per_warehouse {
            insert(
                &db,
                Table::OrderLine,
                k_order_line(w, 1, i, 1),
                order_line_row(),
            );
        }
    }

    db.enable_scan_pool(Table::OrderLine, 4, Some(1));
    let pool = db.scan_pool(Table::OrderLine).unwrap();
    let ts_start = db.current_version();

    let tight_range = Interval::new(
        k_order_line(1, 0, 0, 0),
        k_order_line(num_warehouses + 1, 0, 0, 0) - 1,
    );
    let counts = pool
        .dispatch_evenly(tight_range, move |tree, range| {
            let mut count = 0u64;
            crate::bat_query::iter_query::RangeQueryIter::new(
                tree,
                ts_start,
                range,
                false,
                READ_ONLY_SCAN_WORKER_ID,
            )
            .for_each_ref(|_, _| count += 1);
            count
        })
        .unwrap();

    let total: u64 = counts.iter().sum();
    assert_eq!(total, (num_warehouses * rows_per_warehouse) as u64);
    assert!(
        counts.iter().all(|&c| c == rows_per_warehouse as u64),
        "each of the 4 buckets should land on exactly one warehouse's rows: {counts:?}"
    );
}
