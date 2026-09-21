//! Correctness for `bat_bench::ycsb_txn::scan_parallel` — the first real
//! (non-test-only) caller of `bat_tree::scan_pool::ScanWorkerPool::
//! dispatch_evenly`'s generic, no-hand-written-partitioner path. `YcsbKey`
//! is a plain dense sequential id (unlike `tpcc_schema::TpccKey`'s
//! bit-packed fields), so `bat_query::interval::RangeSplit`'s numeric
//! `u64` bisection is safe here without the caveats documented for
//! `ORDER_LINE` — see `RangeSplit`'s own doc and
//! `tests/interval_range_split_tests.rs`'s imbalance demonstration for why
//! that distinction matters.

use crate::bat_bench::ycsb_load::populate;
use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbKey, YcsbScanPool, YcsbTree};
use crate::bat_bench::ycsb_txn::{scan_parallel, scan_with_mode};
use crate::bat_query::interval::{Interval, RangeSplit};
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_sync::worker::READ_ONLY_SCAN_WORKER_ID;
use std::time::{Duration, Instant};
use triomphe::Arc;

fn tiny_cfg() -> YcsbConfig {
    YcsbConfig {
        record_count: 300,
        field_count: 3,
        field_length: 8,
    }
}

#[test]
fn scan_parallel_matches_sequential_scan_for_a_large_range() {
    let cfg = tiny_cfg();
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    populate(&tree, &cfg);

    let pool = YcsbScanPool::spawn(tree.clone(), 4, Some(1));
    let len = 100_000u64;

    let expected = scan_with_mode(&tree, 1, len, true);
    let actual = scan_parallel(Some(&pool), &tree, 1, len, true);
    assert_eq!(
        actual, expected,
        "scan_parallel must find exactly the same rows as scan_with_mode"
    );
    assert_eq!(
        expected, cfg.record_count as usize,
        "sanity: every populated row should be within [1, len]"
    );
}

#[test]
fn large_range_actually_uses_dispatch_evenly_not_the_fallback() {
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    let pool = YcsbScanPool::spawn(tree.clone(), 4, Some(1));
    let range = Interval::new(1u64, 100_000u64);

    assert!(
        YcsbKey::approx_len(range).unwrap() >= 65_536,
        "sanity: this range should clear the split-worthiness threshold"
    );
    let result = pool.dispatch_evenly(range, |_, _| ());
    assert!(
        result.is_some(),
        "a large range with an available fair share must be dispatched through the pool, not skipped"
    );
}

#[test]
fn scan_parallel_matches_sequential_scan_with_dense_data_across_every_sub_range() {
    let cfg = YcsbConfig {
        record_count: 80_000,
        field_count: 3,
        field_length: 8,
    };
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    populate(&tree, &cfg);

    let pool = YcsbScanPool::spawn(tree.clone(), 8, Some(2)); // fair share = 4 sub-ranges
    assert_eq!(pool.fair_query_fanout(), Some(4));

    let len = cfg.record_count;
    let expected = scan_with_mode(&tree, 1, len, true);
    let actual = scan_parallel(Some(&pool), &tree, 1, len, true);
    assert_eq!(
        expected, cfg.record_count as usize,
        "sanity: every populated row is within [1, len]"
    );
    assert_eq!(actual, expected);
}

#[test]
fn scan_parallel_matches_sequential_scan_for_a_small_range() {
    let cfg = tiny_cfg();
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    populate(&tree, &cfg);

    let pool = YcsbScanPool::spawn(tree.clone(), 4, Some(1));
    let len = 100u64;

    let expected = scan_with_mode(&tree, 1, len, true);
    let actual = scan_parallel(Some(&pool), &tree, 1, len, true);
    assert_eq!(actual, expected);
    assert_eq!(expected, 100, "all 100 keys in [1, 100] were populated");
}

#[test]
fn scan_parallel_skips_the_pool_entirely_for_a_small_range() {
    let cfg = tiny_cfg();
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    populate(&tree, &cfg);

    let pool = YcsbScanPool::spawn(tree.clone(), 2, Some(1));
    let hold = Duration::from_millis(300);

    std::thread::scope(|scope| {
        // Occupy both workers with a slow filler job each, so the pool has
        // zero spare capacity for `hold`'s whole duration.
        for _ in 0..pool.num_workers() {
            scope.spawn(|| {
                pool.dispatch(vec![Interval::new(0u64, 0u64)], move |_, _| {
                    std::thread::sleep(hold);
                });
            });
        }
        std::thread::sleep(hold / 4);
        assert!(
            !pool.has_spare_capacity(),
            "both workers should be busy with the slow filler jobs"
        );

        let start = Instant::now();
        let count = scan_parallel(Some(&pool), &tree, 1, 100, true);
        let elapsed = start.elapsed();
        assert_eq!(count, 100);
        assert!(
            elapsed < hold / 2,
            "a small-range scan_parallel call should skip the pool (and so the busy filler) \
             entirely rather than queue behind it or run inline through try_dispatch, took {elapsed:?}"
        );
    });
}

#[test]
fn scan_parallel_with_no_pool_matches_sequential_scan_for_a_large_range() {
    let cfg = tiny_cfg();
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    populate(&tree, &cfg);
    let len = 100_000u64;

    let expected = scan_with_mode(&tree, 1, len, true);
    let actual = scan_parallel(None, &tree, 1, len, true);
    assert_eq!(
        actual, expected,
        "pool: None must still scan exactly the same rows as scan_with_mode"
    );
    assert_eq!(
        expected, cfg.record_count as usize,
        "sanity: every populated row should be within [1, len]"
    );
}

#[test]
fn range_query_iter_parallel_methods_agree_with_their_sequential_counterparts() {
    let cfg = YcsbConfig {
        record_count: 80_000,
        field_count: 3,
        field_length: 8,
    };
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    populate(&tree, &cfg);

    let pool = YcsbScanPool::spawn(tree.clone(), 8, Some(2));
    let range = Interval::new(1u64, cfg.record_count);

    let ts_collect = tree.begin_snapshot();
    let expected_rows =
        RangeQueryIter::new(&tree, ts_collect, range, false, READ_ONLY_SCAN_WORKER_ID)
            .collect_parallel(None);
    tree.on_release_reader_snapshot(ts_collect);
    let ts_collect_p = tree.begin_snapshot();
    let actual_rows =
        RangeQueryIter::new(&tree, ts_collect_p, range, false, READ_ONLY_SCAN_WORKER_ID)
            .collect_parallel(Some(&pool));
    tree.on_release_reader_snapshot(ts_collect_p);
    assert_eq!(actual_rows.len(), expected_rows.len());
    assert_eq!(actual_rows.len(), cfg.record_count as usize);

    let ts_count = tree.begin_snapshot();
    let expected_count =
        RangeQueryIter::new(&tree, ts_count, range, false, READ_ONLY_SCAN_WORKER_ID)
            .count_ref_parallel(None);
    tree.on_release_reader_snapshot(ts_count);
    let ts_count_p = tree.begin_snapshot();
    let actual_count =
        RangeQueryIter::new(&tree, ts_count_p, range, false, READ_ONLY_SCAN_WORKER_ID)
            .count_ref_parallel(Some(&pool));
    tree.on_release_reader_snapshot(ts_count_p);
    assert_eq!(actual_count, expected_count);
    assert_eq!(actual_count, cfg.record_count as usize);

    let visited_sequential = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let visited_sequential_clone = visited_sequential.clone();
    let ts_visit = tree.begin_snapshot();
    RangeQueryIter::new(&tree, ts_visit, range, false, READ_ONLY_SCAN_WORKER_ID)
        .for_each_ref_parallel(None, move |_, _| {
            visited_sequential_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
    tree.on_release_reader_snapshot(ts_visit);

    let visited_parallel = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let visited_parallel_clone = visited_parallel.clone();
    let ts_visit_p = tree.begin_snapshot();
    RangeQueryIter::new(&tree, ts_visit_p, range, false, READ_ONLY_SCAN_WORKER_ID)
        .for_each_ref_parallel(Some(&pool), move |_, _| {
            visited_parallel_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
    tree.on_release_reader_snapshot(ts_visit_p);

    assert_eq!(
        visited_sequential.load(std::sync::atomic::Ordering::Relaxed),
        cfg.record_count as usize
    );
    assert_eq!(
        visited_parallel.load(std::sync::atomic::Ordering::Relaxed),
        cfg.record_count as usize
    );
}
