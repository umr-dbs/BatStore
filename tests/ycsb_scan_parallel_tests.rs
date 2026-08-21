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
use crate::bat_root::index_root::RootIndexType;
use std::time::{Duration, Instant};
use triomphe::Arc;

fn tiny_cfg() -> YcsbConfig {
    YcsbConfig { record_count: 300, field_count: 3, field_length: 8 }
}

/// `scan_parallel` must agree with the sequential `scan_with_mode` for a
/// range wide enough to clear `bat_tree::scan_pool::
/// MIN_LEN_FOR_SPLIT_DISPATCH` (65,536) — deliberately scanning far past
/// the populated `record_count` (300) so this test stays cheap to set up
/// while the *range itself* is still large enough to exercise the real
/// split-and-dispatch path, not just its own size gate's fallback.
#[test]
fn scan_parallel_matches_sequential_scan_for_a_large_range() {
    let cfg = tiny_cfg();
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    populate(&tree, &cfg);

    let pool = YcsbScanPool::spawn(tree.clone(), 4, Some(1));
    let len = 100_000u64;

    let expected = scan_with_mode(&tree, 1, len, true);
    let actual = scan_parallel(&pool, &tree, 1, len, true);
    assert_eq!(actual, expected, "scan_parallel must find exactly the same rows as scan_with_mode");
    assert_eq!(expected, cfg.record_count as usize, "sanity: every populated row should be within [1, len]");
}

/// Confirms `scan_parallel`'s large-range case genuinely takes
/// `dispatch_evenly`'s split-and-dispatch branch rather than happening to
/// match the sequential answer via its own fallback: the same range/pool
/// combination `scan_parallel_matches_sequential_scan_for_a_large_range`
/// uses must both (a) clear `RangeSplit::approx_len`'s size gate and (b)
/// have `dispatch_evenly` itself report `Some`, not `None`.
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

/// Same as the large-range test above, but densely populated across the
/// *whole* scanned range (not just its first sliver) — so every one of
/// the pool's sub-ranges actually has real matching rows to find and sum,
/// not just the first one, which is what would actually catch a bug in
/// how `dispatch_evenly`'s per-sub-range results get aggregated.
#[test]
fn scan_parallel_matches_sequential_scan_with_dense_data_across_every_sub_range() {
    let cfg = YcsbConfig { record_count: 80_000, field_count: 3, field_length: 8 };
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    populate(&tree, &cfg);

    let pool = YcsbScanPool::spawn(tree.clone(), 8, Some(2)); // fair share = 4 sub-ranges
    assert_eq!(pool.fair_query_fanout(), Some(4));

    let len = cfg.record_count;
    let expected = scan_with_mode(&tree, 1, len, true);
    let actual = scan_parallel(&pool, &tree, 1, len, true);
    assert_eq!(expected, cfg.record_count as usize, "sanity: every populated row is within [1, len]");
    assert_eq!(actual, expected);
}

/// Same correctness check for a range below the size gate (so
/// `scan_parallel` falls straight back to `scan_with_mode` internally) —
/// the fallback path itself must still be correct, not just "doesn't
/// crash".
#[test]
fn scan_parallel_matches_sequential_scan_for_a_small_range() {
    let cfg = tiny_cfg();
    let tree = Arc::new(YcsbTree::make_standard(RootIndexType::default()));
    populate(&tree, &cfg);

    let pool = YcsbScanPool::spawn(tree.clone(), 4, Some(1));
    let len = 100u64;

    let expected = scan_with_mode(&tree, 1, len, true);
    let actual = scan_parallel(&pool, &tree, 1, len, true);
    assert_eq!(actual, expected);
    assert_eq!(expected, 100, "all 100 keys in [1, 100] were populated");
}

/// Proves the size gate actually bypasses the pool for a small range,
/// rather than merely happening to return the right answer: occupies
/// every worker with an artificially slow filler job first, then confirms
/// a small-range `scan_parallel` call still returns almost immediately
/// instead of waiting behind (or being queued alongside) that filler —
/// which it could only do by never touching the pool at all, since
/// `try_dispatch` itself (used once past the size gate) would otherwise
/// either queue behind the busy workers or run inline no faster than the
/// filler's own `hold`.
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
        assert!(!pool.has_spare_capacity(), "both workers should be busy with the slow filler jobs");

        let start = Instant::now();
        let count = scan_parallel(&pool, &tree, 1, 100, true);
        let elapsed = start.elapsed();
        assert_eq!(count, 100);
        assert!(
            elapsed < hold / 2,
            "a small-range scan_parallel call should skip the pool (and so the busy filler) \
             entirely rather than queue behind it or run inline through try_dispatch, took {elapsed:?}"
        );
    });
}
