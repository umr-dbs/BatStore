//! Correctness checks for the "S-HTAP" (streaming HTAP) benchmark harness
//! (`bat_bench::s_htap_random`/`s_htap_txn`): a tiny table, exercising the
//! workload's two write-side behaviors (a genuinely new arrival vs. a
//! late-arrival upsert colliding with an already-materialized key) and its
//! OLAP scan path against real row content, not just `Ok`/`Err`.
//!
//! The last test below is a direct regression test for the hot/cold
//! double-counting bug found and fixed in `bat_query::iter_query`'s cold-chain
//! range-scan path (2026-08-15/16, see `RangeQueryIter::walk_cold_chain_for_range`):
//! it forces enough hot-tail churn to build real cold chains via
//! `VERSION_SPLIT`, then asserts a straddling scan returns exactly one row
//! per live key — a regression would show up here as a scan count exceeding
//! the range's live-key count.

use crate::bat_bench::s_htap_random::{HotTailSampler, olap_scan_bounds};
use crate::bat_bench::s_htap_txn::arrival_upsert;
use crate::bat_bench::ycsb_load::populate;
use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbTree};
use crate::bat_bench::ycsb_txn::{self, YcsbExecutionMode};
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_root::index_root::RootIndexType;

fn tiny_cfg() -> YcsbConfig {
    YcsbConfig {
        record_count: 50,
        field_count: 3,
        field_length: 8,
    }
}

fn read_bytes(tree: &YcsbTree, key: u64) -> Option<Vec<u8>> {
    match tree.dispatch_crud(CRUDOperation::PointSi(key)) {
        CRUDOperationResult::MatchedRecords(v) => v.first().map(|r| r.payload.as_bytes().to_vec()),
        other => panic!("s_htap test: unexpected point result: {other}"),
    }
}

/// A key beyond every cold-loaded/previously-minted key is a genuine new
/// arrival: `arrival_upsert` must report `true` and the row must be readable
/// afterward with the configured shape.
#[test]
fn arrival_upsert_on_a_fresh_key_inserts_a_correctly_shaped_row() {
    let cfg = tiny_cfg();
    let tree = YcsbTree::make_standard(RootIndexType::default());
    populate(&tree, &cfg);

    let fresh_key = cfg.record_count + 1;
    assert!(read_bytes(&tree, fresh_key).is_none());

    let was_new = arrival_upsert(&tree, &cfg, fresh_key, false, YcsbExecutionMode::Atomic);
    assert!(
        was_new,
        "a never-seen key must be reported as a new arrival"
    );

    let bytes = read_bytes(&tree, fresh_key).expect("fresh arrival must be readable");
    assert_eq!(bytes.len(), cfg.field_count * cfg.field_length);
}

/// A "late" event whose (possibly jittered) key collides with an
/// already-materialized cold row is a legitimate upsert, not an error:
/// `arrival_upsert` must report `false` and must actually rewrite the row's
/// content rather than leaving the original bytes or losing the key.
#[test]
fn arrival_upsert_on_an_already_materialized_key_upserts_instead_of_panicking() {
    let cfg = tiny_cfg();
    let tree = YcsbTree::make_standard(RootIndexType::default());
    populate(&tree, &cfg);

    let existing_key = 7u64;
    let before = read_bytes(&tree, existing_key).expect("key must be loaded by populate");

    let was_new = arrival_upsert(&tree, &cfg, existing_key, true, YcsbExecutionMode::Atomic);
    assert!(
        !was_new,
        "a late arrival colliding with an existing key must not be reported as new"
    );

    let after = read_bytes(&tree, existing_key).expect("key must still exist after the upsert");
    assert_eq!(after.len(), cfg.field_count * cfg.field_length);
    assert_ne!(
        before, after,
        "the late-arrival upsert (write_all_fields=true) must actually rewrite the row"
    );
}

/// Same late-arrival-collision behavior, but through the `Transaction`
/// execution mode's registered-snapshot path instead of `Atomic`'s
/// single-operation path — both are exercised by the real driver depending
/// on CLI config, and must agree on outcome.
#[test]
fn arrival_upsert_upserts_correctly_under_transaction_execution_mode() {
    let cfg = tiny_cfg();
    let tree = YcsbTree::make_standard(RootIndexType::default());
    populate(&tree, &cfg);

    let existing_key = 12u64;
    let before = read_bytes(&tree, existing_key).unwrap();

    let was_new = arrival_upsert(
        &tree,
        &cfg,
        existing_key,
        true,
        YcsbExecutionMode::Transaction,
    );
    assert!(!was_new);

    let after = read_bytes(&tree, existing_key).unwrap();
    assert_ne!(before, after);

    let fresh_key = cfg.record_count + 5;
    let was_new = arrival_upsert(
        &tree,
        &cfg,
        fresh_key,
        false,
        YcsbExecutionMode::Transaction,
    );
    assert!(was_new);
    assert!(read_bytes(&tree, fresh_key).is_some());
}

/// A hot-tail update (`ycsb_txn::update_with_execution_mode`, driven by
/// `HotTailSampler`) must land on a key within the sampler's configured
/// window and actually change that row's content.
#[test]
fn hot_tail_sample_targets_the_window_and_the_update_changes_content() {
    let cfg = tiny_cfg();
    let tree = YcsbTree::make_standard(RootIndexType::default());
    populate(&tree, &cfg);

    let window = 10u64;
    let sampler = HotTailSampler::new(0.99, window);
    let key = sampler.sample(cfg.record_count);
    assert!(key <= cfg.record_count && key > cfg.record_count - window);

    let before = read_bytes(&tree, key).unwrap();
    assert!(ycsb_txn::update_with_execution_mode(
        &tree,
        &cfg,
        key,
        false,
        YcsbExecutionMode::Atomic
    ));
    let after = read_bytes(&tree, key).unwrap();
    assert_ne!(before, after);
}

/// Regression test for the hot/cold double-counting bug in
/// `RangeQueryIter::walk_cold_chain_for_range` (fixed 2026-08-15/16): forces
/// real cold-chain creation by hammering a narrow hot-tail window with far
/// more updates than one leaf's capacity, under GC, then scans a range that
/// straddles the cold/hot boundary. Every key in `1..=record_count` is live
/// (never deleted) throughout, so a correct scan must return exactly
/// `record_count` rows — a hot/cold dedup regression would instead
/// over-count by returning two rows for some hot-and-cold-linked key.
#[test]
fn olap_scan_over_a_hot_cold_straddling_range_counts_each_live_key_exactly_once() {
    let cfg = YcsbConfig {
        record_count: 200,
        field_count: 2,
        field_length: 8,
    };
    let tree = YcsbTree::make_standard(RootIndexType::default());
    tree.enable_gc(false);
    populate(&tree, &cfg);

    let hot_window = 20u64;
    let sampler = HotTailSampler::new(0.99, hot_window);

    // Far more updates than a single leaf's capacity, concentrated on the
    // last `hot_window` keys - enough to force repeated VERSION_SPLIT
    // compactions (and, per the 08-14 cold-page-chain work, real cold-chain
    // construction) on those leaves specifically.
    for _ in 0..2_000 {
        let key = sampler.sample(cfg.record_count);
        assert!(ycsb_txn::update_with_execution_mode(
            &tree,
            &cfg,
            key,
            false,
            YcsbExecutionMode::Atomic
        ));
    }

    let (lo, len) = olap_scan_bounds(cfg.record_count, 0, cfg.record_count * 2);
    let scanned = ycsb_txn::scan_with_mode(&tree, lo, len, true);
    assert_eq!(
        scanned, cfg.record_count as usize,
        "a straddling scan must return exactly one row per live key, no duplicates"
    );
}
