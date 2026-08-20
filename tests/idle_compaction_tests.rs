//! Correctness check for idle/proactive compaction
//! (`bat_tree::idle_compaction::MVBTSt::compact_idle_pass`): a leaf that
//! accumulates heavy garbage purely from repeated updates to the *same*
//! key - never enough to physically overflow the leaf, so the ordinary
//! write-triggered `on_overflow_node`/`split` path never runs - should
//! still get cleaned up once `compact_idle_pass` runs, exactly as if it
//! had overflowed for real.

use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_tree::mvbt::MVBT;

/// Reads every leaf's `(active, dead)` count reachable from `tree`'s
/// current root - the same structural walk `compact_idle_pass`'s own
/// candidate scan uses - and sums them, so this test can check the raw
/// physical garbage count directly instead of depending on any
/// diagnostic-only counter (`bat_test::SCAN_TRACE` is compiled out by
/// default and isn't meant for per-test assertions anyway).
fn total_active_dead(tree: &MVBT) -> (u64, u64) {
    let (mut active_sum, mut dead_sum) = (0u64, 0u64);
    let range = Interval::new(tree.cold.min_key, tree.cold.max_key);
    RangeQueryIter::new(tree, tree.current_version(), range, true, tree.worker_id())
        .for_each_leaf_ratio(|_, active, dead| {
            active_sum += active as u64;
            dead_sum += dead as u64;
        });
    (active_sum, dead_sum)
}

/// Per-leaf breakdown, for tests that need one specific leaf's own ratio
/// (not the whole tree's) - e.g. a leaf shared with several untouched
/// neighbor keys dilutes the *global* dead/live ratio well below what that
/// one leaf's own `active_dead_count()` shows, so `compact_idle_pass`'s
/// actual per-leaf threshold check can't be inferred from
/// `total_active_dead` alone.
fn leaf_active_dead_counts(tree: &MVBT) -> Vec<(Interval<u64>, u32, u32)> {
    let mut leaves = Vec::new();
    let range = Interval::new(tree.cold.min_key, tree.cold.max_key);
    RangeQueryIter::new(tree, tree.current_version(), range, true, tree.worker_id())
        .for_each_leaf_ratio(|fence, active, dead| leaves.push((fence, active, dead)));
    leaves
}

#[test]
fn compact_idle_pass_drops_garbage_from_repeated_updates_to_one_key_without_ever_overflowing() {
    let tree = MVBT::make_standard(RootIndexType::default());
    let key = 42u64;
    let payload_counter = AtomicU64::new(0);

    match tree.dispatch_crud(CRUDOperation::Insert(key, payload_counter.fetch_add(1, SeqCst))) {
        CRUDOperationResult::Inserted(_) => {}
        _ => panic!("expected Inserted"),
    }

    // Well under this tree's 123-slot leaf capacity (`FAN_OUT`/`NUM_RECORDS`
    // in `bat_tree::mvbt`), so the ordinary write path's overflow check
    // never trips - every one of these updates just appends another dead
    // version onto the same leaf.
    for _ in 0..80 {
        match tree.dispatch_crud(CRUDOperation::Update(key, payload_counter.fetch_add(1, SeqCst))) {
            CRUDOperationResult::Updated(_) => {}
            _ => panic!("expected Updated"),
        }
    }

    let (active_before, dead_before) = total_active_dead(&tree);
    assert_eq!(active_before, 1, "exactly one live version of the one key");
    assert_eq!(dead_before, 80, "one dead version per update, never compacted");

    let compacted = tree.compact_idle_pass(0.5);
    assert_eq!(compacted, 1, "the one garbage-heavy leaf should be compacted");

    let (active_after, dead_after) = total_active_dead(&tree);
    assert_eq!(active_after, 1, "compaction must not lose the live record");
    assert_eq!(dead_after, 0, "every dead, GC-eligible version should be dropped");

    // The compacted leaf no longer qualifies - a second pass is a no-op.
    assert_eq!(tree.compact_idle_pass(0.5), 0);

    // The live record itself must still read back correctly after compaction.
    match tree.dispatch_crud(CRUDOperation::PointSi(key)) {
        CRUDOperationResult::MatchedRecords(records) => {
            assert_eq!(records.len(), 1);
            assert_eq!(*records[0].payload, 80);
        }
        _ => panic!("expected MatchedRecords"),
    }
}

/// A leaf below `dead_ratio_threshold` must be left untouched - compaction
/// is opt-in per leaf, not a blanket "always compact whatever it finds".
#[test]
fn compact_idle_pass_skips_leaves_below_threshold() {
    let tree = MVBT::make_standard(RootIndexType::default());
    let key = 7u64;
    let payload_counter = AtomicU64::new(0);

    match tree.dispatch_crud(CRUDOperation::Insert(key, payload_counter.fetch_add(1, SeqCst))) {
        CRUDOperationResult::Inserted(_) => {}
        _ => panic!("expected Inserted"),
    }
    // Just one dead version out of two total (50% ratio) - below a strict
    // 0.9 threshold, so this leaf shouldn't qualify.
    match tree.dispatch_crud(CRUDOperation::Update(key, payload_counter.fetch_add(1, SeqCst))) {
        CRUDOperationResult::Updated(_) => {}
        _ => panic!("expected Updated"),
    }

    assert_eq!(tree.compact_idle_pass(0.9), 0);
    let (active, dead) = total_active_dead(&tree);
    assert_eq!((active, dead), (1, 1), "untouched: still below threshold");
}

/// Same check as the first test, but on a multi-leaf tree (enough distinct
/// keys to force real internal pages) - exercises
/// `traversal_compact_internal_olc`'s non-root branch (forcing
/// `on_overflow_node` from a *parent*), not just
/// `retrieve_root_compact_internal_olc`'s root-is-a-leaf special case.
/// Every other key must stay readable with its original value: compaction
/// must touch only the one leaf that actually qualified.
#[test]
fn compact_idle_pass_on_a_multi_leaf_tree_touches_only_the_garbage_heavy_leaf() {
    let tree = MVBT::make_standard(RootIndexType::default());
    let payload_counter = AtomicU64::new(0);

    // Spread far enough apart, and numerous enough, to force several real
    // splits (leaf capacity is 123 records - see `bat_tree::mvbt`).
    let other_keys: Vec<u64> = (0..300u64).map(|i| i * 1000).collect();
    for &k in &other_keys {
        match tree.dispatch_crud(CRUDOperation::Insert(k, payload_counter.fetch_add(1, SeqCst))) {
            CRUDOperationResult::Inserted(_) => {}
            _ => panic!("expected Inserted"),
        }
    }

    let hot_key = other_keys[150];
    // Small enough that even a leaf already packed near its 123-slot
    // capacity from the surrounding inserts alone can't hit a *real*
    // overflow from these updates - this test wants every one of these
    // dead versions to survive untouched until `compact_idle_pass` runs,
    // not partly flushed by the ordinary write path along the way.
    const HOT_UPDATES: u64 = 10;
    for _ in 0..HOT_UPDATES {
        match tree.dispatch_crud(CRUDOperation::Update(hot_key, payload_counter.fetch_add(1, SeqCst))) {
            CRUDOperationResult::Updated(_) => {}
            _ => panic!("expected Updated"),
        }
    }

    let (active_before, dead_before) = total_active_dead(&tree);
    assert_eq!(active_before, other_keys.len() as u64);
    assert_eq!(dead_before, HOT_UPDATES);

    // The hot leaf's *own* dead ratio is almost certainly diluted well
    // below 0.5 by however many untouched neighbor keys share it (the
    // global sums above can't tell us that ratio - see
    // `leaf_active_dead_counts`'s doc) - so derive the threshold from that
    // one leaf's actual counts instead of guessing a fixed number.
    let leaves_before = leaf_active_dead_counts(&tree);
    let hot_leaf = leaves_before
        .iter()
        .find(|(fence, ..)| fence.contains(hot_key))
        .expect("hot_key must be covered by some leaf");
    let (_, hot_active, hot_dead) = *hot_leaf;
    assert_eq!(hot_dead as u64, HOT_UPDATES, "all of it landed on the hot leaf");
    let hot_ratio = hot_dead as f64 / (hot_active + hot_dead) as f64;
    // Every other leaf must still show zero dead entries.
    for (fence, _, dead) in &leaves_before {
        if !fence.contains(hot_key) {
            assert_eq!(*dead, 0, "no other leaf should have accumulated garbage");
        }
    }

    let threshold = hot_ratio - 0.001;
    assert_eq!(tree.compact_idle_pass(threshold), 1, "exactly the one garbage-heavy leaf");

    let (active_after, dead_after) = total_active_dead(&tree);
    assert_eq!(active_after, other_keys.len() as u64, "no live record lost");
    assert_eq!(dead_after, 0);

    // Every key, hot or not, must still read back its latest value.
    for (i, &k) in other_keys.iter().enumerate() {
        let expected = if k == hot_key {
            other_keys.len() as u64 - 1 + HOT_UPDATES
        } else {
            i as u64
        };
        match tree.dispatch_crud(CRUDOperation::PointSi(k)) {
            CRUDOperationResult::MatchedRecords(records) => {
                assert_eq!(records.len(), 1, "key {k}");
                assert_eq!(*records[0].payload, expected, "key {k}");
            }
            _ => panic!("expected MatchedRecords for key {k}"),
        }
    }
}
