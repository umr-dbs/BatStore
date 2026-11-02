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

    match tree.dispatch_crud(CRUDOperation::Insert(
        key,
        payload_counter.fetch_add(1, SeqCst),
    )) {
        CRUDOperationResult::Inserted(_) => {}
        _ => panic!("expected Inserted"),
    }

    for _ in 0..80 {
        match tree.dispatch_crud(CRUDOperation::Update(
            key,
            payload_counter.fetch_add(1, SeqCst),
        )) {
            CRUDOperationResult::Updated(_) => {}
            _ => panic!("expected Updated"),
        }
    }

    let (active_before, dead_before) = total_active_dead(&tree);
    assert_eq!(active_before, 1, "exactly one live version of the one key");
    assert_eq!(
        dead_before, 80,
        "one dead version per update, never compacted"
    );

    let compacted = tree.compact_idle_pass(0.5);
    assert_eq!(
        compacted, 1,
        "the one garbage-heavy leaf should be compacted"
    );

    let (active_after, dead_after) = total_active_dead(&tree);
    assert_eq!(active_after, 1, "compaction must not lose the live record");
    assert_eq!(
        dead_after, 0,
        "every dead, GC-eligible version should be dropped"
    );

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

    match tree.dispatch_crud(CRUDOperation::Insert(
        key,
        payload_counter.fetch_add(1, SeqCst),
    )) {
        CRUDOperationResult::Inserted(_) => {}
        _ => panic!("expected Inserted"),
    }
    // Just one dead version out of two total (50% ratio) - below a strict
    // 0.9 threshold, so this leaf shouldn't qualify.
    match tree.dispatch_crud(CRUDOperation::Update(
        key,
        payload_counter.fetch_add(1, SeqCst),
    )) {
        CRUDOperationResult::Updated(_) => {}
        _ => panic!("expected Updated"),
    }

    assert_eq!(tree.compact_idle_pass(0.9), 0);
    let (active, dead) = total_active_dead(&tree);
    assert_eq!((active, dead), (1, 1), "untouched: still below threshold");
}

#[test]
fn compact_idle_pass_on_a_multi_leaf_tree_touches_only_the_garbage_heavy_leaf() {
    let tree = MVBT::make_standard(RootIndexType::default());
    let payload_counter = AtomicU64::new(0);

    // Spread far enough apart, and numerous enough, to force several real
    // splits (leaf capacity is 123 records - see `bat_tree::mvbt`).
    let other_keys: Vec<u64> = (0..300u64).map(|i| i * 1000).collect();
    for &k in &other_keys {
        match tree.dispatch_crud(CRUDOperation::Insert(
            k,
            payload_counter.fetch_add(1, SeqCst),
        )) {
            CRUDOperationResult::Inserted(_) => {}
            _ => panic!("expected Inserted"),
        }
    }

    let hot_key = other_keys[150];
    const HOT_UPDATES: u64 = 10;
    for _ in 0..HOT_UPDATES {
        match tree.dispatch_crud(CRUDOperation::Update(
            hot_key,
            payload_counter.fetch_add(1, SeqCst),
        )) {
            CRUDOperationResult::Updated(_) => {}
            _ => panic!("expected Updated"),
        }
    }

    let (active_before, dead_before) = total_active_dead(&tree);
    assert_eq!(active_before, other_keys.len() as u64);
    assert_eq!(dead_before, HOT_UPDATES);

    let leaves_before = leaf_active_dead_counts(&tree);
    let hot_leaf = leaves_before
        .iter()
        .find(|(fence, ..)| fence.contains(hot_key))
        .expect("hot_key must be covered by some leaf");
    let (_, hot_active, hot_dead) = *hot_leaf;
    assert_eq!(
        hot_dead as u64, HOT_UPDATES,
        "all of it landed on the hot leaf"
    );
    let hot_ratio = hot_dead as f64 / (hot_active + hot_dead) as f64;
    // Every other leaf must still show zero dead entries.
    for (fence, _, dead) in &leaves_before {
        if !fence.contains(hot_key) {
            assert_eq!(*dead, 0, "no other leaf should have accumulated garbage");
        }
    }

    let threshold = hot_ratio - 0.001;
    assert_eq!(
        tree.compact_idle_pass(threshold),
        1,
        "exactly the one garbage-heavy leaf"
    );

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

/// Fewest live entries that the strong version condition allows in a new
/// leaf (`Block::strong_min_units`): half the key-split threshold of 99.
const STRONG_MIN: u32 = 49;

/// Builds a multi-leaf tree, then deletes keys from the leaf around the
/// middle key until it holds only `keep` live entries (between the weak
/// minimum of 25 and the strong minimum of 49). Returns the tree, the
/// middle key, and the keys that are still live.
fn sparse_leaf_tree(keep: usize, payload_counter: &AtomicU64) -> (MVBT, u64, Vec<u64>) {
    let tree = MVBT::make_standard(RootIndexType::default());
    let keys: Vec<u64> = (0..300u64).map(|i| i * 1000).collect();
    for &k in &keys {
        match tree.dispatch_crud(CRUDOperation::Insert(k, payload_counter.fetch_add(1, SeqCst))) {
            CRUDOperationResult::Inserted(_) => {}
            _ => panic!("expected Inserted"),
        }
    }
    let hot_key = keys[150];
    let (fence, ..) = leaf_active_dead_counts(&tree)
        .into_iter()
        .find(|(fence, ..)| fence.contains(hot_key))
        .expect("hot_key must be covered by some leaf");
    let in_leaf: Vec<u64> = keys.iter().copied().filter(|&k| fence.contains(k)).collect();
    assert!(in_leaf.len() > keep, "the leaf must hold more than {keep} keys");
    let (kept, deleted): (Vec<u64>, Vec<u64>) = {
        let start = in_leaf.iter().position(|&k| k == hot_key).unwrap().saturating_sub(keep / 2);
        let start = start.min(in_leaf.len() - keep);
        let kept = in_leaf[start..start + keep].to_vec();
        let deleted = in_leaf.iter().copied().filter(|k| !kept.contains(k)).collect();
        (kept, deleted)
    };
    for &k in &deleted {
        match tree.dispatch_crud(CRUDOperation::Delete(k)) {
            CRUDOperationResult::Deleted(_) => {}
            other => panic!("expected Deleted for {k}, got {other}"),
        }
    }
    let live: Vec<u64> = keys.iter().copied().filter(|k| !deleted.contains(k)).collect();
    let _ = kept;
    (tree, hot_key, live)
}

fn live_in_leaf_of(tree: &MVBT, key: u64) -> u32 {
    leaf_active_dead_counts(tree)
        .into_iter()
        .find(|(fence, ..)| fence.contains(key))
        .map(|(_, active, _)| active)
        .expect("key must be covered by some leaf")
}

fn assert_all_live_keys_readable(tree: &MVBT, live: &[u64]) {
    for &k in live {
        match tree.dispatch_crud(CRUDOperation::PointSi(k)) {
            CRUDOperationResult::MatchedRecords(records) => assert_eq!(records.len(), 1, "key {k}"),
            _ => panic!("expected MatchedRecords for key {k}"),
        }
    }
}

/// A leaf that overflows with fewer live entries than the strong version
/// condition allows is merged with a sibling, not split by version: a
/// version split would create a new leaf below the strong minimum.
#[test]
fn full_leaf_below_strong_minimum_is_merged_not_version_split() {
    let payload_counter = AtomicU64::new(0);
    let (tree, hot_key, live) = sparse_leaf_tree(35, &payload_counter);
    assert_eq!(live_in_leaf_of(&tree, hot_key), 35);

    // Updates add dead versions until the leaf overflows and is restructured.
    for _ in 0..150 {
        match tree.dispatch_crud(CRUDOperation::Update(hot_key, payload_counter.fetch_add(1, SeqCst))) {
            CRUDOperationResult::Updated(_) => {}
            _ => panic!("expected Updated"),
        }
    }

    let after = live_in_leaf_of(&tree, hot_key);
    assert!(
        after >= STRONG_MIN,
        "the restructured leaf holds {after} live entries, below the strong minimum {STRONG_MIN}"
    );
    let (active, _) = total_active_dead(&tree);
    assert_eq!(active, live.len() as u64, "no live record lost");
    assert_all_live_keys_readable(&tree, &live);
}

/// The compactor follows the same rule: a candidate with fewer live entries
/// than the strong minimum is merged with a sibling.
#[test]
fn compactor_merges_a_candidate_below_strong_minimum() {
    let payload_counter = AtomicU64::new(0);
    let (tree, hot_key, live) = sparse_leaf_tree(35, &payload_counter);

    // Enough updates for a dead share above one half, but too few to overflow.
    for _ in 0..40 {
        match tree.dispatch_crud(CRUDOperation::Update(hot_key, payload_counter.fetch_add(1, SeqCst))) {
            CRUDOperationResult::Updated(_) => {}
            _ => panic!("expected Updated"),
        }
    }
    assert_eq!(live_in_leaf_of(&tree, hot_key), 35, "not yet restructured");

    assert!(tree.compact_idle_pass(0.5) >= 1, "the garbage-heavy leaf is a candidate");

    let after = live_in_leaf_of(&tree, hot_key);
    assert!(
        after >= STRONG_MIN,
        "the compacted leaf holds {after} live entries, below the strong minimum {STRONG_MIN}"
    );
    let (active, _) = total_active_dead(&tree);
    assert_eq!(active, live.len() as u64, "no live record lost");
    assert_all_live_keys_readable(&tree, &live);
}
