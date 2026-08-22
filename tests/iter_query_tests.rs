//! Verifies `iter_query` (`CRUDOperation::Range`/`RangeIter`, backed by
//! `RangeQueryIter`) actually returns the correct data after/around
//! concurrent inserts — nothing missing, nothing phantom, and (for the
//! snapshot-isolation test) nothing from a transaction that hadn't committed
//! as of the read's own version. Existing concurrency tests
//! (`tree_wal_consistency_tests.rs`) only ever verify via `Point` lookups;
//! this file is the first coverage of the range/iterator path itself.
//!
//! Keys are always inserted in shuffled (not ascending sequential) order:
//! `Range` has a separate, pre-existing, unrelated bug with ascending
//! sequential-key splits (see `query_dispatch_tests.rs`'s
//! `repeated_failed_updates_do_not_corrupt_later_state` doc) that would
//! otherwise contaminate these results with an unrelated failure.

use std::collections::HashMap;

use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_record_model::version_info::Version;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_tree::mvbt::MVBTSt;
use rand::prelude::SliceRandom;

const FAN: usize = 8;
type TestTree = MVBTSt<FAN, FAN, u64, u64>;

fn make_tree() -> TestTree {
    TestTree::make_standard(RootIndexType::default())
}

fn shuffled(range: std::ops::Range<u64>) -> Vec<u64> {
    let mut keys: Vec<u64> = range.collect();
    keys.shuffle(&mut rand::rng());
    keys
}

fn payload_for(key: u64) -> u64 {
    key * 31 + 7
}

#[test]
fn streaming_terminals_count_fold_and_stop_on_error() {
    let tree = make_tree();
    for key in shuffled(0..64) {
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))),
            CRUDOperationResult::Inserted(_)
        ));
    }
    let version = tree.current_version();
    let narrow = Interval::new(19, 23);

    assert_eq!(
        RangeQueryIter::new(&tree, version, narrow, false, tree.worker_id()).count_ref(),
        5
    );
    // `inc_key(u64::MAX)` saturates. A full-range streaming scan must mark
    // the final leaf complete instead of routing MAX back into it forever.
    assert_eq!(
        RangeQueryIter::new(
            &tree,
            version,
            Interval::new(u64::MIN, u64::MAX),
            false,
            tree.worker_id()
        )
        .count_ref(),
        64
    );
    let sum = RangeQueryIter::new(&tree, version, narrow, false, tree.worker_id())
        .fold_ref(0u64, |sum, _, payload| sum + *payload);
    assert_eq!(sum, (19..=23).map(payload_for).sum());

    let mut visited = 0;
    let result = RangeQueryIter::new(&tree, version, narrow, false, tree.worker_id())
        .try_for_each_ref(|_, _| {
            visited += 1;
            if visited == 3 { Err("stop") } else { Ok(()) }
        });
    assert_eq!(result, Err("stop"));
    assert_eq!(visited, 3);
}

fn collect_range(tree: &TestTree, range: Interval<u64>, version: Version) -> HashMap<u64, u64> {
    match tree.dispatch_crud(CRUDOperation::Range(range, version)) {
        CRUDOperationResult::MatchedRecords(records) => {
            records.into_iter().map(|r| (r.key, *r.payload)).collect()
        }
        other => panic!("unexpected Range result: {other}"),
    }
}

fn collect_range_iter(
    tree: &TestTree,
    range: Interval<u64>,
    version: Version,
) -> HashMap<u64, u64> {
    match tree.dispatch_crud(CRUDOperation::RangeIter(range, version)) {
        CRUDOperationResult::MatchedRecordIter(iter) => iter.map(|r| (r.key, *r.payload)).collect(),
        other => panic!("unexpected RangeIter result: {other}"),
    }
}

/// Many threads insert disjoint keys concurrently (shuffled insertion order,
/// both across and within threads); once every insert has returned, both the
/// eager `Range` and the lazy `RangeIter` path must yield exactly the full
/// set of (key, payload) pairs actually written — verified two ways: an
/// exact match over the whole key space, and an exact match over an
/// arbitrary sub-range that spans a split boundary.
#[test]
fn range_and_iter_query_return_exactly_the_concurrently_inserted_records() {
    let tree = make_tree();

    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 200;
    const TOTAL: u64 = THREADS * PER_THREAD;

    let mut keys = shuffled(0..TOTAL);
    // Round-robin the shuffled keys across threads rather than contiguous
    // chunks, so no single thread's insert sequence is even locally sorted.
    let mut per_thread_keys: Vec<Vec<u64>> = (0..THREADS).map(|_| Vec::new()).collect();
    for (i, k) in keys.drain(..).enumerate() {
        per_thread_keys[i % THREADS as usize].push(k);
    }

    std::thread::scope(|scope| {
        for chunk in &per_thread_keys {
            let tree = &tree;
            scope.spawn(move || {
                for &key in chunk {
                    match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
                        CRUDOperationResult::Inserted(_) => {}
                        other => panic!("insert of key {key} failed: {other}"),
                    }
                }
            });
        }
    });

    let expected: HashMap<u64, u64> = (0..TOTAL).map(|k| (k, payload_for(k))).collect();
    let version = tree.current_version();
    let full_range = Interval::new(0, TOTAL - 1);

    assert_eq!(
        collect_range(&tree, full_range, version),
        expected,
        "Range missed or corrupted concurrently inserted records"
    );
    assert_eq!(
        collect_range_iter(&tree, full_range, version),
        expected,
        "RangeIter missed or corrupted concurrently inserted records"
    );

    // Sub-range spanning several splits (FAN == 8, TOTAL == 1600 keys): must
    // return exactly the keys inside it, no more, no fewer.
    let lo = TOTAL / 3;
    let hi = lo + TOTAL / 4;
    let sub_range = Interval::new(lo, hi);
    let expected_sub: HashMap<u64, u64> = expected
        .iter()
        .filter(|&(&k, _)| k >= lo && k <= hi)
        .map(|(&k, &v)| (k, v))
        .collect();

    assert_eq!(
        collect_range(&tree, sub_range, version),
        expected_sub,
        "Range over a sub-interval returned the wrong records"
    );
    assert_eq!(
        collect_range_iter(&tree, sub_range, version),
        expected_sub,
        "RangeIter over a sub-interval returned the wrong records"
    );
}

/// A range query taken at a version from *before* a concurrent batch of
/// inserts must see only the pre-existing data — none of the concurrently
/// inserted keys — even though, by the time the query actually runs, the
/// tree already physically contains them. A query taken *after* must see
/// everything. This is the property the whole `registrations_in_flight`/
/// `live_tx` machinery (see `tx_context_registration_tests.rs`,
/// `loom_registration_ordering.rs`) exists to keep safe under GC, and the
/// property a reader actually cares about: not just "do writes eventually
/// show up" but "does *my* snapshot see exactly what it should."
#[test]
fn range_query_respects_snapshot_isolation_across_concurrent_inserts() {
    let tree = make_tree();

    const BEFORE_COUNT: u64 = 100;
    const THREADS: u64 = 8;
    const AFTER_PER_THREAD: u64 = 100;
    const AFTER_TOTAL: u64 = THREADS * AFTER_PER_THREAD;
    const AFTER_BASE: u64 = 10_000; // disjoint from the "before" key space

    for key in shuffled(0..BEFORE_COUNT) {
        match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert of before-key {key} failed: {other}"),
        }
    }

    let version_before = tree.current_version();

    let mut after_keys = shuffled(AFTER_BASE..AFTER_BASE + AFTER_TOTAL);
    let mut per_thread_keys: Vec<Vec<u64>> = (0..THREADS).map(|_| Vec::new()).collect();
    for (i, k) in after_keys.drain(..).enumerate() {
        per_thread_keys[i % THREADS as usize].push(k);
    }

    std::thread::scope(|scope| {
        for chunk in &per_thread_keys {
            let tree = &tree;
            scope.spawn(move || {
                for &key in chunk {
                    match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
                        CRUDOperationResult::Inserted(_) => {}
                        other => panic!("insert of after-key {key} failed: {other}"),
                    }
                }
            });
        }
    });

    let version_after = tree.current_version();
    let full_range = Interval::new(0, AFTER_BASE + AFTER_TOTAL);

    let expected_before: HashMap<u64, u64> =
        (0..BEFORE_COUNT).map(|k| (k, payload_for(k))).collect();
    let mut expected_after = expected_before.clone();
    expected_after.extend((AFTER_BASE..AFTER_BASE + AFTER_TOTAL).map(|k| (k, payload_for(k))));

    assert_eq!(
        collect_range(&tree, full_range, version_before),
        expected_before,
        "Range at the pre-insert version saw concurrently inserted keys it shouldn't have"
    );
    assert_eq!(
        collect_range_iter(&tree, full_range, version_before),
        expected_before,
        "RangeIter at the pre-insert version saw concurrently inserted keys it shouldn't have"
    );

    assert_eq!(
        collect_range(&tree, full_range, version_after),
        expected_after,
        "Range at the post-insert version is missing some concurrently inserted keys"
    );
    assert_eq!(
        collect_range_iter(&tree, full_range, version_after),
        expected_after,
        "RangeIter at the post-insert version is missing some concurrently inserted keys"
    );
}

/// `LeafPage` records are append-ordered, never re-sorted by key
/// (`LeafPage::push_uncommitted` always writes at the next free slot) — so
/// `RangeQueryIter::min_by_key` can't just trust `next()`'s first result,
/// it has to actually compare every match within the first matching leaf.
/// Inserting keys in *descending* order specifically catches a naive "just
/// take next()" implementation, which would return the first-inserted
/// (largest, physically-first) key instead of the true minimum — exactly
/// the bug this test guards against regressing (it's what `bat_bench::
/// tpcc_txn::deliver_one_district` relies on `range_min` for: finding the
/// oldest — smallest-key — queued new-order).
#[test]
fn range_min_by_key_finds_the_true_minimum_despite_descending_insertion_order() {
    let tree = make_tree();

    // FAN == 8: comfortably fits in a single leaf (no split forced), so
    // this exercises purely within-leaf ordering, not cross-leaf.
    for key in (0..6u64).rev() {
        match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert of key {key} failed: {other}"),
        }
    }

    let version = tree.current_version();
    let min = RangeQueryIter::new(&tree, version, Interval::new(0, 5), false, tree.worker_id())
        .min_by_key()
        .expect("range should have at least one match");

    assert_eq!(
        min.key, 0,
        "min_by_key must return the smallest key in range, not whichever the leaf happened to store first"
    );
    assert_eq!(*min.payload, payload_for(0));
}

/// `RangeQueryIter::with_zone_predicate` both skips a leaf outright (see
/// `LeafZoneMap`'s doc) and automatically filters individual records by
/// value as part of the normal range/visibility check. This test's
/// `filtered_scan` re-applies the same payload-range check in its own
/// `visit` closure anyway (redundant with the automatic filter when
/// `with_pruning` is true, but exactly what's needed when it's false, to
/// get a like-for-like comparison), then compares against the same closure
/// run with no zone predicate at all (visiting every leaf, no skipping, no
/// automatic filtering): if zone-map pruning/filtering is sound, the two
/// must always agree.
///
/// `FAN` stays small and the key count large enough to force many splits
/// (both `KEY_SPLIT` and `VERSION_SPLIT`, per `smo::split`'s own 80%/40%
/// thresholds) — the split/merge zone-map carry-forward wiring in
/// `bat_tree::smo` is exactly the part of this feature most at risk of a
/// silent "leaf loses its projected range" bug, and only actually gets
/// exercised once real splits happen.
#[test]
fn zone_map_pruned_scan_matches_unpruned_scan_across_many_forced_splits() {
    const ZONE_FAN: usize = 8;
    type ZoneTree = MVBTSt<ZONE_FAN, ZONE_FAN, u64, u64>;

    let tree = ZoneTree::make_standard(RootIndexType::default());
    // Payload *is* the tracked column here (identity projection), so the
    // zone map's `[lo, hi]` is directly checkable against plain key/payload
    // arithmetic below.
    tree.set_zone_map_projection(|payload: &u64| Some(*payload));

    const TOTAL: u64 = 500;
    for key in shuffled(0..TOTAL) {
        match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert of key {key} failed: {other}"),
        }
    }

    let version = tree.current_version();
    let full_key_range = Interval::new(0, TOTAL - 1);

    let filtered_scan = |zone_lo: u64, zone_hi: u64, with_pruning: bool| -> HashMap<u64, u64> {
        let mut out = HashMap::new();
        let mut it = RangeQueryIter::new(&tree, version, full_key_range, false, tree.worker_id());
        if with_pruning {
            it = it.with_zone_predicate(zone_lo, zone_hi);
        }
        it.for_each_ref(|k, p| {
            if *p >= zone_lo && *p <= zone_hi {
                out.insert(k, *p);
            }
        });
        out
    };

    // Windows chosen to land: entirely inside the data's real payload range,
    // spanning it, a single value, and one guaranteed to match nothing at
    // all (every leaf provably prunable) — the case a broken "empty zone
    // map always skips" bug (see `push_records_onto`'s doc) would otherwise
    // hide, since an all-skip result there would look identical to a
    // correct all-skip result here.
    let payload_lo = payload_for(0);
    let payload_hi = payload_for(TOTAL - 1);
    let windows = [
        (payload_lo, payload_hi),
        (payload_lo, payload_lo + (payload_hi - payload_lo) / 3),
        (payload_hi - (payload_hi - payload_lo) / 5, payload_hi),
        (payload_for(TOTAL / 2), payload_for(TOTAL / 2)),
        (payload_hi + 1, payload_hi + 1_000_000),
    ];

    for (zone_lo, zone_hi) in windows {
        let pruned = filtered_scan(zone_lo, zone_hi, true);
        let unpruned = filtered_scan(zone_lo, zone_hi, false);
        assert_eq!(
            pruned, unpruned,
            "zone-map-pruned scan disagreed with an unpruned scan over payload window [{zone_lo}, {zone_hi}]"
        );
    }
}

/// A tree with no `set_zone_map_projection` configured must behave
/// identically whether or not a caller (mistakenly, or via generic code
/// that doesn't know which trees are configured) calls
/// `with_zone_predicate` — see that method's doc on why this guard exists.
/// Without the `tree.cold.zone_map_projection.is_some()` cross-check in
/// `try_for_each_ref`, every leaf's default-empty zone map would make this
/// silently return nothing at all instead of the true result.
#[test]
fn zone_predicate_is_inert_on_a_tree_with_no_projection_configured() {
    let tree = make_tree();
    for key in shuffled(0..64) {
        match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert of key {key} failed: {other}"),
        }
    }

    let version = tree.current_version();
    let full_range = Interval::new(0, 63);

    let with_predicate = RangeQueryIter::new(&tree, version, full_range, false, tree.worker_id())
        .with_zone_predicate(0, 1)
        .count_ref();
    let without_predicate =
        RangeQueryIter::new(&tree, version, full_range, false, tree.worker_id()).count_ref();

    assert_eq!(
        with_predicate, without_predicate,
        "with_zone_predicate must be a no-op on a tree with no zone_map_projection configured"
    );
    assert_eq!(with_predicate, 64);
}

/// The automatic per-record filter (see `RangeQueryIter::with_zone_predicate`'s
/// doc) must be correct *on its own*, with no help from the caller's `visit`
/// closure — this is the property `q1`/`q1_parallel` actually rely on now
/// that they dropped their own manual `ol_delivery_d` check. `visit` here
/// only counts and never re-checks the payload itself; if the automatic
/// filter were missing or wrong, this would either over- or under-count
/// relative to a plain `HashMap` filter computed independently below.
#[test]
fn zone_predicate_alone_filters_correctly_with_no_help_from_the_visit_closure() {
    const ZONE_FAN: usize = 8;
    type ZoneTree = MVBTSt<ZONE_FAN, ZONE_FAN, u64, u64>;

    let tree = ZoneTree::make_standard(RootIndexType::default());
    tree.set_zone_map_projection(|payload: &u64| Some(*payload));

    const TOTAL: u64 = 300;
    let mut expected: HashMap<u64, u64> = HashMap::new();
    for key in shuffled(0..TOTAL) {
        let payload = payload_for(key);
        expected.insert(key, payload);
        match tree.dispatch_crud(CRUDOperation::Insert(key, payload)) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert of key {key} failed: {other}"),
        }
    }

    let version = tree.current_version();
    let zone_lo = payload_for(50);
    let zone_hi = payload_for(150);

    let mut actual = HashMap::new();
    RangeQueryIter::new(&tree, version, Interval::new(0, TOTAL - 1), false, tree.worker_id())
        .with_zone_predicate(zone_lo, zone_hi)
        .for_each_ref(|k, p| {
            // Deliberately no re-check here: correctness of this result
            // depends entirely on the engine's own automatic filter.
            actual.insert(k, *p);
        });

    let expected_in_window: HashMap<u64, u64> = expected
        .into_iter()
        .filter(|&(_, p)| p >= zone_lo && p <= zone_hi)
        .collect();

    assert_eq!(
        actual, expected_in_window,
        "the automatic zone-predicate filter (no closure help) disagreed with an \
         independently computed payload-window filter"
    );
    assert!(!actual.is_empty(), "sanity: the window should match at least one row");
}

/// Same soundness property as `zone_map_pruned_scan_matches_unpruned_scan_
/// across_many_forced_splits`, but for `bat_tree::smo::merge` instead of
/// `split`: deletes enough keys after the initial (split-heavy) load to
/// force real underflow-driven merges (`smo::merge`'s own `KeySplit`/
/// `Merged` leaf branches), then checks pruned vs. unpruned scans over the
/// *surviving* keys still agree. `push_records_onto`'s zone-map
/// recomputation fix applies identically to merge's call sites, but
/// nothing exercised them until this test — a merge combines *two* source
/// leaves' records into one (or splits the combination back out), a
/// genuinely different code path from split's "one leaf into two."
#[test]
fn zone_map_pruned_scan_matches_unpruned_scan_across_forced_merges() {
    const ZONE_FAN: usize = 8;
    type ZoneTree = MVBTSt<ZONE_FAN, ZONE_FAN, u64, u64>;

    let tree = ZoneTree::make_standard(RootIndexType::default());
    tree.set_zone_map_projection(|payload: &u64| Some(*payload));

    const TOTAL: u64 = 500;
    for key in shuffled(0..TOTAL) {
        match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert of key {key} failed: {other}"),
        }
    }

    // Delete every third key — enough sustained underflow, spread across
    // the whole key range, to force repeated merges rather than emptying
    // one localized region.
    let mut deleted = std::collections::HashSet::new();
    for key in shuffled(0..TOTAL) {
        if key % 3 == 0 {
            match tree.dispatch_crud(CRUDOperation::Delete(key)) {
                CRUDOperationResult::Deleted(_) => {
                    deleted.insert(key);
                }
                other => panic!("delete of key {key} failed: {other}"),
            }
        }
    }

    let version = tree.current_version();
    let full_key_range = Interval::new(0, TOTAL - 1);

    let filtered_scan = |zone_lo: u64, zone_hi: u64, with_pruning: bool| -> HashMap<u64, u64> {
        let mut out = HashMap::new();
        let mut it = RangeQueryIter::new(&tree, version, full_key_range, false, tree.worker_id());
        if with_pruning {
            it = it.with_zone_predicate(zone_lo, zone_hi);
        }
        it.for_each_ref(|k, p| {
            if *p >= zone_lo && *p <= zone_hi {
                out.insert(k, *p);
            }
        });
        out
    };

    let payload_lo = payload_for(0);
    let payload_hi = payload_for(TOTAL - 1);
    let windows = [
        (payload_lo, payload_hi),
        (payload_lo, payload_lo + (payload_hi - payload_lo) / 3),
        (payload_hi - (payload_hi - payload_lo) / 5, payload_hi),
        (payload_for(TOTAL / 2), payload_for(TOTAL / 2)),
    ];

    for (zone_lo, zone_hi) in windows {
        let pruned = filtered_scan(zone_lo, zone_hi, true);
        let unpruned = filtered_scan(zone_lo, zone_hi, false);
        assert_eq!(
            pruned, unpruned,
            "zone-map-pruned scan disagreed with an unpruned scan over payload window \
             [{zone_lo}, {zone_hi}] after merges"
        );
        for key in pruned.keys() {
            assert!(
                !deleted.contains(key),
                "a deleted key ({key}) leaked into a post-merge scan result"
            );
        }
    }
}

/// Concurrent writers keep inserting new keys (forcing live splits) while a
/// zone-pruned scan runs at a fixed snapshot — mirrors this codebase's
/// standard snapshot-isolation test shape (`range_query_respects_snapshot_
/// isolation_across_concurrent_inserts` above) but specifically exercises
/// `with_zone_predicate` against a tree that's actively splitting *during*
/// the scan, not just before it. A scan taken at `version_before` must see
/// none of the concurrently-inserted keys, pruned or not; one taken after
/// must see all of them, and the pruned/unpruned results must still agree
/// with each other at both snapshots.
#[test]
fn zone_map_pruned_scan_is_snapshot_consistent_under_concurrent_inserts_and_splits() {
    const ZONE_FAN: usize = 8;
    type ZoneTree = MVBTSt<ZONE_FAN, ZONE_FAN, u64, u64>;

    let tree = ZoneTree::make_standard(RootIndexType::default());
    tree.set_zone_map_projection(|payload: &u64| Some(*payload));

    const BEFORE_COUNT: u64 = 100;
    const THREADS: u64 = 8;
    const AFTER_PER_THREAD: u64 = 100;
    const AFTER_TOTAL: u64 = THREADS * AFTER_PER_THREAD;
    const AFTER_BASE: u64 = 10_000;

    for key in shuffled(0..BEFORE_COUNT) {
        match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
            CRUDOperationResult::Inserted(_) => {}
            other => panic!("insert of before-key {key} failed: {other}"),
        }
    }
    let version_before = tree.current_version();

    let mut after_keys = shuffled(AFTER_BASE..AFTER_BASE + AFTER_TOTAL);
    let mut per_thread_keys: Vec<Vec<u64>> = (0..THREADS).map(|_| Vec::new()).collect();
    for (i, k) in after_keys.drain(..).enumerate() {
        per_thread_keys[i % THREADS as usize].push(k);
    }
    std::thread::scope(|scope| {
        for chunk in &per_thread_keys {
            let tree = &tree;
            scope.spawn(move || {
                for &key in chunk {
                    match tree.dispatch_crud(CRUDOperation::Insert(key, payload_for(key))) {
                        CRUDOperationResult::Inserted(_) => {}
                        other => panic!("insert of after-key {key} failed: {other}"),
                    }
                }
            });
        }
    });
    let version_after = tree.current_version();

    let full_range = Interval::new(0, AFTER_BASE + AFTER_TOTAL);
    let zone_lo = payload_for(0);
    let zone_hi = payload_for(AFTER_BASE + AFTER_TOTAL);

    let scan = |version: Version, with_pruning: bool| -> HashMap<u64, u64> {
        let mut out = HashMap::new();
        let mut it = RangeQueryIter::new(&tree, version, full_range, false, tree.worker_id());
        if with_pruning {
            it = it.with_zone_predicate(zone_lo, zone_hi);
        }
        it.for_each_ref(|k, p| {
            out.insert(k, *p);
        });
        out
    };

    let expected_before: HashMap<u64, u64> =
        (0..BEFORE_COUNT).map(|k| (k, payload_for(k))).collect();
    let mut expected_after = expected_before.clone();
    expected_after.extend((AFTER_BASE..AFTER_BASE + AFTER_TOTAL).map(|k| (k, payload_for(k))));

    assert_eq!(scan(version_before, true), expected_before);
    assert_eq!(scan(version_before, false), expected_before);
    assert_eq!(scan(version_after, true), expected_after);
    assert_eq!(scan(version_after, false), expected_after);
}
