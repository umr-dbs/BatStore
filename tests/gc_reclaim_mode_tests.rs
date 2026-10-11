use super::*;
use crate::bat_block::block::Block;
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_db::Database;
use crate::bat_record_model::record_point::RecordPoint;
use crate::bat_record_model::version_info::VersionInfo;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_tree::mvbt::MVBTSt;
use std::fmt::{Display, Formatter};
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicUsize, Ordering};

type TestBlock = Block<4, 4, u64, u64>;

fn block() -> BlockRef<4, 4, u64, u64> {
    TestBlock::default().into_cell()
}

#[test]
fn off_never_queues_or_reuses_retired_pages() {
    let tracker = TrackerHandleSt::<4, 4, u64, u64>::new_for_test(2, 4);
    let ctx = TxContext::new(2);
    let retired = block();

    tracker.register_died_page(0, 1, retired);

    assert_eq!(tracker.reclaim_mode(), PageReclaimMode::Off);
    assert!(tracker.free_block(&ctx).is_none());
    assert!(retired.is_retired());
    unsafe { retired.destroy() };
}

#[test]
fn reuse_no_steal_uses_local_but_not_remote_retired_pages() {
    let tracker = TrackerHandleSt::<4, 4, u64, u64>::new_for_test(2, 4);
    let ctx = TxContext::new(2);
    tracker.set_reclaim_mode(PageReclaimMode::ReuseNoSteal);

    let local = block();
    tracker.register_died_page(0, 1, local);
    let got = tracker.free_block(&ctx).expect("local page must be reused");
    assert_eq!(got.0, local.0);
    unsafe { got.destroy() };

    let remote = block();
    tracker.register_died_page(1, 1, remote);
    assert!(tracker.free_block(&ctx).is_none());

    // Prove the page stayed queued rather than being lost.
    tracker.set_reclaim_mode(PageReclaimMode::ReuseSteal);
    let got = tracker
        .free_block(&ctx)
        .expect("remote page must remain stealable");
    assert_eq!(got.0, remote.0);
    unsafe { got.destroy() };
}

#[test]
fn fixed_layout_groups_workers_without_cross_shard_stealing() {
    let tracker = TrackerHandleSt::<4, 4, u64, u64>::new_for_test(2, 4);
    let ctx = TxContext::new(4);
    tracker.set_reclaim_mode(PageReclaimMode::ReuseNoSteal);

    // Workers 0 and 2 map to shard 0, so grouped NoSteal reuse is local
    // to that shard even though the retiring worker differs.
    let grouped = block();
    tracker.register_died_page(2, 1, grouped);
    let got = tracker
        .free_block(&ctx)
        .expect("workers assigned to one shard share its local queue");
    assert_eq!(got.0, grouped.0);
    unsafe { got.destroy() };

    // Worker 1 maps to shard 1 and remains unavailable without stealing.
    let other_shard = block();
    tracker.register_died_page(1, 1, other_shard);
    assert!(tracker.free_block(&ctx).is_none());

    tracker.set_reclaim_mode(PageReclaimMode::ReuseSteal);
    let got = tracker
        .free_block(&ctx)
        .expect("the other shard becomes visible in steal mode");
    assert_eq!(got.0, other_shard.0);
    unsafe { got.destroy() };
}

#[test]
fn reuse_steal_takes_an_eligible_remote_retired_page() {
    let tracker = TrackerHandleSt::<4, 4, u64, u64>::new_for_test(2, 4);
    let ctx = TxContext::new(2);
    tracker.set_reclaim_mode(PageReclaimMode::ReuseSteal);
    let remote = block();
    tracker.register_died_page(1, 1, remote);

    let got = tracker
        .free_block(&ctx)
        .expect("steal mode must consult remote lists");
    assert_eq!(got.0, remote.0);
    unsafe { got.destroy() };
}

#[test]
fn reuse_no_steal_does_not_consume_a_stolen_page_cached_by_an_earlier_mode() {
    let tracker = TrackerHandleSt::<4, 4, u64, u64>::new_for_test(2, 4);
    let ctx = TxContext::new(2);
    tracker.set_reclaim_mode(PageReclaimMode::ReuseSteal);
    let remote_a = block();
    let remote_b = block();
    tracker.register_died_page(1, 1, remote_a);
    tracker.register_died_page(1, 1, remote_b);

    let first = tracker
        .free_block(&ctx)
        .expect("steal mode must prefetch the remote pages");
    unsafe { first.destroy() };

    tracker.set_reclaim_mode(PageReclaimMode::ReuseNoSteal);
    assert!(tracker.free_block(&ctx).is_none());

    tracker.set_reclaim_mode(PageReclaimMode::ReuseSteal);
    let cached = tracker
        .free_block(&ctx)
        .expect("the deferred stolen page must remain available to steal mode");
    assert!(cached.0 == remote_a.0 || cached.0 == remote_b.0);
    unsafe { cached.destroy() };
}

#[derive(Clone, Default)]
struct DropPayload(Option<StdArc<AtomicUsize>>);

impl Display for DropPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("payload")
    }
}

impl Drop for DropPayload {
    fn drop(&mut self) {
        if let Some(counter) = &self.0 {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn os_destroys_eligible_retired_pages_and_drops_their_contents() {
    let tracker = TrackerHandleSt::<4, 4, u64, DropPayload>::new_for_test(2, 4);
    let ctx = TxContext::new(2);
    tracker.set_reclaim_mode(PageReclaimMode::Os);
    let drops = StdArc::new(AtomicUsize::new(0));
    let retired = Block::<4, 4, u64, DropPayload>::default().into_cell();
    let leaf = retired.unsafe_borrow_mut().as_leaf_page();
    leaf.push_uncommitted(
        RecordPoint::new(7, VersionInfo::default(), DropPayload(Some(drops.clone()))),
        0,
    );
    leaf.commit_delta(1, 0);
    tracker.register_died_page(0, 1, retired);

    assert!(tracker.free_block(&ctx).is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    #[cfg(feature = "gc-stats")]
    assert_eq!(
        tracker
            .gc_stats_per_shard()
            .into_iter()
            .map(|stats| stats.os_dealloc)
            .sum::<u64>(),
        1
    );
}

#[test]
fn os_can_consume_a_fresh_local_batch_reserve_without_destroying_it() {
    let tracker = TrackerHandleSt::<4, 4, u64, u64>::new_for_test(2, 4);
    let ctx = TxContext::new(2);
    tracker.set_reclaim_mode(PageReclaimMode::Os);
    let fresh = block();
    tracker.queue_fresh_blocks(0, [fresh]);

    let got = tracker
        .free_block(&ctx)
        .expect("OS mode must use freshly batch-allocated reserves");
    assert_eq!(got.0, fresh.0);
    assert!(!got.is_retired());
    unsafe { got.destroy() };
}

#[test]
fn os_does_not_consume_another_workers_fresh_reserve() {
    let tracker = TrackerHandleSt::<4, 4, u64, u64>::new_for_test(2, 4);
    let ctx = TxContext::new(2);
    tracker.set_reclaim_mode(PageReclaimMode::Os);
    let remote_fresh = block();
    tracker.queue_fresh_blocks(1, [remote_fresh]);

    assert!(tracker.free_block(&ctx).is_none());

    // Move the test worker's local shard to worker 1 only for cleanup by
    // retrieving directly from the trace; production allocation never
    // performs this cross-worker fresh-page access.
    let queued = tracker
        .dead_blocks
        .take_local_fresh(1)
        .expect("remote reserve must remain in its owner's queue");
    assert_eq!(queued.0, remote_fresh.0);
    unsafe { queued.destroy() };
}

#[test]
fn active_snapshot_blocks_os_destruction_until_it_finishes() {
    let tracker = TrackerHandleSt::<4, 4, u64, DropPayload>::new_for_test(2, 4);
    let ctx = TxContext::new(2);
    tracker.set_reclaim_mode(PageReclaimMode::Os);
    ctx.set_block_reclaim_enabled(true);
    let snapshot = ctx.begin_snapshot();
    let drops = StdArc::new(AtomicUsize::new(0));
    let retired = Block::<4, 4, u64, DropPayload>::default().into_cell();
    let leaf = retired.unsafe_borrow_mut().as_leaf_page();
    leaf.push_uncommitted(
        RecordPoint::new(9, VersionInfo::default(), DropPayload(Some(drops.clone()))),
        0,
    );
    leaf.commit_delta(1, 0);
    tracker.register_died_page(0, snapshot + 1, retired);

    assert!(tracker.free_block(&ctx).is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    ctx.end_snapshot(snapshot);
    assert!(tracker.free_block(&ctx).is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn active_snapshot_blocks_reuse_until_it_finishes() {
    let tracker = TrackerHandleSt::<4, 4, u64, u64>::new_for_test(2, 4);
    let ctx = TxContext::new(2);
    tracker.set_reclaim_mode(PageReclaimMode::ReuseSteal);
    ctx.set_block_reclaim_enabled(true);
    let snapshot = ctx.begin_snapshot();
    let retired = block();
    tracker.register_died_page(0, snapshot + 1, retired);

    assert!(tracker.free_block(&ctx).is_none());
    ctx.end_snapshot(snapshot);
    let got = tracker
        .free_block(&ctx)
        .expect("page becomes reusable after the blocking snapshot ends");
    assert_eq!(got.0, retired.0);
    unsafe { got.destroy() };
}

#[test]
fn every_mode_preserves_tree_results_across_splits_and_version_gc() {
    type Tree = MVBTSt<8, 8, u64, u64>;
    for mode in [
        PageReclaimMode::Off,
        PageReclaimMode::ReuseNoSteal,
        PageReclaimMode::ReuseSteal,
        PageReclaimMode::Os,
    ] {
        let tree = Tree::make_standard(RootIndexType::default());
        tree.configure_gc(mode, false);
        assert_eq!(tree.gc_mode(), mode);

        for key in 0..256 {
            assert!(matches!(
                tree.dispatch_crud(CRUDOperation::Insert(key, key)),
                CRUDOperationResult::Inserted(_)
            ));
        }
        for round in 1..=8 {
            for key in 0..256 {
                assert!(matches!(
                    tree.dispatch_crud(CRUDOperation::Update(key, key + round * 1_000)),
                    CRUDOperationResult::Updated(_)
                ));
            }
        }
        for key in 0..256 {
            match tree.dispatch_crud(CRUDOperation::PointSi(key)) {
                CRUDOperationResult::MatchedRecords(rows)
                    if rows.len() == 1 && rows[0].payload == key + 8_000 => {}
                other => panic!("mode {mode:?}, key {key}: unexpected result {other}"),
            }
        }
    }
}

#[test]
fn database_configuration_is_inherited_by_tables_created_later() {
    let db = Database::<8, 8, u64, u64>::new_with_max_workers(
        RootIndexType::default(),
        |key| key.saturating_add(1),
        |key| key.saturating_sub(1),
        u64::MIN,
        u64::MAX,
        7,
    );
    db.configure_gc(PageReclaimMode::ReuseNoSteal, false, None);
    let table = db.create_table("configured_after_gc");
    assert_eq!(table.gc_mode(), PageReclaimMode::ReuseNoSteal);
    assert_eq!(table.tracker().shard_count(), 7);

    db.configure_gc(PageReclaimMode::Os, false, None);
    assert_eq!(table.gc_mode(), PageReclaimMode::Os);
    let later = db.create_table("configured_after_mode_change");
    assert_eq!(later.gc_mode(), PageReclaimMode::Os);

    db.disable_gc();
    assert_eq!(table.gc_mode(), PageReclaimMode::Off);
    assert_eq!(later.gc_mode(), PageReclaimMode::Off);
}

#[test]
fn database_fixed_shard_layout_is_inherited_by_every_table() {
    let db = Database::<8, 8, u64, u64>::new_with_max_workers(
        RootIndexType::default(),
        |key| key.saturating_add(1),
        |key| key.saturating_sub(1),
        u64::MIN,
        u64::MAX,
        32,
    )
    .with_gc_shard_layout(crate::bat_gc::GcShardLayout::fixed(3));

    assert_eq!(db.gc_shard_layout(), crate::bat_gc::GcShardLayout::fixed(3));
    assert_eq!(db.create_table("first").gc_shard_count(), 3);
    assert_eq!(db.create_table("second").gc_shard_count(), 3);
}

#[test]
fn standalone_tree_accepts_a_fixed_shard_layout() {
    let tree = MVBTSt::<8, 8, u64, u64>::make_standard_with_max_workers_and_gc_shards(
        RootIndexType::default(),
        32,
        crate::bat_gc::GcShardLayout::fixed(3),
    );
    assert_eq!(tree.gc_shard_count(), 3);
}
