use crate::bat_page_model::leaf_page::{AbortOutcome, LeafPage};
use crate::bat_record_model::record_point::RecordPoint;
use crate::bat_record_model::tx_stamp::TxStamp;
use crate::bat_record_model::version_info::VersionInfo;

const NUM_RECORDS: usize = 8;
type TestLeaf = LeafPage<NUM_RECORDS, u64, u64>;

#[test]
fn production_soa_capacity_still_fits_one_4k_cell() {
    use crate::bat_block::block::Block;
    use crate::bat_sync::smart_cell::OptCell;
    use crate::bat_tree::mvbt::{FAN_OUT, NUM_RECORDS};

    type ProductionCell = OptCell<Block<FAN_OUT, NUM_RECORDS, u64, u64>>;
    type OneMoreCell = OptCell<Block<FAN_OUT, 124, u64, u64>>;

    assert_eq!(NUM_RECORDS, 123);
    assert_eq!(std::mem::size_of::<ProductionCell>(), 4096);
    assert!(std::mem::size_of::<OneMoreCell>() > 4096);
}

fn insert(leaf: &mut TestLeaf, key: u64, stamp: TxStamp, payload: u64) {
    let len = leaf.len();
    leaf.push_uncommitted(RecordPoint::new(key, VersionInfo::new(stamp), payload), len);
    leaf.commit_delta(1, 0);
}

/// Reverting an aborted `Insert`: the record must become invisible
/// (`is_live() == false`) and count as dead, and a second `abort_write`
/// call for the same key must be a safe no-op (see `abort_write`'s doc).
#[test]
fn abort_write_reverts_a_plain_insert() {
    let mut leaf = TestLeaf::new();
    let stamp = TxStamp::new(1, 100);
    insert(&mut leaf, 1, stamp, 42);
    assert_eq!(leaf.active_dead_count(), (1, 0));

    assert_eq!(leaf.abort_write(1, stamp), AbortOutcome::Invalidated);
    let record = leaf.as_records().into_iter().rfind(|r| r.key == 1).unwrap();
    assert!(!record.version().is_live());
    assert!(record.version().insertion_stamp().is_invalid());
    assert_eq!(leaf.active_dead_invalid(), (0, 0, 1));

    // Idempotent: processing the same key's abort twice must not
    // double-adjust the counts.
    assert_eq!(leaf.abort_write(1, stamp), AbortOutcome::NotFound);
    assert_eq!(leaf.active_dead_invalid(), (0, 0, 1));
}

#[test]
fn soa_layout_keeps_keys_dense_and_validity_mask_skips_aborted_slots() {
    let mut leaf = TestLeaf::new();
    let stamp = TxStamp::new(1, 77);
    insert(&mut leaf, 4, stamp, 40);
    insert(&mut leaf, 9, stamp, 90);
    insert(&mut leaf, 4, stamp, 41);

    let keys = leaf.keys();
    assert_eq!(keys, &[4, 9, 4]);
    assert_eq!(
        (unsafe { keys.as_ptr().add(1) } as usize) - (keys.as_ptr() as usize),
        std::mem::size_of::<u64>(),
        "keys must occupy one dense, key-only array"
    );

    assert_eq!(leaf.latest_position(4, true), Some(2));
    assert_eq!(leaf.abort_write(4, stamp), AbortOutcome::Invalidated);
    assert_eq!(
        leaf.latest_position(4, true),
        Some(0),
        "the validity mask must skip the invalidated newest slot"
    );
    assert_eq!(
        leaf.latest_position(4, false),
        Some(2),
        "physical-order lookup must still be able to inspect invalid history"
    );
}

#[test]
fn abort_writes_reverts_a_same_key_run_under_one_leaf_latch() {
    let mut leaf = TestLeaf::new();
    let stamp = TxStamp::new(1, 101);
    insert(&mut leaf, 7, stamp, 1);
    insert(&mut leaf, 7, stamp, 2);
    assert!(leaf.delete_after_update(7, stamp).unwrap().is_some());
    leaf.commit_delta(-1, 1);
    insert(&mut leaf, 7, stamp, 3);
    assert!(leaf.delete_after_update(7, stamp).unwrap().is_some());
    leaf.commit_delta(-1, 1);

    assert_eq!(leaf.abort_writes(7, stamp, 3), 3);
    assert_eq!(leaf.abort_writes(7, stamp, 1), 0);
    assert!(
        leaf.as_records()
            .iter()
            .all(|r| { r.key != 7 || r.version().insertion_stamp().is_invalid() })
    );
}

#[test]
fn abort_write_reverts_an_update_and_resurrects_its_predecessor() {
    let mut leaf = TestLeaf::new();
    let stamp = TxStamp::new(2, 200);

    insert(&mut leaf, 5, stamp, 1); // the original value
    assert_eq!(leaf.active_dead_count(), (1, 0));

    // Simulate an in-transaction Update: push the new version, then
    // delete_after_update marks the original superseded.
    insert(&mut leaf, 5, stamp, 2);
    assert!(matches!(leaf.delete_after_update(5, stamp), Ok(Some(_))));
    leaf.commit_delta(-1, 1);
    assert_eq!(leaf.active_dead_count(), (1, 1));

    assert_eq!(leaf.abort_write(5, stamp), AbortOutcome::Invalidated);

    let records: Vec<_> = leaf
        .as_records()
        .into_iter()
        .filter(|r| r.key == 5)
        .collect();
    assert_eq!(records.len(), 2);
    assert!(
        !records[1].version().is_live(),
        "the update's own new entry must be invalidated"
    );
    assert!(
        records[0].version().is_live(),
        "the original entry must be resurrected"
    );
    assert_eq!(
        *records[0].payload(),
        1,
        "the resurrected entry is the original value"
    );

    assert_eq!(leaf.active_dead_count(), (1, 1));
}

#[test]
fn cold_predecessor_is_cloned_undeleted_without_mutating_history() {
    let mut cold = TestLeaf::new();
    let insert_stamp = TxStamp::new(1, 10);
    let update_stamp = TxStamp::new(2, 20);
    insert(&mut cold, 5, insert_stamp, 50);
    assert!(cold.version_mut_at(0).delete(update_stamp));
    cold.commit_delta(-1, 1);

    let restored = cold
        .clone_undeleted_matching(5, update_stamp)
        .expect("the exact predecessor must be materialized for the hot leaf");

    assert!(restored.version().is_live());
    assert_eq!(restored.version().insertion_stamp(), insert_stamp);
    assert_eq!(*restored.payload(), 50);
    assert!(
        cold.version_at(0).is_deleted(),
        "cold history remains immutable"
    );
    assert_eq!(cold.active_dead_count(), (0, 1));
}

#[test]
fn delete_after_update_skips_an_invalidated_entry_to_reach_the_true_predecessor() {
    let mut leaf = TestLeaf::new();
    let stamp1 = TxStamp::new(1, 100);

    insert(&mut leaf, 1, stamp1, 10); // v0: the original value

    // T1 updates key 1, then aborts: v0 resurrected (live), v1 invalidated.
    insert(&mut leaf, 1, stamp1, 11); // v1
    assert!(matches!(leaf.delete_after_update(1, stamp1), Ok(Some(_))));
    leaf.commit_delta(-1, 1);
    assert_eq!(leaf.abort_write(1, stamp1), AbortOutcome::Invalidated);
    assert_eq!(leaf.active_dead_count(), (1, 1)); // v0 live, v1 dead(invalid)

    // T2 (a later transaction on the same key) now updates it: pushes v2
    // right after the still-present, invalidated v1.
    let stamp2 = TxStamp::new(1, 200);
    insert(&mut leaf, 1, stamp2, 12); // v2

    // Before the fix, this landed on v1 (invalid) and failed with
    // Err(()) instead of reaching v0 (the true, live predecessor).
    assert!(
        matches!(leaf.delete_after_update(1, stamp2), Ok(Some(_))),
        "delete_after_update must skip the invalidated v1 and mark v0 deleted"
    );
    leaf.commit_delta(-1, 1);

    let records: Vec<_> = leaf
        .as_records()
        .into_iter()
        .filter(|r| r.key == 1)
        .collect();
    assert_eq!(records.len(), 3);
    assert!(
        records[0].version().is_deleted(),
        "v0 must now be marked deleted by T2's update"
    );
    assert!(!records[1].version().is_live(), "v1 stays invalid");
    assert!(records[2].version().is_live(), "v2 is the new live value");
    assert_eq!(*records[2].payload(), 12);
}

#[test]
fn apply_invalidate_does_not_resurrect_an_unrelated_deletion() {
    let mut leaf = TestLeaf::new();
    let stamp_a = TxStamp::new(1, 100);
    let stamp_b = TxStamp::new(1, 200);

    // An earlier, unrelated transaction inserts then deletes key 1 —
    // completely committed history, nothing to do with what follows.
    insert(&mut leaf, 1, stamp_a, 1);
    assert!(leaf.delete(1, stamp_a).unwrap().is_some());
    leaf.commit_delta(-1, 1);
    assert_eq!(leaf.active_dead_count(), (0, 1));

    // A later transaction inserts key 1 fresh (allowed: the prior entry
    // is deleted, not live), then aborts.
    insert(&mut leaf, 1, stamp_b, 2);
    assert_eq!(leaf.abort_write(1, stamp_b), AbortOutcome::Invalidated);

    // The unrelated, genuinely-deleted original entry must stay
    // deleted — this abort has nothing to do with it.
    let records: Vec<_> = leaf
        .as_records()
        .into_iter()
        .filter(|r| r.key == 1)
        .collect();
    assert_eq!(records.len(), 2);
    assert!(
        records[0].version().is_deleted(),
        "the unrelated deletion must not be reverted"
    );
    assert!(
        !records[1].version().is_live(),
        "the aborted fresh insert must be invalid"
    );
}

#[test]
fn abort_write_reverts_a_plain_delete_past_a_trailing_invalidated_entry() {
    let mut leaf = TestLeaf::new();
    let stamp0 = TxStamp::new(1, 100);

    insert(&mut leaf, 1, stamp0, 10); // v0: the original, live value

    // An unrelated transaction inserts a second physical entry for the
    // same key, then aborts it: v1 ends up invalid, sitting physically
    // after v0 (which is still live).
    let stamp_x = TxStamp::new(1, 150);
    insert(&mut leaf, 1, stamp_x, 99); // v1
    assert_eq!(leaf.abort_write(1, stamp_x), AbortOutcome::Invalidated);
    assert_eq!(leaf.active_dead_invalid(), (1, 0, 1)); // v0 live, v1 invalid

    // T2 now plainly deletes key 1: `delete` skips the invalid v1 and
    // marks the true live v0 deleted.
    let stamp_t2 = TxStamp::new(1, 200);
    assert!(leaf.delete(1, stamp_t2).unwrap().is_some());
    leaf.commit_delta(-1, 1);
    assert_eq!(leaf.active_dead_invalid(), (0, 1, 1)); // v0 dead, v1 invalid

    assert_eq!(leaf.abort_write(1, stamp_t2), AbortOutcome::Undeleted);

    let records: Vec<_> = leaf
        .as_records()
        .into_iter()
        .filter(|r| r.key == 1)
        .collect();
    assert_eq!(records.len(), 2);
    assert!(records[0].version().is_live(), "v0 must be resurrected");
    assert_eq!(*records[0].payload(), 10);
    assert!(!records[1].version().is_live(), "v1 stays invalid");
    assert_eq!(leaf.active_dead_invalid(), (1, 0, 1));
}
