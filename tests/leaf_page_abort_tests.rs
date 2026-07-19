use crate::mv_page_model::leaf_page::{AbortOutcome, LeafPage};
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::tx_stamp::TxStamp;
use crate::mv_record_model::version_info::VersionInfo;

const NUM_RECORDS: usize = 8;
type TestLeaf = LeafPage<NUM_RECORDS, u64, u64>;

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
    assert_eq!(leaf.active_dead_count(), (0, 1));

    // Idempotent: processing the same key's abort twice must not
    // double-adjust the counts.
    assert_eq!(leaf.abort_write(1, stamp), AbortOutcome::NotFound);
    assert_eq!(leaf.active_dead_count(), (0, 1));
}

/// Reverting an aborted `Update`: the newer entry must be invalidated
/// *and* the older entry it superseded (via `delete_after_update`) must
/// come back to life — net counts must return to exactly what they were
/// before the update, since it's as if the update never happened.
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

    let records: Vec<_> = leaf.as_records().into_iter().filter(|r| r.key == 5).collect();
    assert_eq!(records.len(), 2);
    assert!(!records[1].version().is_live(), "the update's own new entry must be invalidated");
    assert!(records[0].version().is_live(), "the original entry must be resurrected");
    assert_eq!(*records[0].payload(), 1, "the resurrected entry is the original value");

    // The invalidated entry is still physically present (SMO drops it
    // at the next split/version-compaction, see smo.rs's `is_live()`
    // filters) — one live (the resurrected original) + one dead (the
    // now-invalidated update), not zero dead.
    assert_eq!(leaf.active_dead_count(), (1, 1));
}

/// Regression for a bug found via the TPC-C smoke benchmark: after one
/// update's abort leaves an invalidated entry sitting physically between
/// the true (resurrected) predecessor and wherever the next write lands,
/// a *second* update to the same key must still find and mark that true
/// predecessor deleted — not the invalidated entry that happens to be
/// nearer (which `delete_after_update` used to grab, since it only ever
/// looked at the physically-second-to-last entry for the key).
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
    assert!(matches!(leaf.delete_after_update(1, stamp2), Ok(Some(_))),
        "delete_after_update must skip the invalidated v1 and mark v0 deleted");
    leaf.commit_delta(-1, 1);

    let records: Vec<_> = leaf.as_records().into_iter().filter(|r| r.key == 1).collect();
    assert_eq!(records.len(), 3);
    assert!(records[0].version().is_deleted(), "v0 must now be marked deleted by T2's update");
    assert!(!records[1].version().is_live(), "v1 stays invalid");
    assert!(records[2].version().is_live(), "v2 is the new live value");
    assert_eq!(*records[2].payload(), 12);
}

/// Regression for a second bug found alongside the one above:
/// invalidating a plain `Insert` must never resurrect an unrelated,
/// already-deleted predecessor for the same key — only a predecessor
/// deleted *by the very same stamp being invalidated* (i.e. an
/// `Update`'s own `delete_after_update`) may be undeleted.
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
    let records: Vec<_> = leaf.as_records().into_iter().filter(|r| r.key == 1).collect();
    assert_eq!(records.len(), 2);
    assert!(records[0].version().is_deleted(), "the unrelated deletion must not be reverted");
    assert!(!records[1].version().is_live(), "the aborted fresh insert must be invalid");
}
