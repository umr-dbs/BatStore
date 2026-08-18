use crate::bat_sync::clock::GlobalClock;
use crate::bat_sync::commit_log::CommitLog;

/// The bug this whole file's `commit_pruned` vs. plain `commit` split
/// exists to let callers avoid: with no snapshots ever registered as
/// active (an empty `active_snapshots` iterator every time, exactly
/// what a tree with no open transactions looks like), `commit_pruned`
/// must still keep the log bounded near `max_workers`, not grow with
/// every commit the way `commit` deliberately does.
#[test]
fn commit_pruned_stays_bounded_with_no_active_snapshots() {
    let glc = GlobalClock::new();
    let log = CommitLog::new();
    let max_workers = 8;

    for _ in 0..10_000 {
        log.commit_pruned(&glc, max_workers, std::iter::empty());
    }

    assert!(
        log.len() <= max_workers,
        "expected at most {max_workers} entries after pruning, got {}",
        log.len()
    );
}
