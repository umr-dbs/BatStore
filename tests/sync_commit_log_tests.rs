use crate::bat_sync::clock::GlobalClock;
use crate::bat_sync::commit_log::CommitLog;

#[test]
fn commit_pruned_stays_bounded_with_no_active_snapshots() {
    let glc = GlobalClock::new();
    let log = CommitLog::new();
    let max_workers = 8;

    for _ in 0..10_000 {
        log.commit_pruned(&glc, max_workers, std::iter::empty(), std::iter::empty());
    }

    assert!(
        log.len() <= max_workers,
        "expected at most {max_workers} entries after pruning, got {}",
        log.len()
    );
}

#[test]
fn retained_log_is_pruned_once_per_worker_window() {
    let glc = GlobalClock::new();
    let log = CommitLog::new();
    let max_workers = 4;
    let oldest_possible_snapshot = glc.current_version();

    // This in-flight bound deliberately forces every entry to survive each
    // prune, reproducing the case that previously retriggered pruning on
    // every subsequent commit because len remained >= max_workers.
    for _ in 0..max_workers {
        log.commit_pruned(
            &glc,
            max_workers,
            [oldest_possible_snapshot].into_iter(),
            std::iter::empty(),
        );
    }
    assert_eq!(log.prune_count(), 1);

    for _ in 0..max_workers - 1 {
        log.commit_pruned(
            &glc,
            max_workers,
            [oldest_possible_snapshot].into_iter(),
            std::iter::empty(),
        );
    }
    assert_eq!(
        log.prune_count(),
        1,
        "a retained log must not retrigger pruning on every commit"
    );

    log.commit_pruned(
        &glc,
        max_workers,
        [oldest_possible_snapshot].into_iter(),
        std::iter::empty(),
    );
    assert_eq!(log.prune_count(), 2);
}

#[test]
fn in_flight_bound_preserves_commits_before_the_actual_snapshot() {
    let glc = GlobalClock::new();
    let log = CommitLog::new();
    let no_snapshots = std::iter::empty;

    assert_eq!(glc.next_timestamp(), 1); // writer's first start
    assert_eq!(
        log.commit_pruned(&glc, 2, no_snapshots(), no_snapshots()),
        2
    );
    let bound = glc.current_version();
    assert_eq!(bound, 3);

    assert_eq!(glc.next_timestamp(), 3); // next writer start
    assert_eq!(
        log.commit_pruned(&glc, 2, [bound].into_iter(), no_snapshots()),
        4
    );
    let reader_snapshot = glc.next_timestamp();
    assert_eq!(reader_snapshot, 5); // drawn but not yet published
    assert_eq!(glc.next_timestamp(), 6); // writer starts again
    assert_eq!(
        log.commit_pruned(&glc, 2, [bound].into_iter(), no_snapshots()),
        7
    );

    assert_eq!(log.lcb(reader_snapshot), 4);
    // Once published, the exact snapshot takes over from the lower bound.
    assert_eq!(
        log.commit_pruned(&glc, 2, no_snapshots(), [reader_snapshot].into_iter()),
        8
    );
    assert_eq!(log.lcb(reader_snapshot), 4);
}

#[test]
fn long_overlapping_registrations_match_unpruned_lcb_history() {
    let glc = GlobalClock::new();
    let log = CommitLog::new();
    let mut all_commits = Vec::new();
    let mut bounds = Vec::new();
    let mut snapshots = Vec::new();

    // One published reader and two registrations whose exact timestamps
    // arrive much later. Keep a separate, never-pruned commit history as the
    // oracle for every snapshot while the writer repeatedly triggers prune.
    snapshots.push(glc.next_timestamp());
    bounds.push(glc.current_version());
    for step in 0..1_024 {
        if step == 256 {
            snapshots.push(glc.next_timestamp()); // first registration draws
        }
        if step == 384 {
            bounds.push(glc.current_version()); // second registration begins
        }
        if step == 768 {
            snapshots.push(glc.next_timestamp()); // second registration draws
        }

        glc.next_timestamp(); // writer starts its next transaction
        let committed =
            log.commit_pruned(&glc, 3, bounds.iter().copied(), snapshots.iter().copied());
        all_commits.push(committed);

        for &snapshot in &snapshots {
            let expected = all_commits
                .iter()
                .copied()
                .filter(|&c| c < snapshot)
                .last()
                .unwrap_or(0);
            assert_eq!(
                log.lcb(snapshot),
                expected,
                "step {step}, snapshot {snapshot}"
            );
        }
        // A future reader may start before the next writer commit.
        assert_eq!(log.lcb(glc.current_version()), committed);
    }

    assert!(
        log.len() > 3,
        "a held registration must retain its intervening commits"
    );

    // Hand off both bounds to their exact, still-live snapshots. Repeated
    // commits may now compact the intervening history but not those LCBs.
    bounds.clear();
    for _ in 0..128 {
        glc.next_timestamp();
        let committed =
            log.commit_pruned(&glc, 3, bounds.iter().copied(), snapshots.iter().copied());
        all_commits.push(committed);
        for &snapshot in &snapshots {
            let expected = all_commits
                .iter()
                .copied()
                .filter(|&c| c < snapshot)
                .last()
                .unwrap_or(0);
            assert_eq!(log.lcb(snapshot), expected);
        }
    }
    assert!(log.len() <= snapshots.len() + 1);

    snapshots.clear();
    let prune_count = log.prune_count();
    for _ in 0..3 {
        glc.next_timestamp();
        log.commit_pruned(&glc, 3, std::iter::empty(), std::iter::empty());
        if log.prune_count() != prune_count {
            break;
        }
    }
    assert_eq!(log.prune_count(), prune_count + 1);
    assert_eq!(log.len(), 1);
}
