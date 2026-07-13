use parking_lot::Mutex;
use crate::mv_record_model::version_info::Version;
use crate::mv_sync::clock::GlobalClock;

/// Per-worker OSIC Commit Log (§3.1): a small, ascending list of a single
/// worker's commit timestamps, guarded by one mutex, matching the paper
/// directly. (An `RwLock` was tried here on the theory that cross-worker
/// `lcb` reads from many workers could run fully in parallel against each
/// other, only serializing against the rare owning-worker append/prune —
/// measured under a mixed OLTP+OLAP workload on both a 12-core dev machine
/// and a real server, with no measurable improvement over the plain mutex
/// either time. Reverted rather than kept as unproven complexity — see
/// `commit`'s doc for where the actual cost in this path comes from
/// instead.) Both the owning worker's append (`commit`/`commit_pruned`) and
/// any other worker's `lcb` query take this same mutex — this is what rules
/// out the race the paper's Figure 3 walks through (a concurrent reader must
/// not be able to observe a commit timestamp reserved on the GLC before it
/// has actually been inserted into the log).
///
/// This log only tracks *visibility* (when a write starts being seen by
/// other transactions), not WAL durability — writes are logged and flushed
/// asynchronously, in batches, independently of this log; see
/// `MVBTSt::wal_hardened_version` for that side.
pub struct CommitLog {
    entries: Mutex<Vec<Version>>,
}

impl CommitLog {
    pub fn new() -> Self {
        Self { entries: Mutex::new(Vec::new()) }
    }

    /// Draws `ts_commit` from `glc` and appends it, without ever pruning.
    /// `MVBTSt::commit_tx` falls back to this whenever block-reclaim GC is
    /// disabled (see that method's and `TrackerHandleSt`'s docs for why
    /// pruning isn't sound without it), so each worker's log then grows
    /// unboundedly for the tree's lifetime — accepted the same way a GC-off
    /// tree already accepts unbounded dead pages. Also usable directly by
    /// anything (e.g. tests) that wants a `CommitLog` in isolation, without
    /// a registry to prune against at all.
    ///
    /// A confirmed (not just flat-profile-inferred — checked via matching
    /// `perf script` call stacks directly) cost of a long GC-off run: this
    /// `Vec` growing without bound means `push` occasionally has to demand-
    /// page-fault in the freshly-grown backing memory, with `commit` itself
    /// on the stack when that happens (~2.4% of all `commit`-attributed
    /// samples in one profiling run). Reallocation's *copy* cost couldn't be
    /// pinned on this function the same way in that same profile — the
    /// `memmove`/`rallocx` time visible elsewhere never co-occurred with
    /// `commit` in a single stack, so unlike the page faults, that specific
    /// connection is unconfirmed. A chunked/segmented log (fixed-size blocks
    /// appended without ever copying old ones) would remove the page-fault
    /// cost too; not done here since it's a real rewrite of `prune`/
    /// `lcb_index`'s cross-block search, not a one-line change.
    pub fn commit(&self, glc: &GlobalClock) -> Version {
        let mut entries = self.entries.lock();
        let ts_commit = glc.next_timestamp();
        entries.push(ts_commit);
        ts_commit
    }

    /// Same as `commit`, but once the log reaches `max_workers` entries (the
    /// paper's size bound), prunes down to just the entries that are still
    /// the `LCB` of some snapshot in `active_snapshots` — safe only because
    /// the caller guarantees `active_snapshots` enumerates *every* currently
    /// open snapshot (see `TrackerHandleSt::active_snapshots`).
    pub fn commit_pruned(
        &self,
        glc: &GlobalClock,
        max_workers: usize,
        active_snapshots: impl Iterator<Item = Version>,
    ) -> Version {
        let mut entries = self.entries.lock();
        let ts_commit = glc.next_timestamp();
        entries.push(ts_commit);

        if entries.len() >= max_workers {
            Self::prune(&mut entries, active_snapshots);
        }

        ts_commit
    }

    fn prune(entries: &mut Vec<Version>, active_snapshots: impl Iterator<Item = Version>) {
        let mut keep = vec![false; entries.len()];

        for ts_start in active_snapshots {
            if let Some(i) = Self::lcb_index(entries, ts_start) {
                keep[i] = true;
            }
        }

        // The newest entry is always the LCB for any future ts_start drawn
        // after this prune and before this worker's next commit — keep it
        // unconditionally so `lcb` stays correct for transactions not born yet.
        if let Some(last) = keep.last_mut() {
            *last = true;
        }

        let mut kept = 0;
        for i in 0..entries.len() {
            if keep[i] {
                entries[kept] = entries[i];
                kept += 1;
            }
        }
        entries.truncate(kept);
    }

    #[inline]
    fn lcb_index(entries: &[Version], ts: Version) -> Option<usize> {
        entries.partition_point(|&e| e < ts).checked_sub(1)
    }

    /// `LCB(w, ts)`: the last commit timestamp on this worker strictly
    /// before `ts`, or `0` (safe sentinel — record `ts_start`s are always
    /// `>= START_VERSION == 1`) if this worker has never committed before `ts`.
    pub fn lcb(&self, ts: Version) -> Version {
        let entries = self.entries.lock();
        Self::lcb_index(&entries, ts).map(|i| entries[i]).unwrap_or(0)
    }

    /// Current entry count — for tests/diagnostics confirming pruning keeps
    /// this bounded rather than growing without limit.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
