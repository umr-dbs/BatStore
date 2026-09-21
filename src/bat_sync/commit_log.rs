use crate::bat_record_model::version_info::Version;
use crate::bat_sync::clock::GlobalClock;
use parking_lot::Mutex;

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
    state: Mutex<CommitLogState>,
}

struct CommitLogState {
    entries: Vec<Version>,
    /// Debug-only archive for exact historical Explorer visibility after the
    /// runtime LCB log has pruned entries that active readers no longer need.
    #[cfg(feature = "tree-viz")]
    history: Vec<Version>,
    /// Reused by every prune instead of allocating a fresh bitmap on the
    /// commit path. Capacity follows the largest observed log size.
    keep: Vec<bool>,
    /// Number of pruned commits that may be appended before the next prune.
    /// Pruning once per `max_workers` commits amortizes the worker-slot scan
    /// while keeping at most `max_workers - 1` newly-obsolete entries.
    /// Zero also invalidates the schedule after an unpruned `commit` call.
    commits_until_prune: usize,
    #[cfg(test)]
    prune_count: usize,
}

impl CommitLog {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(CommitLogState {
                entries: Vec::new(),
                #[cfg(feature = "tree-viz")]
                history: Vec::new(),
                keep: Vec::new(),
                commits_until_prune: 0,
                #[cfg(test)]
                prune_count: 0,
            }),
        }
    }

    pub fn commit(&self, glc: &GlobalClock) -> Version {
        let mut state = self.state.lock();
        let ts_commit = glc.next_timestamp();
        state.entries.push(ts_commit);
        #[cfg(feature = "tree-viz")]
        state.history.push(ts_commit);
        // If callers later switch back to `commit_pruned`, make its first
        // call derive a fresh schedule from the (possibly large) log.
        state.commits_until_prune = 0;
        ts_commit
    }

    pub fn commit_pruned(
        &self,
        glc: &GlobalClock,
        max_workers: usize,
        in_flight_bounds: impl Iterator<Item = Version>,
        active_snapshots: impl Iterator<Item = Version>,
    ) -> Version {
        let mut state = self.state.lock();
        let ts_commit = glc.next_timestamp();
        state.entries.push(ts_commit);
        #[cfg(feature = "tree-viz")]
        state.history.push(ts_commit);

        let prune_interval = max_workers.max(1);
        let should_prune = if state.commits_until_prune == 0 {
            if state.entries.len() >= prune_interval {
                true
            } else {
                state.commits_until_prune = prune_interval - state.entries.len();
                false
            }
        } else if state.commits_until_prune == 1 {
            true
        } else {
            state.commits_until_prune -= 1;
            false
        };

        if should_prune {
            Self::prune(&mut state, in_flight_bounds, active_snapshots);
            state.commits_until_prune = prune_interval;
        }

        ts_commit
    }

    fn prune(
        state: &mut CommitLogState,
        in_flight_bounds: impl Iterator<Item = Version>,
        active_snapshots: impl Iterator<Item = Version>,
    ) {
        let len = state.entries.len();
        state.keep.resize(len, false);
        state.keep.fill(false);

        #[cfg(test)]
        {
            state.prune_count += 1;
        }

        if let Some(bound) = in_flight_bounds.min() {
            let first_at_or_after = state.entries.partition_point(|&e| e < bound);
            if let Some(previous) = first_at_or_after.checked_sub(1) {
                state.keep[previous] = true;
            }
            state.keep[first_at_or_after..].fill(true);
        }

        for ts_start in active_snapshots {
            if let Some(i) = Self::lcb_index(&state.entries, ts_start) {
                state.keep[i] = true;
            }
        }

        // The newest entry is always the LCB for any future ts_start drawn
        // after this prune and before this worker's next commit — keep it
        // unconditionally so `lcb` stays correct for transactions not born yet.
        if let Some(last) = state.keep.last_mut() {
            *last = true;
        }

        let mut kept = 0;
        for i in 0..len {
            if state.keep[i] {
                state.entries[kept] = state.entries[i];
                kept += 1;
            }
        }
        state.entries.truncate(kept);
    }

    #[inline]
    fn lcb_index(entries: &[Version], ts: Version) -> Option<usize> {
        entries.partition_point(|&e| e < ts).checked_sub(1)
    }

    /// `LCB(w, ts)`: the last commit timestamp on this worker strictly
    /// before `ts`, or `0` (safe sentinel — record `ts_start`s are always
    /// `>= START_VERSION == 1`) if this worker has never committed before `ts`.
    pub fn lcb(&self, ts: Version) -> Version {
        let state = self.state.lock();
        Self::lcb_index(&state.entries, ts)
            .map(|i| state.entries[i])
            .unwrap_or(0)
    }

    #[cfg(feature = "tree-viz")]
    pub(crate) fn dump_entries(&self) -> (Vec<String>, bool) {
        let state = self.state.lock();
        (state.history.iter().map(Version::to_string).collect(), true)
    }

    /// Current entry count — for tests/diagnostics confirming pruning keeps
    /// this bounded rather than growing without limit.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.state.lock().entries.len()
    }

    #[cfg(test)]
    pub(crate) fn prune_scratch_capacity(&self) -> usize {
        self.state.lock().keep.capacity()
    }

    #[cfg(test)]
    pub(crate) fn prune_count(&self) -> usize {
        self.state.lock().prune_count
    }
}
