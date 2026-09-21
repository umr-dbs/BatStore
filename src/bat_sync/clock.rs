use crate::bat_record_model::version_info::{AtomicVersion, Version};
use crate::bat_sync::version_handle;
use std::sync::atomic::Ordering::Relaxed;

/// The OSIC Global Logical Clock (§3.1): every transaction draws two
/// timestamps from this single atomic counter — `ts_start` at begin and
/// `ts_commit` just before committing — which is what the Transitive Commit
/// Invariant (and thus `CommitLog::lcb`) relies on to totally order starts
/// and commits across all workers. A plain `fetch_add` handles both draws;
/// unlike the single-version scheme this replaces, there is no separate
/// "publish" step (see `bat_sync::commit_log`/`bat_sync::visibility`) — once a
/// timestamp is drawn it's immediately valid to compare.
pub(crate) struct GlobalClock(pub(crate) AtomicVersion);

impl GlobalClock {
    pub(crate) fn new() -> GlobalClock {
        GlobalClock(AtomicVersion::new(version_handle::START_VERSION))
    }

    #[inline(always)]
    pub(crate) fn current_version(&self) -> Version {
        self.0.load(Relaxed)
    }

    #[inline(always)]
    pub(crate) fn next_timestamp(&self) -> Version {
        self.0.fetch_add(1, Relaxed)
    }
}
