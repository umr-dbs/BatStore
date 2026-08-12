use crate::mv_record_model::version_info::{AtomicVersion, Version};
use crate::mv_sync::version_handle;
use std::sync::atomic::Ordering::Relaxed;

/// The OSIC Global Logical Clock (§3.1): every transaction draws two
/// timestamps from this single atomic counter — `ts_start` at begin and
/// `ts_commit` just before committing — which is what the Transitive Commit
/// Invariant (and thus `CommitLog::lcb`) relies on to totally order starts
/// and commits across all workers. A plain `fetch_add` handles both draws;
/// unlike the single-version scheme this replaces, there is no separate
/// "publish" step (see `mv_sync::commit_log`/`mv_sync::visibility`) — once a
/// timestamp is drawn it's immediately valid to compare.
pub(crate) struct GlobalClock(pub(crate) AtomicVersion);

impl GlobalClock {
    pub(crate) fn new() -> GlobalClock {
        GlobalClock(AtomicVersion::new(version_handle::START_VERSION))
    }

    /// The clock's current position, without drawing a new tick. Used for
    /// structural/root-chain versioning (`mv_tree::smo`, `mv_wal::recovery`),
    /// which stays on this flat, worker-agnostic `Version` scheme — SMOs are
    /// physical maintenance, not user transactions, so they don't need an
    /// OSIC `TxStamp`.
    #[inline(always)]
    pub(crate) fn current_version(&self) -> Version {
        self.0.load(Relaxed)
    }

    /// Draws the next unique, totally-ordered timestamp — used for both
    /// `ts_start` (transaction begin) and `ts_commit` (transaction commit),
    /// per the paper ("every transaction draws two timestamps from the same
    /// GLC"), as well as for plain structural version stamps.
    #[inline(always)]
    pub(crate) fn next_timestamp(&self) -> Version {
        // Atomic modification order alone gives every fetch_add a unique,
        // globally ordered value. The clock does not publish any page or
        // transaction data: those happens-before edges are supplied by the
        // commit-log mutex and the snapshot slots' Release/Acquire pairs.
        // SeqCst therefore added a global fence/order constraint without
        // contributing to the OSIC timestamp order.
        self.0.fetch_add(1, Relaxed)
    }
}
