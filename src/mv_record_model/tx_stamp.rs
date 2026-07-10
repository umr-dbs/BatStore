use std::fmt::{Display, Formatter};
use crate::mv_record_model::version_info::Version;

/// Identifies one of a tree's fixed, bounded set of workers (OSIC, §3.1).
/// Kept here (not in `mv_sync`) so `VersionInfo`/`RecordPoint` can be stamped
/// with it without `mv_record_model` depending back on `mv_sync` (which
/// already depends on `mv_record_model` for `Version`).
pub type WorkerId = u16;

/// The two pieces of information OSIC needs on every written record version:
/// which worker wrote it, and that worker's transaction start timestamp.
/// Visibility is decided from this via `LCB(worker_id, reader_ts_start) >
/// ts_start` — see `mv_sync::visibility::is_visible` for the actual check.
#[derive(Copy, Clone, Default, Eq, PartialEq)]
pub struct TxStamp {
    pub worker_id: WorkerId,
    pub ts_start: Version,
}

impl TxStamp {
    #[inline(always)]
    pub const fn new(worker_id: WorkerId, ts_start: Version) -> Self {
        Self { worker_id, ts_start }
    }
}

/// Orders purely by `ts_start` — every non-visibility use site (SMO
/// merge/split ordering, "was this inserted after the newest live snapshot"
/// heuristics) only ever cares about recency, not who wrote it.
impl Ord for TxStamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts_start.cmp(&other.ts_start)
    }
}

impl PartialOrd for TxStamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for TxStamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(w{}@{})", self.worker_id, self.ts_start)
    }
}
