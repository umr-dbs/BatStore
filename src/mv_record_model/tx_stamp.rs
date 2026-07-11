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
///
/// Packed into a single `u64` — `worker_id` in the high 16 bits, `ts_start` in
/// the low 47 bits — rather than a `{ worker_id: u16, ts_start: u64 }` struct.
/// The latter costs a whole extra 8-byte word to alignment padding (`u64`
/// demands 8-byte alignment, so the 10-byte payload rounds up to 16), which
/// doubles `VersionInfo` (two stamps per record) and forces a much smaller
/// leaf `NUM_RECORDS` to keep pages page-sized. Packing keeps `TxStamp` at
/// `size_of::<u64>` (8 bytes), restoring `VersionInfo`/leaf fan-out to their
/// pre-OSIC footprint. Bit 63 is deliberately left unused here — it's
/// `DeletedTxStamp`'s own presence flag (see `version_info::DeletedTxStamp`).
#[derive(Copy, Clone, Default, Eq, PartialEq)]
pub struct TxStamp(u64);

impl TxStamp {
    const WORKER_ID_BITS: u32 = 16;
    const TS_START_BITS: u32 = 63 - Self::WORKER_ID_BITS;
    const TS_START_MASK: u64 = (1u64 << Self::TS_START_BITS) - 1;
    const WORKER_ID_MASK: u64 = ((1u64 << Self::WORKER_ID_BITS) - 1) << Self::TS_START_BITS;

    #[inline(always)]
    pub const fn new(worker_id: WorkerId, ts_start: Version) -> Self {
        // Masking (rather than asserting) `ts_start` here is a deliberate
        // choice: with `TS_START_BITS` = 47, this clock would need to tick
        // over 140 trillion times before silently wrapping — asserting would
        // add a branch to every single write's hot path to guard against a
        // scenario no realistic run reaches.
        Self(((worker_id as u64) << Self::TS_START_BITS) | (ts_start & Self::TS_START_MASK))
    }

    #[inline(always)]
    pub const fn worker_id(&self) -> WorkerId {
        ((self.0 & Self::WORKER_ID_MASK) >> Self::TS_START_BITS) as WorkerId
    }

    #[inline(always)]
    pub const fn ts_start(&self) -> Version {
        self.0 & Self::TS_START_MASK
    }

    /// Raw packed bits, including whatever `DeletedTxStamp` has stashed in
    /// bit 63 — crate-private, only `version_info::DeletedTxStamp` (same
    /// crate, different module) needs this.
    #[inline(always)]
    pub(crate) const fn raw(self) -> u64 {
        self.0
    }

    #[inline(always)]
    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

/// Orders purely by `ts_start` — every non-visibility use site (SMO
/// merge/split ordering, "was this inserted after the newest live snapshot"
/// heuristics) only ever cares about recency, not who wrote it.
impl Ord for TxStamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts_start().cmp(&other.ts_start())
    }
}

impl PartialOrd for TxStamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for TxStamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(w{}@{})", self.worker_id(), self.ts_start())
    }
}
