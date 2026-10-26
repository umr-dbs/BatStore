use crate::bat_record_model::version_info::Version;
use std::fmt::{Display, Formatter};

/// Identifies one of a tree's fixed, bounded set of workers (OSIC, §3.1).
/// Kept here (not in `bat_sync`) so `VersionInfo`/`RecordPoint` can be stamped
/// with it without `bat_record_model` depending back on `bat_sync` (which
/// already depends on `bat_record_model` for `Version`).
pub type WorkerId = u16;

/// The two pieces of information OSIC needs on every written record version:
/// which worker wrote it, and that worker's transaction start timestamp.
/// Visibility is decided from this via `LCB(worker_id, reader_ts_start) >
/// ts_start` — see `bat_sync::visibility::is_visible` for the actual check.
///
/// Packed into a single `u64` rather than a `{ worker_id: u16, ts_start: u64 }`
/// struct. The latter costs a whole extra 8-byte word to alignment padding
/// (`u64` demands 8-byte alignment, so the 10-byte payload rounds up to 16),
/// which doubles `VersionInfo` (two stamps per record) and forces a much
/// smaller leaf `NUM_RECORDS` to keep pages page-sized. Packing keeps
/// `TxStamp` at `size_of::<u64>` (8 bytes), restoring `VersionInfo`/leaf
/// fan-out to their pre-OSIC footprint.
///
/// Layout (identical for insertion and deletion stamps):
///
/// ```text
///  63 | 62 ............ 48 | 47 .................... 0
///  I  |  worker_id (15 b)  |      ts_start (48 b)
/// ```
///
/// The high 16 bits form the worker field, whose top bit (63) is the
/// invalidation flag (`INVALID_FLAG`) — leaving 15 bits (`MAX_WORKER_ID`)
/// for the actual worker id. `DeletedTxStamp`/`VersionInfo` reuses that same
/// bit 63 as its delete-presence flag (a delete stamp is never itself
/// invalidated — an aborted delete is undone via `VersionInfo::undelete`).
#[derive(Copy, Clone, Default, Eq, PartialEq, Debug)]
pub struct TxStamp(u64);

impl TxStamp {
    /// Width of the whole worker field, including the invalidation bit.
    const WORKER_FIELD_BITS: u32 = 16;
    /// Bits actually available to the worker id (one is the invalid flag).
    const WORKER_ID_BITS: u32 = Self::WORKER_FIELD_BITS - 1;
    pub const TS_START_BITS: u32 = 64 - Self::WORKER_FIELD_BITS;
    const TS_START_MASK: u64 = (1u64 << Self::TS_START_BITS) - 1;
    const WORKER_ID_MASK: u64 = ((1u64 << Self::WORKER_ID_BITS) - 1) << Self::TS_START_BITS;
    /// Largest worker id a stamp can encode (`2^15 - 1`).
    pub const MAX_WORKER_ID: WorkerId = ((1u32 << Self::WORKER_ID_BITS) - 1) as WorkerId;
    /// Largest `ts_start` a stamp can encode (`2^48 - 1`).
    pub const MAX_TS_START: Version = Self::TS_START_MASK;
    /// Bit 63 — the top bit of the 16-bit worker field, which
    /// `worker_id`/`ts_start` never touch (see this type's doc). Set on a record's `insert_stamp` when the transaction
    /// that wrote it aborts (dropped without `commit()`) instead of
    /// physically undoing the write: an invalidated stamp is unconditionally
    /// invisible to every reader (`bat_sync::visibility::is_visible`) and
    /// treated as dead by SMO (`VersionInfo::is_live`), so the record is
    /// dropped at the next split/merge exactly like a properly-superseded
    /// one. Same "steal the top bit" idiom as
    /// `bat_page_model::time_matcher::OBSOLETE_VERSION_MARK` (a different
    /// field, internal-page child versions, not leaf record stamps).
    const INVALID_FLAG: u64 = 0x80_00000000000000;

    #[inline(always)]
    pub const fn new(worker_id: WorkerId, ts_start: Version) -> Self {
        // Unchecked in release (hot path); a worker id past 15 bits would
        // otherwise spill into `INVALID_FLAG`, hence the explicit masks.
        debug_assert!(worker_id <= Self::MAX_WORKER_ID, "TxStamp: worker_id exceeds 15 bits");
        debug_assert!(ts_start <= Self::MAX_TS_START, "TxStamp: ts_start exceeds 48 bits");
        Self(
            (((worker_id as u64) << Self::TS_START_BITS) & Self::WORKER_ID_MASK)
                | (ts_start & Self::TS_START_MASK),
        )
    }

    /// Checked `new`: `None` if either field doesn't fit its bit width.
    /// For decoding untrusted bytes (e.g. the WAL), where masking would
    /// silently turn a corrupt value into a different, valid-looking stamp.
    #[inline(always)]
    pub const fn try_new(worker_id: WorkerId, ts_start: Version) -> Option<Self> {
        if worker_id > Self::MAX_WORKER_ID || ts_start > Self::MAX_TS_START {
            None
        } else {
            Some(Self::new(worker_id, ts_start))
        }
    }

    #[inline(always)]
    pub const fn worker_id(&self) -> WorkerId {
        ((self.0 & Self::WORKER_ID_MASK) >> Self::TS_START_BITS) as WorkerId
    }

    #[inline(always)]
    pub const fn ts_start(&self) -> Version {
        self.0 & Self::TS_START_MASK
    }

    /// True once this stamp's transaction has aborted — see `INVALID_FLAG`'s
    /// doc. Orthogonal to `worker_id()`/`ts_start()`/`Ord` (none of them read
    /// bit 63), so marking a stamp invalid never changes what those report.
    #[inline(always)]
    pub const fn is_invalid(&self) -> bool {
        self.0 & Self::INVALID_FLAG != 0
    }

    #[inline(always)]
    pub const fn mark_invalid(&self) -> Self {
        Self(self.0 | Self::INVALID_FLAG)
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
/// heuristics) only ever cares about recency, not who wrote it. Deliberately
/// coarser than the derived `Eq`/`PartialEq` above (which compare all raw
/// bits, including the invalidation flag — see `mark_invalid`'s doc): two
/// stamps can compare `Ordering::Equal` here while still being `!=` (one
/// invalidated, one not). Nothing relies on `Ord`/`Eq` agreeing (no
/// sorted/hashed collection keyed by `TxStamp` — see `INVALID_FLAG`'s
/// history for why that distinction actually matters).
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
