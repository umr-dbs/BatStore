use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicU64;
use crate::mv_record_model::tx_stamp::TxStamp;

/// Declares the version type.
pub type Version = u64;

/// Declares the atomic version type.
pub type AtomicVersion = AtomicU64;

/// A `TxStamp` that may or may not be present, distinguished by one leading
/// marker bit stolen from its `ts_start` — the same trick the original
/// (pre-OSIC) `DeletedVersion` used for a bare `Version`, just applied to a
/// full `TxStamp` now so `VersionInfo` doesn't need `Option<TxStamp>` (which,
/// lacking any niche to exploit, would otherwise cost a whole extra
/// discriminant + padding on top of `TxStamp`'s own size).
#[derive(Clone, Copy)]
struct DeletedTxStamp(TxStamp);

impl DeletedTxStamp {
    /// Mask of a non-null stamp, i.e. where the left outer most bit of
    /// `ts_start` is set. Otherwise, a null mapping, i.e. does not exist.
    const NON_NULL_FLAG: Version = 0x80_00000000000000;

    /// Defines the null instance.
    const NULL_FLAG: Version = 0;

    /// The actual mask for selecting `ts_start`.
    const EXTRACTOR: Version = 0x7F_FFFFFFFFFFFFFF;

    #[inline(always)]
    const fn new_null() -> Self {
        Self(TxStamp::new(0, Self::NULL_FLAG))
    }

    #[inline(always)]
    const fn new(stamp: TxStamp) -> Self {
        Self(TxStamp::new(stamp.worker_id, stamp.ts_start | Self::NON_NULL_FLAG))
    }

    /// Retrieves the underlying stamp if present, otherwise None.
    #[inline(always)]
    const fn get(&self) -> Option<TxStamp> {
        match self.0.ts_start & Self::NON_NULL_FLAG {
            0 => None,
            _ => Some(TxStamp::new(self.0.worker_id, self.0.ts_start & Self::EXTRACTOR)),
        }
    }
}

impl Default for DeletedTxStamp {
    #[inline(always)]
    fn default() -> Self {
        Self::new_null()
    }
}

/// Defines the version information structure: the OSIC (worker, ts_start)
/// stamp of the transaction that inserted this record version, and,
/// optionally, the stamp of the transaction that deleted it.
#[derive(Clone, Copy, Default)]
pub struct VersionInfo {
    pub insert_stamp: TxStamp,
    delete_stamp: DeletedTxStamp,
}

/// Sugar implementation, wrapping a stamp into a VersionInfo.
impl Into<VersionInfo> for TxStamp {
    #[inline(always)]
    fn into(self) -> VersionInfo {
        VersionInfo::new(self)
    }
}

/// Managing methods implementation for VersionInfo.
impl VersionInfo {
    /// Basic constructor, setting insertion stamp via supplied stamp and deletion to None.
    #[inline(always)]
    pub const fn new(insert_stamp: TxStamp) -> Self {
        Self {
            insert_stamp,
            delete_stamp: DeletedTxStamp::new_null(),
        }
    }

    /// Extended constructor, setting both fields via supplied parameters.
    #[inline(always)]
    pub const fn from(insert_stamp: TxStamp, delete_stamp: TxStamp) -> Self {
        Self {
            insert_stamp,
            delete_stamp: DeletedTxStamp::new(delete_stamp),
        }
    }

    /// Returns true iff this version is visible to a reader for whom
    /// `is_visible(stamp)` decides whether a given writer's stamp has
    /// already committed (see `mv_sync::visibility::is_visible` for the
    /// actual OSIC LCB-based check) — visible iff the insertion is visible
    /// and, if deleted, the deletion is not (yet) visible to this reader.
    #[inline(always)]
    pub fn matches(&self, is_visible: &mut impl FnMut(TxStamp) -> bool) -> bool {
        is_visible(self.insert_stamp)
            && !self.delete_stamp.get().map(|del| is_visible(del)).unwrap_or(false)
    }

    /// Retrieves the insertion stamp.
    #[inline(always)]
    pub const fn insertion_stamp(&self) -> TxStamp {
        self.insert_stamp
    }

    /// Retrieves the deletion stamp.
    #[inline(always)]
    pub const fn deletion_stamp(&self) -> Option<TxStamp> {
        self.delete_stamp.get()
    }

    /// Returns true, if this version has been deleted.
    #[inline(always)]
    pub const fn is_deleted(&self) -> bool {
        self.delete_stamp.get().is_some()
    }

    /// Actively deletes this version by setting deletion to supplied delete stamp.
    #[inline(always)]
    pub fn delete(&mut self, delete_stamp: TxStamp) -> bool {
        if self.is_deleted() {
            false
        } else {
            self.delete_stamp = DeletedTxStamp::new(delete_stamp);
            true
        }
    }

    #[inline(always)]
    pub fn undelete(&mut self) {
        self.delete_stamp = DeletedTxStamp::new_null()
    }
}

/// Implements standard pretty printers for VersionInfo, displaying both insertion and deletion stamps.
impl Display for VersionInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(insert: {}, deleted: {})",
            self.insert_stamp,
            self.delete_stamp
                .get()
                .map(|del| del.to_string())
                .unwrap_or("*".to_string())
        )
    }
}
