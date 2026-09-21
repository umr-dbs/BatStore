use crate::bat_record_model::tx_stamp::TxStamp;
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

/// Declares the version type.
pub type Version = u64;

/// Declares the atomic version type.
pub type AtomicVersion = AtomicU64;

/// Mask of a present `delete_stamp`, i.e. where bit 63 of the packed
/// representation is set — otherwise, no delete stamp (never deleted). Same
/// "steal the top bit" idiom `TxStamp::INVALID_FLAG` documents, applied to
/// whether a delete stamp is present at all rather than to a stamp's own
/// validity.
const DELETE_PRESENT_FLAG: u64 = 0x80_00000000000000;

/// The OSIC (worker, ts_start) stamp of the transaction that inserted this
/// record version, and, optionally, the stamp of the transaction that
/// deleted it.
///
/// Backed by `AtomicU64`, not plain `TxStamp` fields: `delete`/`invalidate`/
/// `undelete` (called by an in-place `update`/`delete` while holding the
/// block's exclusive write lock) mutate an *existing, already-visible*
/// record's stamp while concurrent OLC readers may be reading that very
/// record with no lock and no re-validation (see `bat_query::query`'s doc
/// for that separate, still-open gap). A plain, non-atomic field here let a
/// reader observe a torn mix of old and new bytes — corrupting
/// `worker_id`/`ts_start` into a nonsense value that slips straight past
/// `TxStamp::is_invalid` (a different bit) and panics much later indexing
/// `SnapshotCache` with it. Confirmed via ThreadSanitizer:
/// `VersionInfo::delete`'s write racing `VersionInfo::deletion_stamp`'s
/// read, both non-atomic on the same word. A same-sized atomic load/store
/// can never observe a torn value, closing this specific gap regardless of
/// whether the read side ever gets its own OLC re-validation.
pub struct VersionInfo {
    insert_stamp: AtomicU64,
    delete_stamp: AtomicU64,
}

impl Clone for VersionInfo {
    #[inline(always)]
    fn clone(&self) -> Self {
        Self {
            insert_stamp: AtomicU64::new(self.insert_stamp.load(Relaxed)),
            delete_stamp: AtomicU64::new(self.delete_stamp.load(Relaxed)),
        }
    }
}

impl Default for VersionInfo {
    #[inline(always)]
    fn default() -> Self {
        Self {
            insert_stamp: AtomicU64::new(0),
            delete_stamp: AtomicU64::new(0),
        }
    }
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
    pub fn new(insert_stamp: TxStamp) -> Self {
        Self {
            insert_stamp: AtomicU64::new(insert_stamp.raw()),
            delete_stamp: AtomicU64::new(0),
        }
    }

    /// Extended constructor, setting both fields via supplied parameters.
    #[inline(always)]
    pub fn from(insert_stamp: TxStamp, delete_stamp: TxStamp) -> Self {
        Self {
            insert_stamp: AtomicU64::new(insert_stamp.raw()),
            delete_stamp: AtomicU64::new(delete_stamp.raw() | DELETE_PRESENT_FLAG),
        }
    }

    #[inline(always)]
    pub fn matches<F: FnMut(TxStamp) -> bool + ?Sized>(&self, is_visible: &mut F) -> bool {
        is_visible(self.insertion_stamp())
            && !self
                .deletion_stamp()
                .map(|del| is_visible(del))
                .unwrap_or(false)
    }

    /// Retrieves the insertion stamp.
    #[inline(always)]
    pub fn insertion_stamp(&self) -> TxStamp {
        TxStamp::from_raw(self.insert_stamp.load(Relaxed))
    }

    /// Retrieves the deletion stamp.
    #[inline(always)]
    pub fn deletion_stamp(&self) -> Option<TxStamp> {
        let raw = self.delete_stamp.load(Relaxed);
        if raw & DELETE_PRESENT_FLAG == 0 {
            None
        } else {
            Some(TxStamp::from_raw(raw & !DELETE_PRESENT_FLAG))
        }
    }

    /// Returns true, if this version has been deleted.
    #[inline(always)]
    pub fn is_deleted(&self) -> bool {
        self.delete_stamp.load(Relaxed) & DELETE_PRESENT_FLAG != 0
    }

    #[inline(always)]
    pub fn is_live(&self) -> bool {
        !self.is_deleted() && !self.insertion_stamp().is_invalid()
    }

    #[inline(always)]
    pub fn invalidate(&self) {
        let invalidated = self.insertion_stamp().mark_invalid();
        self.insert_stamp.store(invalidated.raw(), Relaxed);
    }

    #[inline(always)]
    pub fn delete(&self, delete_stamp: TxStamp) -> bool {
        if !self.is_live() {
            false
        } else {
            self.delete_stamp
                .store(delete_stamp.raw() | DELETE_PRESENT_FLAG, Relaxed);
            true
        }
    }

    /// `&self` — see `invalidate`'s doc for why.
    #[inline(always)]
    pub fn undelete(&self) {
        self.delete_stamp.store(0, Relaxed);
    }
}

/// Implements standard pretty printers for VersionInfo, displaying both insertion and deletion stamps.
impl Display for VersionInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(insert: {}, deleted: {})",
            self.insertion_stamp(),
            self.deletion_stamp()
                .map(|del| del.to_string())
                .unwrap_or("*".to_string())
        )
    }
}
