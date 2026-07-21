use crate::mv_page_model::node::{Active, Dead, PageLenField, PageLenPrimitive, active_len, dead_len, from_active_dead, from_len, from_len_sum};
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::tx_stamp::TxStamp;
use crate::mv_record_model::version_info::VersionInfo;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{Ordering::{Acquire, Relaxed, Release}};

pub struct LeafPage<
    const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> {
    pub(crate) len: PageLenField,
    pub(crate) record_data: [MaybeUninit<RecordPoint<Key, Payload>>; NUM_RECORDS],
    _marker: PhantomData<[RecordPoint<Key, Payload>]>,
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> Clone for LeafPage<NUM_RECORDS, Key, Payload> {
    fn clone(&self) -> Self {
        Self::from(self)
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> Default for LeafPage<NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        LeafPage::new()
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> Drop for LeafPage<NUM_RECORDS, Key, Payload> {
    fn drop(&mut self) {
        self.as_records_mut().iter_mut().for_each(|record| unsafe {
            (record as *mut RecordPoint<Key, Payload>)
                .drop_in_place()
        })
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> LeafPage<NUM_RECORDS, Key, Payload> {
    #[inline]
    pub(crate) fn from(leaf_page: &Self) -> Self {
        let mut new_page
            = Self::new();

        unsafe {
            leaf_page
                .as_records()
                .iter()
                .enumerate()
                .for_each(|(index, record)| new_page
                    .record_data
                    .as_mut_ptr()
                    .add(index)
                    .write(MaybeUninit::new((*record).clone()))
                );
        }

        let (active, dead)
            = leaf_page.active_dead_count();

        new_page.len.store(from_active_dead(active, dead), Release);

        new_page
    }

    #[inline(always)]
    pub const fn new() -> Self {
        // debug_assert!(mem::size_of::<Len>() +
        //                   mem::size_of::<[RecordPoint<Key, Payload>; NUM_RECORDS]>()
        //                   <= 4096, "FAN_OUT Invalid!");
        Self {
            len: PageLenField::new(0),
            record_data: unsafe { MaybeUninit::uninit().assume_init() }, // <[MaybeUninit<Entry>; NUM_RECORDS]>::
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn as_records(&self) -> &[RecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts(
                self.record_data.as_ptr() as *const RecordPoint<Key, Payload>,
                self.len())
        }
    }

    #[inline(always)]
    pub fn as_records_mut(&mut self) -> &mut [RecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.record_data.as_mut_ptr() as *mut _,
                self.len())
        }
    }

    #[inline(always)]
    pub fn as_records_uncommitted_mut(&mut self) -> &mut [RecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.record_data.as_mut_ptr() as *mut _,
                self.len() + 1)
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let len = self.len.load(Acquire) as _;
        // Pairs with the `fence(Release)` before every `len` store in this
        // file (`bulk_push`/`bulk_push_from_slice_ref`/`commit_delta`/
        // `from`) — without it, a reader that observes a bumped `len` isn't
        // guaranteed to see the record-data writes that preceded it (see
        // `mv_query::query::traverse_read_key`'s doc for the crash this
        // gap allowed once page reuse made it frequent enough to hit).
        // fence(Acquire);

        from_len_sum(len)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.len() == NUM_RECORDS
    }

    #[inline(always)]
    pub fn active_dead_count(&self) -> (Active, Dead) {
        from_len(self.len.load(Acquire))
    }

    #[inline(always)]
    pub fn active_dead_invalid(&self) -> (PageLenPrimitive, Active, Dead) {
        self.as_records().iter().fold((0,0,0),
        |(active, dead, invalid), record| {
            if !record.version.is_deleted() && !record.version.insert_stamp.is_invalid() {
                (active + 1, dead, invalid)
            } else if record.version.is_deleted() && !record.version.insert_stamp.is_invalid() {
                (active, dead + 1, invalid)
            } else {
                (active, dead, invalid + 1)
            }
        })
    }

    #[inline]
    pub fn push_uncommitted(&mut self, record: RecordPoint<Key, Payload>, index: usize) {
        debug_assert!(index < NUM_RECORDS, "LeafPage::push_uncommitted: index {index} out of bounds for NUM_RECORDS={NUM_RECORDS}");
        unsafe {
            self.record_data
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(record))
        }
    }

    #[inline(always)]
    pub fn commit_delta(&self, active_delta: i32, dead_delta: i32) {
        let len= self.len.load(Relaxed);
        let active = active_len(len) as i32 + active_delta;
        let dead = dead_len(len) as i32 + dead_delta;
        
        debug_assert!(active >= 0, 
                      "LeafPage active count went negative: len={len}, active_delta={active_delta}");
        debug_assert!(dead >= 0,
                      "LeafPage dead count went negative: len={len}, dead_delta={dead_delta}");
        // let active = active.max(0);
        // let dead = dead.max(0) as u32;
        
        self.len.store(from_active_dead(active as Active, dead as Dead), Release)
    }

    #[inline]
    pub fn undo_uncommitted(&mut self, index: usize) {
        unsafe {
            ptr::drop_in_place(self.record_data
                .as_mut_ptr()
                .add(index) as *mut RecordPoint<Key, Payload>);
        }
    }

    #[inline]
    pub fn on_reuse(&mut self) {
        let len = self.len();
        self.len.store(0, Release);

        unsafe {
            (0..len).for_each(|index| {
                ptr::drop_in_place(self.record_data
                    .as_mut_ptr()
                    .add(index) as *mut RecordPoint<Key, Payload>);
            });
        }
    }

    #[inline(always)]
    pub(crate) fn bulk_push(&mut self, records: Vec<&RecordPoint<Key, Payload>>) {
        let len
            = self.len();

        debug_assert_eq!(len, 0);
        let n_records_len
            = records.len();

        unsafe {
            records.into_iter().enumerate().for_each(|(index, record)| {
                self.record_data
                    .as_mut_ptr()
                    .add(index + len)
                    .write(MaybeUninit::new(record.clone()));
            });
        }

        // See `len()`'s doc.
        // fence(Release);
        self.len.store(
            from_active_dead(len as PageLenPrimitive + n_records_len as PageLenPrimitive, 0),
            Release)
    }

    #[inline(always)]
    pub(crate) fn bulk_push_from_slice_ref(&mut self, records: &[&RecordPoint<Key, Payload>]) {
        let len
            = self.len();

        debug_assert_eq!(len, 0);
        unsafe {
            records.into_iter().enumerate().for_each(|(index, record)| {
                self.record_data
                    .as_mut_ptr()
                    .add(index + len)
                    .write(MaybeUninit::new((*record).clone()));
            });
        }

        // See `len()`'s doc.
        // fence(Release);
        self.len.store(
            from_active_dead(len as PageLenPrimitive + records.len() as PageLenPrimitive, 0),
            Release)
    }

    // #[inline(always)]
    // pub(crate) fn bulk_push_from_slice(&mut self, records: &[RecordPoint<Key, Payload>]) {
    //     let len
    //         = self.len();
    //
    //     unsafe {
    //         records.into_iter().enumerate().for_each(|(index, record)| {
    //             self.record_data
    //                 .as_mut_ptr()
    //                 .add(index + len)
    //                 .write(MaybeUninit::new(record.clone()));
    //         });
    //     }
    //
    //     fence(Release);
    //     self.len.store(
    //         from_active_dead(len as LenP + records.len() as LenP, 0),
    //         Release)
    // }

    /// Skips physically-present but invalid entries (a since-aborted
    /// insert/update — see `TxStamp::is_invalid`'s doc) when hunting for
    /// "the" entry for `key`: an invalidated write isn't removed from the
    /// page until the next SMO, so it can sit between the true live/deleted
    /// lineage and whatever this call is looking for, and must not be
    /// mistaken for it (see `delete`/`delete_after_update`/`apply_invalidate`'s
    /// own doc for the concrete bug this closes).
    #[inline]
    fn is_live_lineage(record: &RecordPoint<Key, Payload>, key: Key) -> bool {
        record.key == key && !record.version().insertion_stamp().is_invalid()
    }

    #[inline]
    pub(crate) fn delete(&mut self, key: Key, del: TxStamp) -> Result<Option<VersionInfo>, ()>  {
        match self.as_records_mut()
            .iter_mut()
            .rfind(|record| Self::is_live_lineage(record, key))
        {
            Some(record) => {
                let ver_info = record
                    .version_mut();

                if ver_info.delete(del) {
                    Ok(Some(ver_info.clone()))
                } else {
                    Err(())
                }
            }
            _ => Ok(None)
        }
    }

    /// Reads back the newest entry for `key` (invalid or not) to locate the
    /// just-pushed entry this call supersedes, then walks past it (and past
    /// any invalid entries beyond it — see `is_live_lineage`) to reach the
    /// true previous live/deleted entry to mark deleted.
    #[inline]
    pub(crate) fn delete_after_update(&mut self, key: Key, del: TxStamp) -> Result<Option<VersionInfo>, ()>  {
        match self.as_records_mut()
            .iter_mut()
            .rev()
            .skip(1)
            .find(|record| Self::is_live_lineage(record, key))
        {
            Some(record) => {
                let ver_info = record
                    .version_mut();

                if ver_info.delete(del) {
                    Ok(Some(ver_info.clone()))
                } else {
                    Err(())
                }
            }
            _ => Ok(None)
        }
    }

    /// Live path only: decides which of `apply_invalidate`/`apply_undelete`
    /// this abort needs, by checking whether the *newest* entry for `key`
    /// was written by `my_stamp` (an `Insert`/`Update` — invalidate it) or
    /// not (a plain `Delete` of a pre-existing record — just undelete it).
    /// `NotFound` if there's no entry for `key` at all (defensive — a
    /// transaction only ever calls this for a key it itself wrote).
    ///
    /// Safe to call twice for the same key (e.g. a transaction that wrote it
    /// more than once, ending up in this same worker's write set twice):
    /// the second call's stamp-equality check compares raw bits, which no
    /// longer match once the first call's `apply_invalidate`/`apply_undelete`
    /// changed the entry (invalidating sets a bit; undeleting on a *plain*
    /// delete leaves no further `Delete`d entry to find under the same
    /// stamp), so it correctly falls through to a no-op `NotFound` rather
    /// than double-applying anything.
    #[inline]
    pub(crate) fn abort_write(&mut self, key: Key, my_stamp: TxStamp) -> AbortOutcome {
        let newest_is_mine = self.as_records()
            .iter()
            .rfind(|r| r.key == key)
            .map(|r| r.version().insertion_stamp() == my_stamp);

        match newest_is_mine {
            // None => AbortOutcome::NotFound,
            Some(true) => {
                self.apply_invalidate(key);
                AbortOutcome::Invalidated
            }
            // `apply_undelete` reports back whether it actually found a
            // deleted entry to undo — needed because "not mine" also
            // matches the idempotent-recall case (see this method's doc):
            // a key already fully processed by a prior `abort_write` call
            // has a newest entry that's now invalid (so no longer "mine"
            // by raw-stamp equality) but isn't deleted, and reporting that
            // as `Undeleted` would make the caller WAL-log a spurious op.
            Some(false) => if self.apply_undelete(key) {
                AbortOutcome::Undeleted
            } else {
                AbortOutcome::NotFound
            }
            _ => {
                AbortOutcome::NotFound
            }
        }
    }

    /// Marks the newest entry for `key` invalid (see `TxStamp::is_invalid`'s
    /// doc), then undeletes its predecessor *if and only if that predecessor
    /// was deleted by this exact same insertion stamp* — reversing an
    /// `Update`'s `delete_after_update`, which always deletes the
    /// predecessor under the very same stamp it inserts the new version
    /// with (see `Transaction::update`/`TpccTxn::update`). This is not the
    /// same thing as "the predecessor happens to be deleted": a plain
    /// `Insert`'s invalidation has no predecessor relationship at all, and
    /// if one of those lands right after some unrelated, already-committed
    /// transaction's genuine delete of the same key, a blind "is it deleted"
    /// check would wrongly resurrect that unrelated deletion. The
    /// predecessor search also skips any invalid entries in between (see
    /// `is_live_lineage`) — an aborted write isn't removed from the page
    /// until the next SMO, so one can sit between the entry being
    /// invalidated and its true predecessor.
    ///
    /// Adjusts `commit_delta` to match: the invalidated entry moves from
    /// active to dead (unless it was already deleted — e.g. a transaction
    /// that inserted then deleted the same key before aborting — in which
    /// case it's already counted dead and this is a no-op count-wise), and
    /// an undeleted predecessor moves back from dead to active. Used by
    /// `abort_write` above (live path) *and* directly by WAL replay of a
    /// logged `Invalidate` op — replay doesn't need the original stamp
    /// passed in either, since it's read back off the entry itself right
    /// before invalidating it (see `mv_wal::recovery`).
    #[inline]
    pub(crate) fn apply_invalidate(&mut self, key: Key) {
        let (stamp, was_live) = match self
            .as_records_mut()
            .iter_mut()
            .rfind(|r| r.key == key)
        {
            Some(record) => {
                let stamp = record.version().insertion_stamp();
                let was_live = record.version().is_live();
                record.version_mut().invalidate();
                (stamp, was_live)
            }
            None => return,
        };

        if was_live {
            self.commit_delta(-1, 1);
        }

        if let Some(record) = self
            .as_records_mut()
            .iter_mut()
            .rev()
            .skip(1)
            .find(|r| Self::is_live_lineage(r, key))
        {
            if record.version().deletion_stamp() == Some(stamp) {
                record.version_mut().undelete();
                self.commit_delta(1, -1);
            }
        }
    }

    /// Clears the newest entry's delete_stamp for `key`, adjusting
    /// `commit_delta` back from dead to active, and reports whether there
    /// was actually a deleted entry to undo. Used by `abort_write` (live
    /// path, reversing a plain `Delete`) and WAL replay of a logged
    /// `Undelete` op.
    ///
    /// Must search via `is_live_lineage`, not raw key equality: `delete`
    /// (the op this reverses) only ever marks a *live-lineage* record
    /// deleted, skipping past any invalid entry that sits physically after
    /// it (see `is_live_lineage`'s doc). A raw newest-by-key search would
    /// instead land on that trailing invalid entry — which is never
    /// deleted — and silently report `false`, leaving the true deleted
    /// record un-undone.
    #[inline]
    pub(crate) fn apply_undelete(&mut self, key: Key) -> bool {
        if let Some(record) = self
            .as_records_mut()
            .iter_mut()
            .rfind(|r| Self::is_live_lineage(r, key))
        {
            if record.version().is_deleted() {
                record.version_mut().undelete();
                self.commit_delta(1, -1);
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbortOutcome {
    /// No entry found for the key at all (defensive; shouldn't happen for a
    /// key this transaction actually wrote).
    NotFound,
    /// The newest entry was created by the aborting transaction (an
    /// `Insert`/`Update`) and has been marked invalid.
    Invalidated,
    // /// The newest entry pre-dated the aborting transaction, which only
    // /// deleted it (a plain `Delete`) — it has been undeleted.
    Undeleted,
}

