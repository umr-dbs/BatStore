use crate::bat_page_model::node::{
    Active, Dead, PageLenField, PageLenPrimitive, active_len, dead_len, from_active_dead, from_len,
    from_len_sum,
};
use crate::bat_record_model::record_point::{PayloadSlot, RecordPoint};
use crate::bat_record_model::tx_stamp::TxStamp;
use crate::bat_record_model::version_info::VersionInfo;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

struct LeafData<Payload> {
    version: VersionInfo,
    payload: PayloadSlot<Payload>,
}

/// Borrowed, zero-copy view of one structure-of-arrays leaf slot.
#[derive(Clone, Copy)]
pub struct LeafRecordRef<'a, Key, Payload> {
    pub key: Key,
    pub version: &'a VersionInfo,
    payload: &'a PayloadSlot<Payload>,
}

impl<'a, Key: Copy, Payload> LeafRecordRef<'a, Key, Payload> {
    #[inline(always)]
    pub const fn key(&self) -> Key {
        self.key
    }
    #[inline(always)]
    pub const fn version(&self) -> &'a VersionInfo {
        self.version
    }
    #[inline(always)]
    pub fn payload(&self) -> &'a Payload {
        self.payload.get()
    }
    #[inline(always)]
    pub(crate) fn payload_slot(&self) -> &'a PayloadSlot<Payload> {
        self.payload
    }
}

impl<Key: Display, Payload> Display for LeafRecordRef<'_, Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordPoint(Key: {}, Version: {})",
            self.key, self.version
        )
    }
}

pub(crate) trait LeafRecordSource<Key, Payload> {
    fn source_key(&self) -> Key;
    fn source_version(&self) -> &VersionInfo;
    fn source_payload_slot(&self) -> &PayloadSlot<Payload>;
}

impl<Key: Copy, Payload> LeafRecordSource<Key, Payload> for LeafRecordRef<'_, Key, Payload> {
    #[inline(always)]
    fn source_key(&self) -> Key {
        self.key
    }
    #[inline(always)]
    fn source_version(&self) -> &VersionInfo {
        self.version
    }
    #[inline(always)]
    fn source_payload_slot(&self) -> &PayloadSlot<Payload> {
        self.payload
    }
}

impl<Key: Copy + Ord + Hash + Default, Payload: Clone + Default> LeafRecordSource<Key, Payload>
    for RecordPoint<Key, Payload>
{
    #[inline(always)]
    fn source_key(&self) -> Key {
        self.key()
    }
    #[inline(always)]
    fn source_version(&self) -> &VersionInfo {
        self.version()
    }
    #[inline(always)]
    fn source_payload_slot(&self) -> &PayloadSlot<Payload> {
        self.payload_slot()
    }
}

impl<Key: Copy, Payload, R: LeafRecordSource<Key, Payload>> LeafRecordSource<Key, Payload> for &R {
    fn source_key(&self) -> Key {
        (*self).source_key()
    }
    fn source_version(&self) -> &VersionInfo {
        (*self).source_version()
    }
    fn source_payload_slot(&self) -> &PayloadSlot<Payload> {
        (*self).source_payload_slot()
    }
}

pub struct LeafRecords<
    'a,
    const N: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
> {
    page: &'a LeafPage<N, Key, Payload>,
}
impl<'a, const N: usize, Key: Hash + Ord + Copy + Default, Payload: Clone + Default> Copy
    for LeafRecords<'a, N, Key, Payload>
{
}
impl<'a, const N: usize, Key: Hash + Ord + Copy + Default, Payload: Clone + Default> Clone
    for LeafRecords<'a, N, Key, Payload>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, const N: usize, Key, Payload> LeafRecords<'a, N, Key, Payload>
where
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
{
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.page.len()
    }
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<LeafRecordRef<'a, Key, Payload>> {
        (index < self.len()).then(|| self.page.record(index))
    }
    #[inline(always)]
    pub fn iter(&self) -> LeafRecordIter<'a, N, Key, Payload> {
        LeafRecordIter {
            page: self.page,
            front: 0,
            back: self.len(),
        }
    }
}

impl<'a, const N: usize, Key, Payload> IntoIterator for LeafRecords<'a, N, Key, Payload>
where
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
{
    type Item = LeafRecordRef<'a, Key, Payload>;
    type IntoIter = LeafRecordIter<'a, N, Key, Payload>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct LeafRecordIter<
    'a,
    const N: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
> {
    page: &'a LeafPage<N, Key, Payload>,
    front: usize,
    back: usize,
}
impl<'a, const N: usize, Key, Payload> Iterator for LeafRecordIter<'a, N, Key, Payload>
where
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
{
    type Item = LeafRecordRef<'a, Key, Payload>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            None
        } else {
            let i = self.front;
            self.front += 1;
            Some(self.page.record(i))
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.back - self.front;
        (n, Some(n))
    }
}
impl<'a, const N: usize, Key, Payload> DoubleEndedIterator for LeafRecordIter<'a, N, Key, Payload>
where
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            None
        } else {
            self.back -= 1;
            Some(self.page.record(self.back))
        }
    }
}
impl<'a, const N: usize, Key, Payload> ExactSizeIterator for LeafRecordIter<'a, N, Key, Payload>
where
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
{
}

pub struct LeafPage<
    const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
> {
    pub(crate) len: PageLenField,
    key_region: [MaybeUninit<Key>; NUM_RECORDS],
    data_region: [MaybeUninit<LeafData<Payload>>; NUM_RECORDS],
}

impl<const N: usize, Key: Hash + Ord + Copy + Default, Payload: Clone + Default> Default
    for LeafPage<N, Key, Payload>
{
    fn default() -> Self {
        Self::new()
    }
}
impl<const N: usize, Key: Hash + Ord + Copy + Default, Payload: Clone + Default> Clone
    for LeafPage<N, Key, Payload>
{
    fn clone(&self) -> Self {
        Self::from(self)
    }
}
impl<const N: usize, Key: Hash + Ord + Copy + Default, Payload: Clone + Default> Drop
    for LeafPage<N, Key, Payload>
{
    fn drop(&mut self) {
        self.drop_records(self.len())
    }
}

impl<const N: usize, Key: Hash + Ord + Copy + Default, Payload: Clone + Default>
    LeafPage<N, Key, Payload>
{
    pub(crate) fn from(other: &Self) -> Self {
        let mut p = Self::new();
        let records: Vec<_> = other.as_records().iter().collect();
        p.bulk_push(records);
        p
    }
    #[inline]
    pub fn new() -> Self {
        Self {
            len: PageLenField::new(0),
            key_region: unsafe { MaybeUninit::uninit().assume_init() },
            data_region: unsafe { MaybeUninit::uninit().assume_init() },
        }
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        from_len_sum(self.len.load(Acquire))
    }
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.len() == N
    }
    #[inline(always)]
    pub fn active_dead_count(&self) -> (Active, Dead) {
        from_len(self.len.load(Acquire))
    }
    #[inline(always)]
    pub fn keys(&self) -> &[Key] {
        unsafe { std::slice::from_raw_parts(self.key_region.as_ptr() as *const Key, self.len()) }
    }
    #[inline(always)]
    fn data(&self) -> &[LeafData<Payload>] {
        unsafe {
            std::slice::from_raw_parts(
                self.data_region.as_ptr() as *const LeafData<Payload>,
                self.len(),
            )
        }
    }
    #[inline(always)]
    fn data_mut(&mut self) -> &mut [LeafData<Payload>] {
        let n = self.len();
        unsafe {
            std::slice::from_raw_parts_mut(
                self.data_region.as_mut_ptr() as *mut LeafData<Payload>,
                n,
            )
        }
    }
    #[inline(always)]
    pub fn as_records(&self) -> LeafRecords<'_, N, Key, Payload> {
        LeafRecords { page: self }
    }
    #[inline(always)]
    pub fn record(&self, index: usize) -> LeafRecordRef<'_, Key, Payload> {
        let d = &self.data()[index];
        LeafRecordRef {
            key: self.keys()[index],
            version: &d.version,
            payload: &d.payload,
        }
    }
    #[inline(always)]
    pub fn version_at(&self, index: usize) -> &VersionInfo {
        &self.data()[index].version
    }
    #[inline(always)]
    pub fn version_mut_at(&mut self, index: usize) -> &mut VersionInfo {
        &mut self.data_mut()[index].version
    }
    #[inline(always)]
    pub fn payload_at(&self, index: usize) -> &Payload {
        self.data()[index].payload.get()
    }
    #[inline(always)]
    pub fn set_payload_at(&mut self, index: usize, payload: Payload) {
        self.data_mut()[index].payload.set(payload)
    }
    /// Whether the entry at `index` still has a live (non-aborted)
    /// insertion — i.e. not `VersionInfo::invalidate`d. Deletedness is a
    /// separate axis (see `is_live`); this only tracks abort of the insert
    /// itself, matching what `latest_position(_, skip_invalid=true)` and
    /// the same-stamp predecessor searches below need.
    #[inline(always)]
    fn bit_is_valid(&self, index: usize) -> bool {
        !self.version_at(index).insertion_stamp().is_invalid()
    }
    #[inline(always)]
    pub(crate) fn latest_position(&self, key: Key, skip_invalid: bool) -> Option<usize> {
        self.keys()
            .iter()
            .enumerate()
            .rfind(|(i, k)| **k == key && (!skip_invalid || self.bit_is_valid(*i)))
            .map(|(i, _)| i)
    }
    pub fn active_dead_invalid(&self) -> (PageLenPrimitive, Active, Dead) {
        self.data().iter().fold((0, 0, 0), |(a, d, i), r| {
            if !r.version.is_deleted() && !r.version.insertion_stamp().is_invalid() {
                (a + 1, d, i)
            } else if r.version.is_deleted() && !r.version.insertion_stamp().is_invalid() {
                (a, d + 1, i)
            } else {
                (a, d, i + 1)
            }
        })
    }
    pub fn push_uncommitted(&mut self, record: RecordPoint<Key, Payload>, index: usize) {
        assert!(
            index < N,
            "LeafPage::push_uncommitted: index {index} out of bounds for NUM_RECORDS={N}"
        );
        let (key, version, payload) = record.into_parts();
        unsafe {
            self.key_region
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(key));
            self.data_region
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(LeafData { version, payload }));
        }
    }
    #[inline(always)]
    pub fn commit_delta(&self, active_delta: i32, dead_delta: i32) {
        let len = self.len.load(Relaxed);
        let a = active_len(len) as i32 + active_delta;
        let d = dead_len(len) as i32 + dead_delta;
        debug_assert!(a >= 0 && d >= 0);
        self.len
            .store(from_active_dead(a as Active, d as Dead), Release)
    }
    pub fn undo_uncommitted(&mut self, index: usize) {
        unsafe {
            self.data_region
                .as_mut_ptr()
                .add(index)
                .cast::<LeafData<Payload>>()
                .drop_in_place()
        };
    }
    pub fn on_reuse(&mut self) {
        let n = self.len();
        self.len.store(0, Release);
        self.drop_records(n);
    }
    fn drop_records(&mut self, n: usize) {
        unsafe {
            std::ptr::drop_in_place(std::slice::from_raw_parts_mut(
                self.data_region.as_mut_ptr() as *mut LeafData<Payload>,
                n,
            ))
        }
    }
    pub(crate) fn bulk_push<R: LeafRecordSource<Key, Payload>>(&mut self, records: Vec<R>) {
        let count = records.len();
        self.bulk_push_iter(records, count)
    }
    pub(crate) fn bulk_push_from_slice_ref<R: LeafRecordSource<Key, Payload>>(
        &mut self,
        records: &[R],
    ) {
        self.bulk_push_iter(records.iter(), records.len())
    }
    fn bulk_push_iter<R: LeafRecordSource<Key, Payload>, I: IntoIterator<Item = R>>(
        &mut self,
        records: I,
        count: usize,
    ) {
        let len = self.len();
        assert!(len + count <= N);
        let mut active = 0;
        let mut written = 0;
        for (index, r) in records.into_iter().enumerate() {
            written += 1;
            active += usize::from(r.source_version().is_live());
            unsafe {
                self.key_region
                    .as_mut_ptr()
                    .add(len + index)
                    .write(MaybeUninit::new(r.source_key()));
                self.data_region
                    .as_mut_ptr()
                    .add(len + index)
                    .write(MaybeUninit::new(LeafData {
                        version: r.source_version().clone(),
                        payload: r.source_payload_slot().clone(),
                    }));
            }
        }
        assert_eq!(written, count, "bulk source length changed while inserting");
        self.len.store(
            from_active_dead(
                (len + active) as PageLenPrimitive,
                (count - active) as PageLenPrimitive,
            ),
            Release,
        )
    }

    /// Moves materialized records into the page. SMOs already own these
    /// records, so cloning their versions and refcounted payload slots would
    /// only add an avoidable increment/decrement pair.
    pub(crate) fn bulk_push_owned<I>(&mut self, records: I)
    where
        I: IntoIterator<Item = RecordPoint<Key, Payload>>,
        I::IntoIter: ExactSizeIterator,
    {
        let records = records.into_iter();
        let count = records.len();
        let len = self.len();
        assert!(len + count <= N);
        let mut active = 0;
        let mut written = 0;
        for (index, record) in records.enumerate() {
            written += 1;
            let (key, version, payload) = record.into_parts();
            active += usize::from(version.is_live());
            unsafe {
                self.key_region
                    .as_mut_ptr()
                    .add(len + index)
                    .write(MaybeUninit::new(key));
                self.data_region
                    .as_mut_ptr()
                    .add(len + index)
                    .write(MaybeUninit::new(LeafData { version, payload }));
            }
        }
        assert_eq!(written, count, "owned bulk source length was incorrect");
        self.len.store(
            from_active_dead(
                (len + active) as PageLenPrimitive,
                (count - active) as PageLenPrimitive,
            ),
            Release,
        );
    }
    pub(crate) fn delete(&mut self, key: Key, del: TxStamp) -> Result<Option<VersionInfo>, ()> {
        let Some(i) = self.latest_position(key, true) else {
            return Ok(None);
        };
        let v = self.version_mut_at(i);
        if v.delete(del) {
            Ok(Some(v.clone()))
        } else {
            Err(())
        }
    }
    pub(crate) fn delete_after_update(
        &mut self,
        key: Key,
        del: TxStamp,
    ) -> Result<Option<VersionInfo>, ()> {
        let Some(i) = self
            .keys()
            .iter()
            .enumerate()
            .rev()
            .skip(1)
            .find(|(i, k)| **k == key && self.bit_is_valid(*i))
            .map(|(i, _)| i)
        else {
            return Ok(None);
        };
        let v = self.version_mut_at(i);
        if v.delete(del) {
            Ok(Some(v.clone()))
        } else {
            Err(())
        }
    }
    /// A key's versions never straddle a split (`smo::split`'s `KEY_SPLIT`
    /// partitions by key range, and its `VERSION_SPLIT` moves a whole key's
    /// surviving chain together), so an `Update`-abort's linked predecessor
    /// (see `apply_invalidate`'s doc) is always on this same page when one
    /// exists at all.
    pub(crate) fn abort_write(&mut self, key: Key, my_stamp: TxStamp) -> AbortOutcome {
        let Some(i) = self.latest_position(key, true) else {
            return AbortOutcome::NotFound;
        };
        if self.version_at(i).insertion_stamp() == my_stamp {
            self.apply_invalidate(key);
            AbortOutcome::Invalidated
        } else if self.apply_undelete(key) {
            AbortOutcome::Undeleted
        } else {
            AbortOutcome::NotFound
        }
    }
    pub(crate) fn abort_writes(&mut self, key: Key, my_stamp: TxStamp, limit: usize) -> usize {
        let mut n = 0;
        while n < limit && !matches!(self.abort_write(key, my_stamp), AbortOutcome::NotFound) {
            n += 1
        }
        n
    }
    /// Finds an entry for `key` — physically before index `before` if
    /// given (bounds the search to `0..before`), else searches this whole
    /// page — whose own `deletion_stamp` exactly matches `stamp`, and
    /// undeletes it. Used by `apply_invalidate`'s own local, bounded
    /// predecessor search.
    pub(crate) fn undelete_matching_deletion_stamp(
        &mut self,
        key: Key,
        stamp: TxStamp,
        before: Option<usize>,
    ) -> bool {
        let end = before.unwrap_or_else(|| self.len());
        if let Some(j) = (0..end).rev().find(|j| {
            self.keys()[*j] == key
                && self.bit_is_valid(*j)
                && self.version_at(*j).deletion_stamp() == Some(stamp)
        }) {
            self.version_mut_at(j).undelete();
            self.commit_delta(1, -1);
            return true;
        }
        false
    }

    /// Clones the exact predecessor an aborted hot update must resurrect,
    /// but leaves this historical page unchanged. The caller installs the
    /// undeleted clone in the owning hot leaf, preserving the invariant that
    /// all current write-facing state is hot.
    pub(crate) fn clone_undeleted_matching(
        &self,
        key: Key,
        stamp: TxStamp,
    ) -> Option<RecordPoint<Key, Payload>> {
        let i = (0..self.len()).rev().find(|i| {
            self.keys()[*i] == key
                && self.bit_is_valid(*i)
                && self.version_at(*i).deletion_stamp() == Some(stamp)
        })?;
        let mut record =
            RecordPoint::new(key, self.version_at(i).clone(), self.payload_at(i).clone());
        record.version_mut().undelete();
        Some(record)
    }
    /// Invalidates `key`'s latest (valid) entry — the reversal half of an
    /// aborted `Insert`/`Update`'s own insert — and, if it was itself an
    /// `Update`'s insert-half, resurrects the predecessor its own
    /// `delete_after_update` marked deleted (same stamp on both halves).
    /// A bare `Insert`'s abort has no such predecessor, so the search below
    /// legitimately finds nothing in that case.
    pub(crate) fn apply_invalidate(&mut self, key: Key) {
        let Some(i) = self.latest_position(key, true) else {
            return;
        };
        let stamp = self.version_at(i).insertion_stamp();
        let was_live = self.version_at(i).is_live();
        self.version_mut_at(i).invalidate();
        if was_live {
            self.commit_delta(-1, 1)
        }
        self.undelete_matching_deletion_stamp(key, stamp, Some(i));
    }
    pub(crate) fn apply_undelete(&mut self, key: Key) -> bool {
        let Some(i) = self.latest_position(key, true) else {
            return false;
        };
        if self.version_at(i).is_deleted() {
            self.version_mut_at(i).undelete();
            self.commit_delta(1, -1);
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbortOutcome {
    Invalidated,
    Undeleted,
    NotFound,
}
