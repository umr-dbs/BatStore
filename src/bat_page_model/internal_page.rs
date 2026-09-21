use crate::bat_block::block::Block;
use crate::bat_page_model::BlockRef;
use crate::bat_page_model::node::{
    Active, Dead, PageLenField, PageLenPrimitive, active_len, dead_len, from_active_dead, from_len,
    from_len_sum,
};
use crate::bat_query::interval::Interval;
use crate::bat_record_model::version_info::Version;
use crate::bat_sync::smart_cell::{OptCell, SmartCell};
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

const INTERNAL_LIVE_MASK_WORDS: usize = 2;

pub type Fence<Key> = Interval<Key>;

type ChildBase<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload> =
    OptCell<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>;

pub struct InternalPage<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> {
    pub(crate) len: PageLenField,
    key_interval_region: [MaybeUninit<Interval<Key>>; FAN_OUT],
    version_region: [MaybeUninit<Version>; FAN_OUT],
    pointer_region: [BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>; FAN_OUT],
    live_mask_region: [AtomicU64; INTERNAL_LIVE_MASK_WORDS],
    _marker: PhantomData<[(Key, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)]>,
}

#[cfg(test)]
mod live_mask_tests {
    use super::*;

    type Page = InternalPage<123, 123, u64, u64>;

    #[test]
    fn appended_split_intervals_supersede_the_old_child() {
        let mut page = Page::new();
        let null = SmartCell(ptr::null());

        page.push_uncommitted(Interval::new(0, 100), Version::default(), null, 0);
        page.commit_delta(1, 0);
        assert!(page.is_slot_live(0));

        page.push_uncommitted(Interval::new(0, 49), Version::default(), null, 1);
        page.push_uncommitted(Interval::new(50, 100), Version::default(), null, 2);
        page.commit_delta(1, 1);

        assert!(!page.is_slot_live(0));
        assert!(page.is_slot_live(1));
        assert!(page.is_slot_live(2));
        assert_eq!(page.live_count(), 2);
    }

    #[test]
    fn clone_preserves_and_reuse_clears_current_liveness() {
        let mut page = Page::new();
        let null = SmartCell(ptr::null());
        page.push_uncommitted(Interval::new(0, 100), Version::default(), null, 0);
        page.commit_delta(1, 0);
        page.push_uncommitted(Interval::new(0, 100), Version::default(), null, 1);
        page.commit_delta(0, 1);

        let cloned = page.clone();
        assert!(!cloned.is_slot_live(0));
        assert!(cloned.is_slot_live(1));
        assert_eq!(cloned.live_count(), 1);

        page.on_reuse();
        assert_eq!(page.sum_len(), 0);
        assert_eq!(page.live_count(), 0);
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> Clone for InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn clone(&self) -> Self {
        Self::from(self)
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    pub fn from(from: &Self) -> Self {
        let mut new_page = Self::new();

        let (keys, versions, pointers) = from.keys_versions_pointers();

        keys.iter()
            .zip(versions.iter())
            .zip(pointers.into_iter())
            .enumerate()
            .for_each(|(index, ((key, version), pointer))| {
                unsafe {
                    new_page
                        .key_interval_region
                        .as_mut_ptr()
                        .add(index)
                        .write(MaybeUninit::new(key.clone()));

                    new_page
                        .version_region
                        .as_mut_ptr()
                        .add(index)
                        .write(MaybeUninit::new(*version));
                }

                // Fresh page, never-written slot: no concurrent reader of
                // this brand new page exists yet.
                new_page.pointer_region[index] = *pointer;
            });

        let (active, dead) = from.active_dead_count();

        for (dst, src) in new_page.live_mask_region.iter().zip(&from.live_mask_region) {
            dst.store(src.load(Acquire), Relaxed);
        }

        new_page.len.store(from_active_dead(active, dead), Release);

        new_page
    }

    #[inline(always)]
    pub const fn new() -> Self {
        assert!(
            FAN_OUT <= INTERNAL_LIVE_MASK_WORDS * u64::BITS as usize,
            "InternalPage live mask supports at most 128 slots"
        );
        InternalPage {
            len: PageLenField::new(0),
            key_interval_region: unsafe { MaybeUninit::uninit().assume_init() },
            version_region: unsafe { MaybeUninit::uninit().assume_init() },
            pointer_region: [SmartCell(ptr::null()); FAN_OUT],
            live_mask_region: [const { AtomicU64::new(0) }; INTERNAL_LIVE_MASK_WORDS],
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn push_uncommitted(
        &mut self,
        key_interval: Interval<Key>,
        version: Version,
        ptr: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        index: usize,
    ) {
        assert!(
            index < FAN_OUT,
            "InternalPage::push_uncommitted: index {index} out of bounds for FAN_OUT={FAN_OUT}"
        );
        unsafe {
            self.key_interval_region
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(key_interval));

            self.version_region
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(version));
        }

        // Fresh slot (never written this "life" of the page): there's no
        // prior value to read/drop, and nothing here owns a refcount to
        // release either way (see `SmartCell`'s doc).
        self.pointer_region[index] = ptr;
        self.publish_live_interval(index, &key_interval);
    }

    #[inline(always)]
    pub fn commit_delta(&self, active_delta: i32, dead_delta: u32) {
        let len = self.len.load(Relaxed);
        let active = active_len(len) as i32 + active_delta;
        let dead = dead_len(len) + dead_delta;

        debug_assert!(
            active >= 0,
            "InternalPage active count went negative: len={len}, active_delta={active_delta}"
        );
        self.len
            .store(from_active_dead(active as Active, dead as Dead), Release)
    }

    #[inline]
    pub fn on_reuse(&mut self) {
        self.clear_live_mask();
        self.len.store(0, Release);
    }

    #[inline]
    pub fn force_reinit_pointer_region(&mut self) {
        self.pointer_region
            .iter_mut()
            .for_each(|slot| *slot = SmartCell(ptr::null()));
    }

    #[inline]
    pub fn bulk_push(
        &self,
        entries: Vec<(
            (&Interval<Key>, Version),
            &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        )>,
    ) {
        let len = self.active_len();

        debug_assert_eq!(self.dead_len(), 0);
        let add = entries.len();

        assert!(
            len + add <= FAN_OUT,
            "InternalPage::bulk_push: {add} entries pushed at len={len} overflow FAN_OUT={FAN_OUT}"
        );

        entries
            .into_iter()
            .enumerate()
            .for_each(|(index, ((key, version), pointer))| {
                unsafe {
                    (self.key_interval_region.as_ptr() as *mut Interval<Key>)
                        .add(index + len)
                        .write(key.clone());

                    (self.version_region.as_ptr() as *mut Version)
                        .add(index + len)
                        .write(version);
                }

                unsafe {
                    (self.pointer_region.as_ptr()
                        as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                        .add(index + len)
                        .write(*pointer);
                }

                self.publish_live_interval(index + len, key);
            });

        self.len.store(
            from_active_dead(len as PageLenPrimitive + add as PageLenPrimitive, 0),
            Release,
        );
    }

    #[inline]
    pub fn bulk_push_from_slice(
        &mut self,
        entries: &[(
            (&Interval<Key>, Version),
            &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        )],
    ) {
        let len = self.active_len();

        debug_assert_eq!(self.dead_len(), 0);
        let add = entries.len();

        // See `push_uncommitted`'s matching assert.
        assert!(
            len + add <= FAN_OUT,
            "InternalPage::bulk_push_from_slice: {add} entries pushed at len={len} overflow FAN_OUT={FAN_OUT}"
        );

        entries
            .into_iter()
            .enumerate()
            .for_each(|(index, ((key, version), pointer))| {
                unsafe {
                    self.key_interval_region
                        .as_mut_ptr()
                        .add(index + len)
                        .write(MaybeUninit::new((*key).clone()));

                    self.version_region
                        .as_mut_ptr()
                        .add(index + len)
                        .write(MaybeUninit::new(*version));
                }

                unsafe {
                    (self.pointer_region.as_ptr()
                        as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                        .add(index + len)
                        .write(**pointer);
                }

                self.publish_live_interval(index + len, key);
            });

        self.len.store(
            from_active_dead(len as PageLenPrimitive + add as PageLenPrimitive, 0),
            Release,
        )
    }

    #[inline(always)]
    pub fn active_dead_count(&self) -> (Active, Dead) {
        from_len(self.len.load(Acquire))
    }

    #[inline(always)]
    pub fn active_len(&self) -> usize {
        let len = self.len.load(Acquire);

        active_len(len) as _
    }

    #[inline(always)]
    pub fn dead_len(&self) -> usize {
        let len = self.len.load(Acquire);

        dead_len(len) as _
    }

    #[inline(always)]
    pub fn sum_len(&self) -> usize {
        let len = self.len.load(Acquire) as _;

        from_len_sum(len)
    }

    #[inline(always)]
    pub fn keys_versions(&self) -> (&[Interval<Key>], &[Version]) {
        let len = self.sum_len();

        unsafe {
            (
                std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, len),
                std::slice::from_raw_parts(self.version_region.as_ptr() as _, len),
            )
        }
    }

    #[inline(always)]
    pub fn last_child(&self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.get_pointer(self.sum_len() - 1)
    }

    #[inline(always)]
    pub fn keys_versions_pointers(
        &self,
    ) -> (
        &[Interval<Key>],
        &[Version],
        &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>],
    ) {
        let len = self.sum_len();

        unsafe {
            (
                std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, len),
                std::slice::from_raw_parts(self.version_region.as_ptr() as _, len),
                std::slice::from_raw_parts(self.pointer_region.as_ptr(), len),
            )
        }
    }

    #[inline(always)]
    pub fn keys(&self) -> &[Interval<Key>] {
        unsafe {
            std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, self.sum_len())
        }
    }

    #[inline(always)]
    pub fn get_key(&self, index: usize) -> &Interval<Key> {
        unsafe { &*(self.key_interval_region.as_ptr().add(index) as *const Interval<Key>) }
    }

    #[inline(always)]
    pub fn versions(&self) -> &[Version] {
        unsafe { std::slice::from_raw_parts(self.version_region.as_ptr() as _, self.sum_len()) }
    }

    #[inline(always)]
    pub fn get_version(&self, index: usize) -> Version {
        unsafe { *(self.version_region.as_ptr().add(index) as *const Version) }
    }

    #[inline(always)]
    pub fn is_slot_live(&self, index: usize) -> bool {
        debug_assert!(index < self.sum_len());
        let word = index / u64::BITS as usize;
        let bit = index % u64::BITS as usize;
        self.live_mask_region[word].load(Acquire) & (1u64 << bit) != 0
    }

    #[inline]
    pub fn live_count(&self) -> usize {
        let len = self.sum_len();
        self.live_mask_region
            .iter()
            .enumerate()
            .map(|(word_index, word)| {
                let remaining = len.saturating_sub(word_index * u64::BITS as usize);
                let valid_bits = remaining.min(u64::BITS as usize);
                let tail_mask = if valid_bits == u64::BITS as usize {
                    u64::MAX
                } else if valid_bits == 0 {
                    0
                } else {
                    (1u64 << valid_bits) - 1
                };
                (word.load(Acquire) & tail_mask).count_ones() as usize
            })
            .sum()
    }

    #[inline]
    fn publish_live_interval(&self, index: usize, interval: &Interval<Key>) {
        debug_assert!(index < FAN_OUT);

        for older in 0..index {
            if self.is_slot_live_unchecked(older) && self.get_key(older).overlap(interval) {
                let word = older / u64::BITS as usize;
                let bit = older % u64::BITS as usize;
                self.live_mask_region[word].fetch_and(!(1u64 << bit), Relaxed);
            }
        }

        let word = index / u64::BITS as usize;
        let bit = index % u64::BITS as usize;
        self.live_mask_region[word].fetch_or(1u64 << bit, Relaxed);
    }

    #[inline(always)]
    fn is_slot_live_unchecked(&self, index: usize) -> bool {
        let word = index / u64::BITS as usize;
        let bit = index % u64::BITS as usize;
        self.live_mask_region[word].load(Relaxed) & (1u64 << bit) != 0
    }

    #[inline(always)]
    fn clear_live_mask(&self) {
        for word in &self.live_mask_region {
            word.store(0, Relaxed);
        }
    }
    #[inline(always)]
    pub fn children(&self) -> &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>] {
        let len = self.sum_len();

        unsafe { std::slice::from_raw_parts(self.pointer_region.as_ptr(), len) }
    }

    #[inline(always)]
    pub fn get_pointer(&self, index: usize) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe { *self.pointer_region.get_unchecked(index) }
    }
}
