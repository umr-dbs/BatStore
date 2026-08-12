use crate::mv_block::block::Block;
use crate::mv_page_model::BlockRef;
use crate::mv_page_model::node::{Active, Dead, PageLenField, PageLenPrimitive, active_len, dead_len, from_active_dead, from_len, from_len_sum};
use crate::mv_query::interval::Interval;
use crate::mv_record_model::version_info::Version;
use crate::mv_sync::smart_cell::{OptCell, SmartCell};
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::AtomicU64;

const INTERNAL_LIVE_MASK_WORDS: usize = 2;

pub type Fence<Key> = Interval<Key>;

type ChildBase<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload> =
    OptCell<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>;

pub struct InternalPage<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    pub(crate) len: PageLenField,
    key_interval_region: [MaybeUninit<Interval<Key>>; FAN_OUT],
    // Plain `Version`, not `AtomicVersion`: a slot is written exactly once,
    // by `push_uncommitted`/`bulk_push*`, strictly before the `len` bump
    // that publishes it (see this struct's own doc on `pointer_region`) —
    // nothing ever mutates it again afterward. There used to be a second
    // writer (`mark_version_obsolete`, OR-ing an "obsolete" bit into an
    // already-published slot for a concurrent OLC reader to observe) which
    // needed `AtomicVersion` to avoid racing that in-place mutation against
    // a lock-free reader; it is gone now. Current liveness is maintained in
    // a separate page-level mask, so `Version` itself remains immutable and
    // rides the exact same `len`-Acquire/Release happens-before edge
    // `pointer_region` already relies on, no atomicity needed on the slot
    // itself.
    version_region: [MaybeUninit<Version>; FAN_OUT],
    // A slot holds either `null` (never written this "life" of the page) or
    // a genuinely raw pointer to a `Block`'s `OptCell`, wrapped in the same
    // `BlockRef`/`SmartCell` every other alias of that block uses — not an
    // `Arc` reconstructed via `into_raw`/`from_raw`, and not owning
    // anything: `get_pointer` just copies it out, no refcount bump. See
    // `SmartCell`'s own doc for why that's sound (nothing here is ever
    // freed while the tree is live) and what trade-off that rests on.
    // Storing `BlockRef` directly (not a bare raw pointer) means the whole
    // array is `Copy`/unconditionally `Send + Sync` the same way
    // `SmartCell` itself is, and a slice of it can be handed out directly —
    // no per-element unwrap/rewrap needed the way a bare `*const` would.
    // Plain reads/writes, same as `key_interval_region`/`version_region`:
    // every caller reads `sum_len()` (an `Acquire` load) before ever
    // indexing into this array, and that already establishes happens-before
    // for everything a writer stored (via `Release`) before its own `len`
    // bump — no per-slot atomicity needed on top of that.
    pointer_region: [BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>; FAN_OUT],
    // Current-version liveness for SMOs. Production FAN_OUT is 123, so two
    // words cover every slot and consume the internal page's existing 16
    // bytes of union padding: the enclosing 4 KiB block does not grow.
    // Historical traversal deliberately ignores this current-state cache
    // and continues to select entries through `version_region`.
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

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Clone for InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn clone(&self) -> Self {
        Self::from(self)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline]
    pub fn from(from: &Self) -> Self {
        let mut new_page
            = Self::new();

        let (keys, versions, pointers)
            = from.keys_versions_pointers();

        keys.iter()
            .zip(versions.iter())
            .zip(pointers.into_iter())
            .enumerate()
            .for_each(|(index, ((key, version), pointer))| {
                unsafe {
                    new_page.key_interval_region
                        .as_mut_ptr()
                        .add(index)
                        .write(MaybeUninit::new(key.clone()));

                    new_page.version_region
                        .as_mut_ptr()
                        .add(index)
                        .write(MaybeUninit::new(*version));
                }

                // Fresh page, never-written slot: no concurrent reader of
                // this brand new page exists yet.
                new_page.pointer_region[index] = *pointer;
            });

        let (active, dead)
            = from.active_dead_count();

        for (dst, src) in new_page.live_mask_region.iter().zip(&from.live_mask_region) {
            dst.store(src.load(Acquire), Relaxed);
        }

        new_page.len.store(
            from_active_dead(active, dead), Release);

        new_page
    }

    #[inline(always)]
    pub const fn new() -> Self {
        assert!(FAN_OUT <= INTERNAL_LIVE_MASK_WORDS * u64::BITS as usize,
                "InternalPage live mask supports at most 128 slots");
        // debug_assert!(mem::size_of::<[Interval<Key>; FAN_OUT]>() +
        //                   mem::size_of::<[Version; FAN_OUT]>() +
        //                   mem::size_of::<[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>; FAN_OUT]>() +
        //                   mem::size_of::<Len>()
        //                   <= 4096, "FAN_OUT Invalid!"
        // );
        InternalPage {
            len: PageLenField::new(0),
            key_interval_region: unsafe { MaybeUninit::uninit().assume_init() },
            version_region: unsafe { MaybeUninit::uninit().assume_init() },
            // Each slot is only ever read after `push_uncommitted`/
            // `bulk_push*` wrote it (gated by `sum_len()`'s `Acquire` load —
            // see this struct's own doc), so a genuinely uninitialized
            // `MaybeUninit<Version>` here is never observed as-is.
            pointer_region: [SmartCell(ptr::null()); FAN_OUT],
            live_mask_region: [const { AtomicU64::new(0) }; INTERNAL_LIVE_MASK_WORDS],
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn push_uncommitted(&mut self, key_interval: Interval<Key>, version: Version, ptr: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, index: usize) {
        // A real `assert!`, not `debug_assert!` (this profile has
        // `debug-assertions = false`, so that never actually ran): without
        // it, `index == FAN_OUT` writes past the end of
        // `key_interval_region`/`version_region` via a raw, unchecked
        // pointer store, landing in whatever's next in the struct's layout
        // — in practice, `pointer_region[0]`, corrupting a real pointer with
        // a stray `Version`/`Key` value (confirmed: caught a slot holding
        // `0x16b5`, not a plausible heap address). That's a silent
        // memory-corruption bug wearing a SIGSEGV-shaped costume; this
        // turns it into the same loud, clean panic `pointer_region`'s own
        // bounds-checked indexing already gives for the same out-of-bounds
        // condition.
        assert!(index < FAN_OUT, "InternalPage::push_uncommitted: index {index} out of bounds for FAN_OUT={FAN_OUT}");
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
        let len= self.len.load(Relaxed);
        let active = active_len(len) as i32 + active_delta;
        let dead = dead_len(len) + dead_delta;

        debug_assert!(active >= 0,
                      "InternalPage active count went negative: len={len}, active_delta={active_delta}");
        self.len.store(from_active_dead(active as Active, dead as Dead), Release)
    }

    #[inline]
    pub fn on_reuse(&mut self) {
        // Just resets the length — no per-slot release loop anymore.
        // `pointer_region`'s old entries own nothing (see `SmartCell`'s
        // doc), so there's nothing to drop; they're simply unreachable
        // (`sum_len() == 0` means no reader/accessor ever iterates to them)
        // until the next round of `push_uncommitted`/`bulk_push*` overwrites
        // them with fresh content.
        self.clear_live_mask();
        self.len.store(0, Release);
    }

    /// Unconditionally re-initializes every slot to `null` via a raw write
    /// that never reads whatever was previously there. Needed because
    /// `Node`'s `page` field is a `union` (`InnerPage`): when a block that
    /// was previously a *leaf* gets reused as an *internal* page
    /// (`Node::on_reuse` dispatches to `LeafPage::on_reuse`, which knows
    /// nothing about `pointer_region`), this array's bytes are still
    /// whatever `LeafPage`'s record data left behind — not a legitimate
    /// `AtomicPtr`. Treating it as one (reading or swapping into it as-is)
    /// would interpret garbage bytes as a raw pointer, which `get_pointer`'s
    /// caller could then dereference. Safe to call unconditionally even
    /// when the block *was* already internal: `on_reuse` above has already
    /// made every slot unreachable (`sum_len() == 0`) by then, so
    /// overwriting them again without reading them first loses nothing.
    ///
    /// Was drafted but never actually wired into `Node::on_reuse` — GC
    /// stayed off by default long enough that a leaf/internal reuse cycle
    /// (the only way to hit this) was rare in practice. Confirmed via gdb on
    /// a GC-enabled heavy-concurrency repro: `get_pointer` returning a
    /// straight-up null `BlockRef` (0x0), later dereferenced by
    /// `borrow_read` inside `traversal_write_internal_olc` — a leftover
    /// `RecordPoint` field (small inline payload, unset key, ...) from this
    /// exact block's previous life as a leaf, reinterpreted as a pointer.
    #[inline]
    pub fn force_reinit_pointer_region(&mut self) {
        self.pointer_region
            .iter_mut()
            .for_each(|slot| *slot = SmartCell(ptr::null()));
    }

    #[inline]
    pub fn bulk_push(&self, entries: Vec<((&Interval<Key>, Version), &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)>) {
        let len
            = self.active_len();

        debug_assert_eq!(self.dead_len(), 0);
        let add
            = entries.len();

        // See `push_uncommitted`'s matching assert: without this, an
        // overflowing bulk-push writes `key_interval_region`/
        // `version_region` past `FAN_OUT` via a raw, unchecked store,
        // silently corrupting `pointer_region`'s adjacent bytes instead of
        // failing where the actual out-of-bounds condition is.
        assert!(len + add <= FAN_OUT, "InternalPage::bulk_push: {add} entries pushed at len={len} overflow FAN_OUT={FAN_OUT}");

        entries.into_iter()
            .enumerate()
            .for_each(|(index, ((key, version), pointer))| {
                unsafe {
                    (self.key_interval_region
                        .as_ptr() as *mut Interval<Key>)
                        .add(index + len)
                        .write(key.clone());

                    (self.version_region
                        .as_ptr() as *mut Version)
                        .add(index + len)
                        .write(version);
                }

                unsafe {
                    (self.pointer_region
                        .as_ptr() as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                        .add(index + len)
                        .write(*pointer);
                }

                self.publish_live_interval(index + len, key);
            });

        self.len.store(
            from_active_dead(len as PageLenPrimitive + add as PageLenPrimitive, 0), Release);
    }

    #[inline]
    pub fn bulk_push_from_slice(
        &mut self,
        entries: &[((&Interval<Key>, Version), &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)])
    {
        let len
            = self.active_len();

        debug_assert_eq!(self.dead_len(), 0);
        let add
            = entries.len();

        // See `push_uncommitted`'s matching assert.
        assert!(len + add <= FAN_OUT, "InternalPage::bulk_push_from_slice: {add} entries pushed at len={len} overflow FAN_OUT={FAN_OUT}");

        entries.into_iter()
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
                    (self.pointer_region
                        .as_ptr() as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                        .add(index + len)
                        .write(**pointer);
                }

                self.publish_live_interval(index + len, key);
            });

        self.len.store(
            from_active_dead(len as PageLenPrimitive + add as PageLenPrimitive, 0), Release)
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
        let len
            = self.sum_len();

        unsafe {
            (std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.version_region.as_ptr() as _, len))
        }
    }

    #[inline(always)]
    pub fn last_child(&self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.get_pointer(self.sum_len() - 1)
    }

    #[inline(always)]
    pub fn keys_versions_pointers(&self) -> (&[Interval<Key>], &[Version], &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>]) {
        let len
            = self.sum_len();

        unsafe {
            (std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.version_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.pointer_region.as_ptr(), len))
        }
    }

    #[inline(always)]
    pub fn keys(&self) -> &[Interval<Key>] {
        unsafe { std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, self.sum_len()) }
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

    /// Whether a slot belongs to the page's current child partition.
    ///
    /// Live sibling entries within one page are always pairwise disjoint
    /// (children partition their parent's key range), and a slot, once
    /// superseded, stays superseded forever (append-only, no
    /// "un-obsoleting"). So slot `i` is dead **iff** some later slot `j > i`
    /// overlaps it at all: two entries that were *simultaneously* live could
    /// never overlap, so a later overlapping entry can only be a descendant
    /// that (directly, or via its own further descendants) superseded `i`.
    /// This holds even for a split, where *no single* later child covers the
    /// old entry's full interval on its own (e.g. `i=[0,10)` replaced by
    /// `left=[0,5)`/`right=[5,10)`) — each still individually overlaps `i`,
    /// which is exactly what plain `Interval::overlap` (not `covers`) is
    /// for, so no separate interval-subtraction bookkeeping is needed.
    ///
    /// The two-word mask replaces the former allocating O(FAN_OUT^2)
    /// reconstruction on every SMO. It is not an MVCC visibility oracle:
    /// snapshot readers must keep using versions because an older slot can
    /// be dead *now* and still be the correct child for an older snapshot.
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

        // Append order defines supersession: the new interval kills every
        // older overlapping current entry. The page's writer/SMO lock
        // serializes these updates; atomics only make concurrent diagnostic
        // observation data-race-free. The following Release `len` store
        // publishes both slot data and these Relaxed mask changes together.
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
        let len
            = self.sum_len();

        unsafe { std::slice::from_raw_parts(self.pointer_region.as_ptr(), len) }
    }

    #[inline(always)]
    pub fn get_pointer(&self, index: usize) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe { *self.pointer_region.get_unchecked(index) }
        // let cell = self.pointer_region[index];
        // let raw = cell.0;
        //
        // A real `assert!`, not `debug_assert!`: a null slot here means the
        // caller is about to dereference a null `SmartCell` — an instant,
        // silent SIGSEGV, in release builds too (this profile has
        // `debug-assertions = false`). Fail loudly instead of trading a
        // diagnosable panic for a crash with no message.
        // assert!(!raw.is_null(), "InternalPage::get_pointer: slot {index} must be populated");
        // assert!(
        //     (raw as usize) >= 0x1000 && (raw as usize) % std::mem::align_of::<ChildBase<FAN_OUT, NUM_RECORDS, Key, Payload>>() == 0,
        //     "InternalPage::get_pointer: slot {index} holds a garbage pointer {raw:#x?} (sum_len={}, self={:p})",
        //     self.sum_len(), self
        // );
        //
        // cell
    }

}
