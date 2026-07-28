use crate::mv_block::block::Block;
use crate::mv_page_model::BlockRef;
use crate::mv_page_model::node::{Active, Dead, PageLenField, PageLenPrimitive, active_len, dead_len, from_active_dead, from_len, from_len_sum};
use crate::mv_page_model::time_matcher::OBSOLETE_VERSION_MARK;
use crate::mv_query::interval::Interval;
use crate::mv_record_model::version_info::{AtomicVersion, Version};
use crate::mv_sync::smart_cell::{OptCell, SmartCell};
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

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
    // `AtomicVersion`, not plain `Version`: `mark_version_obsolete` mutates
    // an *already-published* slot in place (OR-ing in `OBSOLETE_VERSION_MARK`
    // after a split/merge supersedes it) while concurrent OLC readers
    // (`keys_versions`/`versions`/`get_version`) may be reading that exact
    // slot with no lock at all — the same in-place-mutation-of-a-visible-
    // value hazard `VersionInfo`'s own doc describes, confirmed here too via
    // ThreadSanitizer: `InternalPage::mark_version_obsolete`'s plain
    // `ptr.write()` racing a concurrent `RangeQueryIter::next`'s plain read
    // of the same slot, both non-atomic on the same word.
    version_region: [MaybeUninit<AtomicVersion>; FAN_OUT],
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
    _marker: PhantomData<[(Key, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)]>,
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
                        .write(MaybeUninit::new(AtomicVersion::new(version.load(Relaxed))));
                }

                // Fresh page, never-written slot: no concurrent reader of
                // this brand new page exists yet.
                new_page.pointer_region[index] = *pointer;
            });

        let (active, dead)
            = from.active_dead_count();

        new_page.len.store(
            from_active_dead(active, dead), Release);

        new_page
    }

    #[inline(always)]
    pub const fn new() -> Self {
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
            // `MaybeUninit<AtomicVersion>` here is never observed as-is.
            pointer_region: [SmartCell(ptr::null()); FAN_OUT],
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
        debug_assert!(index < FAN_OUT, "InternalPage::push_uncommitted: index {index} out of bounds for FAN_OUT={FAN_OUT}");
        unsafe {
            self.key_interval_region
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(key_interval));

            self.version_region
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(AtomicVersion::new(version)));
        }

        // Fresh slot (never written this "life" of the page): there's no
        // prior value to read/drop, and nothing here owns a refcount to
        // release either way (see `SmartCell`'s doc).
        self.pointer_region[index] = ptr;
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
        debug_assert!(len + add <= FAN_OUT, "InternalPage::bulk_push: {add} entries pushed at len={len} overflow FAN_OUT={FAN_OUT}");

        entries.into_iter()
            .enumerate()
            .for_each(|(index, ((key, version), pointer))| {
                unsafe {
                    (self.key_interval_region
                        .as_ptr() as *mut Interval<Key>)
                        .add(index + len)
                        .write(key.clone());

                    (self.version_region
                        .as_ptr() as *mut AtomicVersion)
                        .add(index + len)
                        .write(AtomicVersion::new(version));
                }

                unsafe {
                    (self.pointer_region
                        .as_ptr() as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                        .add(index + len)
                        .write(*pointer);
                }
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
                        .write(MaybeUninit::new(AtomicVersion::new(*version)));
                }

                unsafe {
                    (self.pointer_region
                        .as_ptr() as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                        .add(index + len)
                        .write(**pointer);
                }
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
    pub fn keys_versions(&self) -> (&[Interval<Key>], &[AtomicVersion]) {
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
    pub fn keys_versions_pointers(&self) -> (&[Interval<Key>], &[AtomicVersion], &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>]) {
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
    pub fn versions(&self) -> &[AtomicVersion] {
        unsafe { std::slice::from_raw_parts(self.version_region.as_ptr() as _, self.sum_len()) }
    }

    #[inline(always)]
    pub fn get_version(&self, index: usize) -> Version {
        unsafe { (*(self.version_region.as_ptr().add(index) as *const AtomicVersion)).load(Acquire) }
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

    /// `&self`, not `&mut self`: the caller always already holds this page's
    /// own exclusive write lock (so no lost-update risk from the plain
    /// load-then-store below), but the store itself must stay atomic so a
    /// concurrent *reader* (OLC traversal takes no lock at all) can't
    /// observe a torn value — see `version_region`'s own doc.
    ///
    /// `Release`, paired with every lock-free reader's `Acquire` load of this
    /// same slot (`keys_versions`/`versions`/`get_version` and every
    /// `.load(Acquire)` call site over their result): a bare `Relaxed`
    /// fetch_or here (as this used to be) gave no happens-before edge at
    /// all between this mark and a concurrent reader's own observation of
    /// it, so a reader could see this child as still-active for an
    /// unbounded time after this call returned. `register_dead` (which
    /// makes the superseded block eligible for GC reclaim once every *live*
    /// snapshot has moved past its death version — see that fn's doc) is
    /// always called strictly after this in program order on this same
    /// thread; without this `Release`, that ordering was invisible to other
    /// threads, letting a reader that should have observed "obsolete" here
    /// instead descend into a block GC had already handed out for reuse.
    /// Confirmed as the mechanism behind a real crash: a reader whose own
    /// registered snapshot was *newer* than the block's death version (so,
    /// by MVCC visibility rules, it never legitimately needed this block at
    /// all) still ended up dereferencing it, because the staleness here is
    /// about memory visibility, not snapshot age.
    #[inline(always)]
    pub fn mark_version_obsolete(&self, index: usize) {
        unsafe {
            let slot
                = &*(self.version_region.as_ptr().add(index) as *const AtomicVersion);

            slot.fetch_or(OBSOLETE_VERSION_MARK, Release);
        }
    }
}
