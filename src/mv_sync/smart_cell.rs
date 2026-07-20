use std::fmt::{Display, Formatter};
use std::{hint, mem, ptr};
use std::mem::transmute_copy;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use CCBPlusTree::locking::locking_strategy::LockingStrategy;
use crate::mv_page_model::Attempts;
use crate::mv_page_model::time_matcher::OBSOLETE_VERSION_MARK;
use crate::mv_record_model::AtomicVersion;
use crate::mv_record_model::version_info::Version;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::SmartGuard::{Reader, Writer};

const WRITE_FLAG_VERSION: LatchVersion = 0x4_000000000000000;

const WRITE_PIN_FLAG_VERSION: LatchVersion = 0x6_000000000000000;
const WRITE_PIN_OBSOLETE_FLAG_VERSION: LatchVersion = 0xE_000000000000000;

#[cfg(all(feature = "hardware-lock-elision", any(target_arch = "x86", target_arch = "x86_64")))]
pub trait AtomicElisionExt {
    fn elision_compare_exchange_acquire(
        &self,
        current: Version,
        new: Version,
    ) -> Result<Version, Version>;
}

#[cfg(all(feature = "hardware-lock-elision", any(target_arch = "x86", target_arch = "x86_64")))]
impl AtomicElisionExt for AtomicVersion {
    #[inline(always)]
    fn elision_compare_exchange_acquire(&self, current: Version, new: Version) -> Result<Version, Version> {
        unsafe {
            use core::arch::asm;
            let prev: Version;
            #[cfg(target_pointer_width = "32")]
            asm!(
            "xacquire",
            "lock",
            "cmpxchg [{:e}], {:e}",
            in(reg) self,
            in(reg) new,
            inout("eax") current => prev,
            );
            #[cfg(target_pointer_width = "64")]
            asm!(
            "xacquire",
            "lock",
            "cmpxchg [{}], {}",
            in(reg) self,
            in(reg) new,
            inout("rax") current => prev,
            );
            if prev == current {
                Ok(prev)
            } else {
                Err(prev)
            }
        }
    }
}

// pub static mut COUNTERS: (AtomicUsize, AtomicUsize) =
//     (AtomicUsize::new(0), AtomicUsize::new(0));

/// Above this many attempts, back off with randomized jitter instead of a
/// plain, deterministic yield/spin — two threads each holding one half of a
/// pair of locks the other needs (e.g. one thread's `simba` is another
/// thread's merge `candidate` and vice versa) can otherwise retry in
/// lockstep forever: both back off by the same amount, both re-acquire
/// their own side at the same cadence, neither ever lines up with the
/// other's release. Randomizing the delay desynchronizes them, the same
/// idea as Ethernet/TCP backoff jitter. Left low enough that ordinary,
/// quickly-resolving contention (the overwhelming majority of retries)
/// never reaches it and keeps spinning/yielding exactly as before.
const JITTER_BACKOFF_THRESHOLD: Attempts = 1000;

#[inline(always)]
#[cfg(target_os = "linux")]
pub fn sched_yield(attempt: Attempts) {
    if attempt > JITTER_BACKOFF_THRESHOLD {
        let jitter_micros = rand::random_range(1..=attempt.min(5_000));
        std::thread::sleep(std::time::Duration::from_micros(jitter_micros as u64));
    } else if attempt > 3 {
        unsafe {
            // COUNTERS.1.fetch_add(1, Relaxed);
            libc::sched_yield();
        }
    } else {
        // unsafe { COUNTERS.0.fetch_add(1, Relaxed); }
        hint::spin_loop();
    }
}

pub const FORCE_YIELD: Attempts = 4;

#[inline(always)]
#[cfg(not(target_os = "linux"))]
pub fn sched_yield(attempt: Attempts) {
    if attempt > JITTER_BACKOFF_THRESHOLD {
        let jitter_micros = rand::random_range(1..=attempt.min(5_000));
        std::thread::sleep(std::time::Duration::from_micros(jitter_micros as u64));
    } else if attempt > 3 {
        std::thread::yield_now();
    } else {
        hint::spin_loop();
    }
}

type LatchVersion = Version;
type IsRead = bool;

pub struct OptCell<E: Default> {
    pub cell: SafeCell<E>,
    pub cell_version: AtomicVersion,
}

impl<E: Default + Display> Display for OptCell<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OptCell {{\ncell: {}\n\t\tcell_version: {}\n\t}}", self.cell.get_mut(), self.cell_version.load(Relaxed))
    }
}

impl<E: Default> Default for OptCell<E> {
    fn default() -> Self {
        Self::new(E::default())
    }
}

impl<E: Default> OptCell<E> {
    const CELL_START_VERSION: LatchVersion = 0;

    #[inline(always)]
    pub const fn new(data: E) -> Self {
        Self {
            cell: SafeCell::new(data),
            cell_version: AtomicVersion::new(Self::CELL_START_VERSION),
        }
    }

    #[cfg(not(all(feature = "hardware-lock-elision", any(
        target_arch = "x86",
        target_arch = "x86_64"
    ))))]
    #[inline(always)]
    pub fn write_lock(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        match self.cell_version.compare_exchange_weak(
            read_version,
            WRITE_FLAG_VERSION | read_version,
            AcqRel,
            Acquire)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => None
        }
    }

    #[cfg(all(feature = "hardware-lock-elision", any(target_arch = "x86", target_arch = "x86_64")))]
    #[inline(always)]
    pub fn write_lock(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        match self.cell_version.elision_compare_exchange_acquire(
            read_version,
            WRITE_FLAG_VERSION | read_version)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => None
        }
    }
}

// A genuinely raw, non-owning pointer — not an `Arc` reconstructed via
// `into_raw`/`from_raw`. Cloning is a plain pointer copy (no atomic refcount
// bump at all, unlike the previous `triomphe::Arc`-backed design), and
// nothing here ever frees the pointee: a block, once allocated
// (`Block::into_cell`, which now leaks via `Box::into_raw`), lives for the
// rest of the process. That's sound today specifically because block
// reclaim (GC) is off by default and — per the same reasoning that already
// justified skipping refcounting when GC is disabled — nothing currently
// calls `Box::from_raw` on a block's `OptCell` to actually free it either.
// Enabling GC's block-reclaim path with this design reuses the *same*
// allocation in place (`on_reuse`, still valid — reuse never frees or moves
// the memory), but any child a reused page used to point to that isn't
// independently registered dead via `register_dead`/`register_dead_col` is
// no longer released on that page's reuse (there is no refcount left to
// drop it via) — it simply leaks instead of being freed, trading a
// use-after-free hazard for a memory-growth one.
// `Copy`/`Clone`/`Default` are hand-written, not derived: `derive` would add
// a spurious `E: Copy`/`E: Clone`/`E: Default` bound on the impl (a
// well-known derive-macro limitation for generic structs), even though
// copying/cloning/defaulting a raw pointer never needs `E` to be anything in
// particular. `Default` returns a null pointer — never itself dereferenced,
// only ever overwritten before any real use (the same "unpopulated slot"
// invariant `InternalPage::pointer_region` already relies on) — needed
// because `Root`/`RootTree` derive `Default` over a field of this type.
pub struct SmartCell<E: Default>(pub *const OptCell<E>);

impl<E: Default> Copy for SmartCell<E> {}

impl<E: Default> Clone for SmartCell<E> {
    #[inline(always)]
    fn clone(&self) -> Self {
        SmartCell(self.0)
    }
}

impl<E: Default> Default for SmartCell<E> {
    #[inline(always)]
    fn default() -> Self {
        SmartCell(ptr::null())
    }
}

unsafe impl<E: Default> Send for SmartCell<E> {}
unsafe impl<E: Default> Sync for SmartCell<E> {}

pub enum SmartGuard<E: Default> {
    Reader(SmartCell<E>, LatchVersion),
    Writer(SmartCell<E>, LatchVersion),
}

impl<E: Default + 'static> Clone for SmartGuard<E> {
    fn clone(&self) -> Self {
        match self {
            Reader(cell, latch) => Reader(cell.clone(), *latch),
            _ => unreachable!()
        }
    }
}

impl<E: Default + 'static> Deref for SmartGuard<E> {
    type Target = E;
    fn deref(&self) -> &Self::Target {
        match self {
            Reader(cell, ..) => unsafe { (*cell.0).cell.as_ref() },
            Writer(cell, ..) => unsafe { (*cell.0).cell.as_ref() },
        }
    }
}

impl<E: Default + 'static> SmartGuard<E> {
    #[inline(always)]
    pub fn upgrade_write_lock(&mut self) -> bool {
        match self {
            Reader(cell, read_latch) => unsafe {
                if let Some(write_latch)
                    = (*cell.0).write_lock(*read_latch & !WRITE_FLAG_VERSION)
                {
                    let writer = Writer(cell.clone(), write_latch);
                    ptr::write(self, writer);
                    return true;
                }
                false
            }
            _ => true
        }
    }

    /// Still an unexcluded `Reader` — as opposed to a `Writer` this same
    /// traversal already upgraded to (e.g. correcting a sibling's
    /// overflow/underflow earlier at this level), whose own held write lock
    /// makes its content unconditionally stable to read regardless of what
    /// `is_write_locked`/`live_version` report (that flag is *ours*).
    #[inline(always)]
    pub fn is_reader(&self) -> bool {
        matches!(self, Reader(..))
    }

    /// TEMPORARY diagnostic: has *anything* written to this cell since this
    /// guard's own snapshot was taken? Compares the version this `Reader`
    /// captured at `borrow_read()` time against the cell's current, live
    /// version — a mismatch means some writer acquired and released this
    /// cell's write lock in between, entirely unbeknownst to whoever is
    /// still holding this stale `Reader`. Used to check whether `simba`
    /// (read once for `split()`/`merge()`, then unconditionally retired via
    /// `mark_version_obsolete` with no re-validation at all) ever actually
    /// changes out from under a split in practice, not just in theory.
    /// Always `false` for a `Writer` (nothing else could have touched it).
    #[inline(always)]
    pub fn changed_since_snapshot(&self) -> bool {
        match self {
            Reader(cell, read_latch) => unsafe { (*cell.0).cell_version.load(Relaxed) != *read_latch },
            Writer(..) => false,
        }
    }

    /// TEMPORARY diagnostic: the cell's current, live version — for a
    /// precise "did this specific narrower window see a write" check,
    /// narrower than `changed_since_snapshot`'s "since this guard was first
    /// created" (which includes a lot of normal, harmless earlier activity).
    /// Call this immediately before `split()`/`merge()`'s own read, then
    /// compare the result against a second call after — a mismatch there
    /// means a write landed in exactly the window between the content
    /// snapshot and retirement, not merely sometime since this guard's
    /// birth.
    #[inline(always)]
    pub fn live_version(&self) -> LatchVersion {
        match self {
            Reader(cell, ..) |
            Writer(cell, ..) =>
                unsafe { (*cell.0).cell_version.load(Acquire) },
        }
    }

    /// Does *someone* (possibly a genuinely different thread) currently
    /// hold this cell's write lock? `cell_version` is pinned at one
    /// constant value — flag bit included — for a writer's entire critical
    /// section (only the unlock, via `Drop`, changes it), so a bare
    /// before/after `live_version()` comparison around a read is blind to a
    /// writer that's already mid-flight when "before" is sampled and still
    /// mid-flight at "after": both samples show the identical locked value,
    /// so nothing looks like it changed. Checking this flag *in addition to*
    /// the before/after comparison — bail before even reading if it's
    /// already set, not just if the value changes — closes that gap: a
    /// clean “unlocked, same version” bracket around the whole read window
    /// means no writer's critical section could have overlapped it at all.
    #[inline(always)]
    pub fn is_write_locked(&self) -> bool {
        self.live_version() & WRITE_FLAG_VERSION != 0
    }

    #[inline(always)]
    pub fn downgrade(&mut self) {
        match self {
            Writer(cell, ..) => {
                let reader = cell.borrow_read();
                drop(mem::replace(self, reader))
            }
            _ => { }
        }
    }

    pub fn inner_cell(self) -> SmartCell<E> {
        match self {
            Reader(ref cell, ..) => cell.clone(),
            Writer(ref cell, ..) => cell.clone(),
        }
    }

    pub fn inner(&self) -> SmartCell<E> {
        match self {
            Reader(cell, ..) => cell.clone(),
            Writer(cell, ..) => cell.clone(),
        }
    }

    // pub fn inner_cell(mut self) -> SmartCell<E> { // requires manual unlatch on reuse
    //     match self {
    //         Reader(cell, ..) => cell.clone(),
    //         Writer(ref cell, latch) => unsafe {
    //             let cell = transmute_copy(cell);
    //             ptr::write(&mut self, Reader(mem::transmute(&cell),
    //                                          latch & !WRITE_OBSOLETE_FLAG_VERSION));
    //             cell
    //         }
    //     }
    // }

    #[inline(always)]
    pub fn deref_mut(&self) -> &mut E {
        match self {
            Writer(cell, ..) => cell.unsafe_borrow_mut(),
            Reader(cell, ..) => cell.unsafe_borrow_mut(),
        }
    }
}

impl<E: Default> Deref for SmartCell<E> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        unsafe { (*self.0).cell.as_ref() }
    }
}

impl<E: Default> SmartCell<E> {
    #[inline(always)]
    pub fn unsafe_borrow(&self) -> &E {
        self.deref()
    }

    #[inline(always)]
    pub fn unsafe_borrow_mut(&self) -> &mut E {
        unsafe { (*self.0).cell.get_mut() }
    }

    /// `self.clone()` — a plain pointer copy now, not an `Arc::clone`/
    /// refcount bump: a `Reader` still owns its own `SmartCell` value (so it
    /// can be produced from, and used independently of, a single atomic
    /// load out of an `InternalPage`'s `pointer_region`; see that field's
    /// doc for why a bare reference into the array slot doesn't work), but
    /// nothing about that ownership keeps the pointee alive anymore — see
    /// `SmartCell`'s own doc for what now takes that role instead (nothing
    /// ever frees it, by design, as long as GC's block-reclaim stays off).
    #[inline(always)]
    pub fn borrow_read(&self) -> SmartGuard<E> {
        Reader(self.clone(), unsafe { (*self.0).cell_version.load(Acquire) } & !WRITE_FLAG_VERSION)
    }
}

impl<E: Default> Drop for SmartGuard<E> {
    fn drop(&mut self) {
        match self {
            Writer(cell, write_version) =>
                unsafe { (*cell.0).cell_version.store((*write_version + 1) ^ WRITE_FLAG_VERSION, Release) },
            _ => {}
        }
    }
}

unsafe impl<E: Default> Sync for SmartGuard<E> {}
unsafe impl<E: Default> Send for SmartGuard<E> {}
