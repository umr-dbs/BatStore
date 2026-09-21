use crate::bat_page_model::Attempts;
use crate::bat_record_model::AtomicVersion;
use crate::bat_record_model::version_info::Version;
use crate::bat_sync::safe_cell::SafeCell;
use crate::bat_sync::smart_cell::SmartGuard::{Reader, Writer};
use CCBPlusTree::locking::locking_strategy::LockingStrategy;
use std::fmt::{Display, Formatter};
use std::mem::transmute_copy;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::{hint, mem, ptr};

const WRITE_FLAG_VERSION: LatchVersion = 0x4_000000000000000;

const WRITE_PIN_FLAG_VERSION: LatchVersion = 0x6_000000000000000;
const WRITE_PIN_OBSOLETE_FLAG_VERSION: LatchVersion = 0xE_000000000000000;

/// Set once, permanently, the moment a cell stops being anyone's live child —
/// i.e. exactly when its *parent's* corresponding slot is superseded by a
/// fresh entry during the same split/merge (see `TrackerHandleSt::
/// register_died_page`/`register_died_page_col`, called from the same
/// `on_overflow_node`/`on_underflow_node`/`split_root`/`merge_root` sites
/// that supersede it). Closes a gap the version-CAS alone can't: a `Reader` that
/// obtained this cell as a *child pointer* before it was retired keeps
/// re-validating cleanly forever afterward — nothing ever mutates a retired
/// cell's content again, so a before/after `cell_version` comparison
/// trivially "passes" for it for all time, even though the parent that
/// handed it out no longer considers it live. A thread delayed (scheduling,
/// contention) between reading that stale parent and finally locking this
/// cell could walk an entire already-orphaned subtree and silently commit a
/// write nobody currently reachable from the root will ever see (confirmed
/// empirically: a fresh insert landing in a leaf a concurrent merge had
/// already folded into a different combined block moments earlier).
///
/// Packed into `cell_version` itself (a distinct bit from `WRITE_FLAG_VERSION`)
/// rather than a separate field: `borrow_read` masks it out of the captured
/// `read_latch` the same way it already masks `WRITE_FLAG_VERSION`, so a
/// retired cell's actual value (base version | this bit) can never again
/// equal any `read_latch` a `Reader` could hold — `write_lock`'s CAS fails
/// *by construction*, no separate check required for that path alone (an
/// explicit `is_retired` check on the `Reader` side is kept anyway, both as
/// a fast path that skips a doomed CAS and for the traversal's own
/// stale-ancestor gate, which never attempts to lock at all). The one thing
/// this sharing requires: `SmartGuard`'s `Writer` `Drop` must preserve
/// whatever's in this bit rather than blindly overwriting `cell_version` —
/// every call site that retires a node does so *while still holding its
/// dying write-lock guard* (the guard only drops at scope end), so without
/// that preservation, `Drop`'s unconditional store — computed purely from
/// the version captured back at lock *acquisition* time, before any
/// retirement — would silently erase the bit again.
///
/// Independent of GC/block-reclaim — this is a correctness fix, not a
/// reclaim-scheduling one, so it applies whether or not block reuse is ever
/// turned on. Cleared (`SmartCell::clear_retired`) when a block is handed
/// back out by `free_block` for reuse.
const RETIRED_FLAG_VERSION: LatchVersion = 0x2_000000000000000;

#[cfg(all(
    feature = "hardware-lock-elision",
    any(target_arch = "x86", target_arch = "x86_64")
))]
pub trait AtomicElisionExt {
    fn elision_compare_exchange_acquire(
        &self,
        current: Version,
        new: Version,
    ) -> Result<Version, Version>;
}

#[cfg(all(
    feature = "hardware-lock-elision",
    any(target_arch = "x86", target_arch = "x86_64")
))]
impl AtomicElisionExt for AtomicVersion {
    #[inline(always)]
    fn elision_compare_exchange_acquire(
        &self,
        current: Version,
        new: Version,
    ) -> Result<Version, Version> {
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
            if prev == current { Ok(prev) } else { Err(prev) }
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
/// idea as Ethernet/TCP backoff jitter.
///
/// Lowered from 1000 (2026-08-09): `bat_test::RESTART_TRACE` attempt-count
/// profiling under TPC-C showed retries are sharply bimodal — 98.6% of
/// writes never retry at all, but the ~1.4% that do retry often do so deep
/// into the hundreds of attempts. Under the old threshold, that entire
/// stretch (attempts 4 through 1000) was spent calling plain
/// `sched_yield()`/`yield_now()` with no growth at all; under a saturated
/// machine (all cores busy) that syscall frequently returns almost
/// immediately, so the tail was effectively a tight retry loop hammering
/// the same contended cache line (root's `cell_version`, or a hot internal
/// page's) at close to full speed, not a backoff. Real jittered backoff now
/// starts far sooner for exactly the writes that need it, while ordinary,
/// quickly-resolving contention (still the overwhelming majority) never
/// reaches it.
const JITTER_BACKOFF_THRESHOLD: Attempts = 64;

/// Above this many attempts, stop growing the busy-spin and hand the CPU to
/// the scheduler instead (`sched_yield`/`yield_now`) — spinning
/// `2^attempt` times would already be excessive by here, so further
/// attempts get a flat, capped spin count instead of unbounded growth.
const EXP_SPIN_CAP: Attempts = 6;

#[inline(always)]
fn exp_spin(attempt: Attempts) {
    for _ in 0..(1u32 << attempt.min(EXP_SPIN_CAP) as u32) {
        hint::spin_loop();
    }
}

#[inline(always)]
#[cfg(target_os = "linux")]
pub fn sched_yield(attempt: Attempts) {
    if attempt > JITTER_BACKOFF_THRESHOLD {
        let jitter_micros = rand::random_range(1..=attempt.min(5_000));
        std::thread::sleep(std::time::Duration::from_micros(jitter_micros as u64));
    } else if attempt > FORCE_YIELD {
        unsafe {
            libc::sched_yield();
        }
    } else {
        exp_spin(attempt);
    }
}

pub const FORCE_YIELD: Attempts = 4;

#[inline(always)]
#[cfg(not(target_os = "linux"))]
pub fn sched_yield(attempt: Attempts) {
    if attempt > JITTER_BACKOFF_THRESHOLD {
        let jitter_micros = rand::random_range(1..=attempt.min(5_000));
        std::thread::sleep(std::time::Duration::from_micros(jitter_micros as u64));
    } else if attempt > FORCE_YIELD {
        std::thread::yield_now();
    } else {
        exp_spin(attempt);
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
        write!(
            f,
            "OptCell {{\ncell: {}\n\t\tcell_version: {}\n\t}}",
            self.cell.get_mut(),
            self.cell_version.load(Relaxed)
        )
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

    #[cfg(not(all(
        feature = "hardware-lock-elision",
        any(target_arch = "x86", target_arch = "x86_64")
    )))]
    #[inline(always)]
    pub fn write_lock(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        match self.cell_version.compare_exchange_weak(
            read_version,
            WRITE_FLAG_VERSION | read_version,
            AcqRel,
            Acquire,
        ) {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => None,
        }
    }

    #[cfg(all(
        feature = "hardware-lock-elision",
        any(target_arch = "x86", target_arch = "x86_64")
    ))]
    #[inline(always)]
    pub fn write_lock(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        match self
            .cell_version
            .elision_compare_exchange_acquire(read_version, WRITE_FLAG_VERSION | read_version)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => None,
        }
    }
}

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
            _ => unreachable!(),
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
                if (*cell.0).cell_version.load(Acquire) & RETIRED_FLAG_VERSION != 0 {
                    return false;
                }

                if let Some(write_latch) =
                    (*cell.0).write_lock(*read_latch & !(WRITE_FLAG_VERSION | RETIRED_FLAG_VERSION))
                {
                    let writer = Writer(cell.clone(), write_latch);
                    ptr::write(self, writer);
                    return true;
                }
                false
            },
            _ => true,
        }
    }

    #[inline(always)]
    pub fn is_retired(&self) -> bool {
        match self {
            Reader(cell, ..) => unsafe {
                (*cell.0).cell_version.load(Acquire) & RETIRED_FLAG_VERSION != 0
            },
            Writer(..) => false,
        }
    }

    #[inline(always)]
    pub fn retire(self) -> SmartCell<E> {
        match &self {
            Writer(cell, write_version) => unsafe {
                let cell_copy = cell.clone();
                (*cell.0).cell_version.store(
                    ((*write_version + 1) ^ WRITE_FLAG_VERSION) | RETIRED_FLAG_VERSION,
                    Release,
                );
                mem::forget(self);
                cell_copy
            },
            Reader(cell, ..) => {
                let cell_copy = cell.clone();
                cell_copy.mark_retired();
                cell_copy
            }
        }
    }

    #[inline(always)]
    pub fn try_retire(self) -> Result<SmartCell<E>, Self> {
        match self {
            Reader(cell, read_latch) => unsafe {
                match (*cell.0).cell_version.compare_exchange_weak(
                    read_latch,
                    read_latch | RETIRED_FLAG_VERSION,
                    AcqRel,
                    Acquire,
                ) {
                    Ok(..) => Ok(cell),
                    Err(..) => Err(Reader(cell, read_latch)),
                }
            },
            writer @ Writer(..) => Ok(writer.retire()),
        }
    }

    #[inline(always)]
    pub fn is_reader(&self) -> bool {
        matches!(self, Reader(..))
    }

    #[inline(always)]
    pub fn live_version(&self) -> LatchVersion {
        match self {
            Reader(cell, ..) => unsafe { (*cell.0).cell_version.load(Acquire) },
            Writer(.., latch) => *latch,
        }
    }

    #[inline(always)]
    pub fn is_write_locked(&self) -> bool {
        self.live_version() & WRITE_FLAG_VERSION != 0
    }

    #[inline(always)]
    pub fn checked_live_version(&self) -> Option<LatchVersion> {
        match self {
            Reader(cell, ..) => unsafe {
                let v = (*cell.0).cell_version.load(Acquire);
                if v & (WRITE_FLAG_VERSION | RETIRED_FLAG_VERSION) != 0 {
                    None
                } else {
                    Some(v)
                }
            },
            Writer(.., latch) => Some(*latch),
        }
    }

    #[inline(always)]
    pub fn downgrade(&mut self) {
        match self {
            Writer(cell, ..) => {
                let reader = cell.borrow_read();
                drop(mem::replace(self, reader))
            }
            _ => {}
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

    #[inline(always)]
    pub fn borrow_read(&self) -> SmartGuard<E> {
        Reader(
            self.clone(),
            unsafe { (*self.0).cell_version.load(Acquire) }
                & !(WRITE_FLAG_VERSION | RETIRED_FLAG_VERSION),
        )
    }

    #[inline(always)]
    pub fn mark_retired(&self) {
        unsafe {
            (*self.0)
                .cell_version
                .fetch_or(RETIRED_FLAG_VERSION, AcqRel);
        }
    }

    #[inline(always)]
    pub fn is_retired(&self) -> bool {
        unsafe { (*self.0).cell_version.load(Acquire) & RETIRED_FLAG_VERSION != 0 }
    }

    /// Reverses `mark_retired` for a block GC hands back out via
    /// `free_block` — otherwise a reused block would look permanently
    /// retired to `upgrade_write_lock` and could never be written to again.
    #[inline(always)]
    pub fn clear_retired(&self) {
        unsafe {
            (*self.0)
                .cell_version
                .fetch_and(!RETIRED_FLAG_VERSION, AcqRel);
        }
    }
}

impl<E: Default> Drop for SmartGuard<E> {
    fn drop(&mut self) {
        match self {
            Writer(cell, write_version) => unsafe {
                let retired_bit = (*cell.0).cell_version.load(Relaxed) & RETIRED_FLAG_VERSION;

                (*cell.0).cell_version.store(
                    ((*write_version + 1) ^ WRITE_FLAG_VERSION) | retired_bit,
                    Release,
                )
            },
            _ => {}
        }
    }
}

unsafe impl<E: Default> Sync for SmartGuard<E> {}
unsafe impl<E: Default> Send for SmartGuard<E> {}
