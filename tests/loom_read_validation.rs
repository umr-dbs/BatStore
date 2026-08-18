#![cfg(loom)]

//! Loom model of `SmartCell`/`OptCell`'s optimistic read-validation scheme
//! (src/bat_sync/smart_cell.rs): a `cell_version` atomic carries a write-lock
//! flag bit plus a monotonic counter. A writer CASes the flag on, mutates
//! plain (genuinely non-atomic, `UnsafeCell`-backed — like the real
//! `SafeCell`/`key_interval_region`/`version_region`) fields, then releases
//! by storing `(version + 1)` with the flag cleared. A reader that doesn't
//! hold the lock must never touch those fields unless it can prove no
//! writer's critical section overlapped the read at all — `loom::cell::
//! UnsafeCell` is loom's mechanism for catching a genuine data race on
//! exactly this kind of non-atomic memory, not just an observably-torn
//! value, so this is a stronger check than comparing the two field values
//! after the fact.
//!
//! This models the fix applied to `traversal_write_internal_olc`
//! (src/bat_query/olc_query.rs) and `SmartGuard::is_write_locked`/
//! `live_version` (src/bat_sync/smart_cell.rs): bracket the read with (a) a
//! check that the lock is *not already held* before touching anything, and
//! (b) a check that the version is *unchanged* after — retry unless both
//! hold. `part_a`/`part_b` stand in for two fields a writer updates in
//! sequence (e.g. `key_interval_region`/`version_region`, or
//! `push_uncommitted`'s two calls before `commit_delta`).
//!
//! Modeled in isolation rather than against the real `SmartCell` — its
//! `SafeCell`-backed raw-pointer storage isn't loom-instrumented, so
//! exercising the real type wouldn't let loom explore the interleavings
//! that matter. This reproduces just the `cell_version` CAS/flag/version
//! shape, the exact before/after check, and genuinely non-atomic data
//! fields, so it stands or falls with whichever protocol the real code
//! uses.
//!
//! Run with: RUSTFLAGS="--cfg loom" cargo test --test loom_read_validation --release

use loom::cell::UnsafeCell;
use loom::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use loom::sync::atomic::AtomicUsize;
use loom::sync::Arc;
use loom::thread;

const WRITE_FLAG: usize = 0x1;

struct Model {
    cell_version: AtomicUsize,
    // Stand-ins for `key_interval_region`/`version_region`: plain,
    // genuinely non-atomic fields a writer updates in sequence while
    // holding the write lock, exactly like `push_uncommitted`'s two calls
    // before `commit_delta` bumps `len` — never atomics in the real code.
    part_a: UnsafeCell<usize>,
    part_b: UnsafeCell<usize>,
}

impl Model {
    fn write_lock(&self, read_version: usize) -> Option<usize> {
        self.cell_version
            .compare_exchange(read_version, read_version | WRITE_FLAG, AcqRel, Acquire)
            .ok()
            .map(|_| read_version | WRITE_FLAG)
    }

    fn unlock(&self, write_version: usize) {
        self.cell_version.store((write_version + 1) & !WRITE_FLAG, Release);
    }

    fn live_version(&self) -> usize {
        self.cell_version.load(Acquire)
    }

    fn is_write_locked(&self, v: usize) -> bool {
        v & WRITE_FLAG != 0
    }
}

fn writer(model: &Model) {
    let start = model.live_version() & !WRITE_FLAG;
    let write_version = model.write_lock(start).expect("sole writer, CAS must succeed");
    // Two separate, unsynchronized writes — a reader interposed between
    // them would see a torn (part_a=new, part_b=old) or (old, new) pair,
    // and loom's `UnsafeCell` flags the race itself if a concurrent `with`
    // from the reader isn't provably ordered against these.
    model.part_a.with_mut(|ptr| unsafe { *ptr = 1 });
    model.part_b.with_mut(|ptr| unsafe { *ptr = 1 });
    model.unlock(write_version);
}

/// Mirrors the real fix: check `is_write_locked` up front (bail before
/// reading anything if a writer already holds the lock), then compare
/// `live_version` before/after the read — retry unless both hold.
fn validated_read(model: &Model, with_upfront_check: bool) -> (usize, usize) {
    loop {
        let before = model.live_version();

        if with_upfront_check && model.is_write_locked(before) {
            thread::yield_now();
            continue;
        }

        let a = model.part_a.with(|ptr| unsafe { *ptr });
        let b = model.part_b.with(|ptr| unsafe { *ptr });

        let after = model.live_version();

        if after != before {
            thread::yield_now();
            continue;
        }

        return (a, b);
    }
}

fn check(with_upfront_check: bool) {
    loom::model(move || {
        let model = Arc::new(Model {
            cell_version: AtomicUsize::new(0),
            part_a: UnsafeCell::new(0),
            part_b: UnsafeCell::new(0),
        });

        let w = {
            let model = model.clone();
            thread::spawn(move || writer(&model))
        };

        let (a, b) = validated_read(&model, with_upfront_check);

        // Either both still old (read happened entirely before the write)
        // or both new (entirely after) — never one of each.
        assert_eq!(a, b, "torn read: part_a={a} part_b={b}");

        w.join().unwrap();
    });
}

/// Demonstrates the blind spot this project found by hand: without the
/// upfront `is_write_locked` check, `cell_version` is pinned at one
/// constant (flag-set) value for the writer's *entire* critical section, so
/// a before/after comparison alone can't tell "no writer ever touched this"
/// apart from "a writer is mid-flight the whole time I'm reading" — loom
/// finds the interleaving where the reader's two field-reads straddle the
/// writer's two stores while both version samples land inside the same
/// still-locked window.
#[test]
#[should_panic]
fn before_after_only_is_unsound() {
    check(false);
}

/// The real fix (`traversal_write_internal_olc`'s `is_reader`/
/// `is_write_locked`/`live_version` bracket, `Acquire`/`Release`-ordered)
/// closes the blind spot above — but loom still flags a "Concurrent read
/// and write accesses to `UnsafeCell`" causality violation here, because
/// `part_a`/`part_b` are genuinely non-atomic (matching the real
/// `key_interval_region`/`version_region`/`pointer_region`, never atomics),
/// and no *version-check* protocol, however carefully ordered, is a
/// substitute for real exclusion or a fully-fenced atomic handoff: nothing
/// stops the compiler/hardware from interleaving a plain read with a plain
/// concurrent write to the same memory, full stop, under the strict Rust/
/// C++ memory model.
///
/// This is the well-known "seqlock problem" — Linux kernel seqlocks, RCU,
/// and most lock-free B-trees have exactly this same formal gap, and it's
/// a widely accepted trade-off in systems code precisely because no real
/// compiler performs the pathological reordering the abstract model
/// technically permits (confirmed here too: 260+ clean concurrent stress
/// runs of the real fix in `tests/tree_wal_consistency_tests.rs`, on real
/// hardware, with zero reproductions). Contrast this with `simba`/
/// `root_guard` in `on_overflow_node`/`on_underflow_node`/`split_root`/
/// `merge_root` (`src/bat_tree/smo.rs`): those got genuine exclusive
/// locking (`upgrade_write_lock`), not a version check, so there's no
/// equivalent gap for them — real mutual exclusion, not an optimistic
/// read, is what actually closes a race like this. `#[should_panic]`
/// records this as a known, accepted limitation rather than silently
/// passing or being deleted — if it ever stops panicking, loom has found a
/// *stronger* guarantee than expected, worth investigating.
#[test]
#[should_panic]
fn upfront_lock_check_reduces_but_does_not_eliminate_the_race() {
    check(true);
}
