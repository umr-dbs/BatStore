#![cfg(loom)]

//! Loom model of the synchronization shape used by
//! `TxContext::begin_snapshot_registration`/`end_snapshot_registration`/
//! `registrations_in_flight` (src/mv_sync/tx_context.rs) together with
//! `TrackerHandleSt::free_block`'s use of them (src/mv_gc/tracker_handle.rs):
//! one thread publishes a value into shared state and then clears an
//! "in flight" flag; a concurrent reader must never observe the flag clear
//! without also observing the published value, or it may treat a still-live
//! snapshot as if it doesn't exist and reclaim a block out from under it.
//!
//! Modeled in isolation rather than against the real `TxContext` — its other
//! fields (`crossbeam_skiplist::SkipMap`, a `parking_lot`-backed
//! `CommitLog`) aren't loom-instrumented, so exercising the real type here
//! wouldn't let loom explore the interleavings that matter. This reproduces
//! just the two atomics and the exact begin/publish/end/check shape and
//! orderings, so it stands or falls with whichever `Ordering`s are used for
//! the same pattern in the real code.
//!
//! Run with: RUSTFLAGS="--cfg loom" cargo test --test loom_registration_ordering --release

use loom::sync::Arc;
use loom::sync::atomic::Ordering::{self, Acquire, Relaxed, Release};
use loom::sync::atomic::{AtomicBool, AtomicUsize};
use loom::thread;

struct Model {
    /// Stands in for `live_tx`: shared state a registration publishes before
    /// it's safe for a reader to trust as complete.
    published: AtomicUsize,
    /// Stands in for `registrations_in_flight`: cleared only after
    /// `published` is written; checked by the reader before trusting it.
    in_flight: AtomicBool,
}

fn check(publish_order: Ordering, clear_order: Ordering, check_order: Ordering) {
    loom::model(move || {
        let model = Arc::new(Model {
            published: AtomicUsize::new(0),
            in_flight: AtomicBool::new(true),
        });

        let registrar = {
            let model = model.clone();
            thread::spawn(move || {
                model.published.store(42, publish_order);
                model.in_flight.store(false, clear_order);
            })
        };

        // GC-side: exactly `free_block`'s `registrations_in_flight() > 0`
        // guard followed by `live_min_snapshot()` — only trust `published`
        // once the registration is no longer in flight.
        while model.in_flight.load(check_order) {
            thread::yield_now();
        }
        assert_eq!(model.published.load(Relaxed), 42);

        registrar.join().unwrap();
    });
}

/// Demonstrates the bug this fix addresses: with `Relaxed` on both the clear
/// and the check, loom finds a legal interleaving where the reader observes
/// `in_flight == false` without yet observing the `published` write.
#[test]
#[should_panic]
fn relaxed_ordering_is_unsound() {
    check(Relaxed, Relaxed, Relaxed);
}

/// The actual fix: `Release` on the store that clears the flag, `Acquire` on
/// the load that checks it (mirrors `end_snapshot_registration`'s `fetch_sub`
/// and `registrations_in_flight()`'s `load` in tx_context.rs). Regression
/// test — if this starts failing, the ordering is no longer sufficient.
#[test]
fn release_acquire_ordering_is_sound() {
    check(Relaxed, Release, Acquire);
}
