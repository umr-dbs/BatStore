#![cfg(loom)]

//! Loom model of a *separate-flag-and-value* publish/clear pattern: one
//! thread publishes a value into shared state and then clears an "in flight"
//! flag; a concurrent reader must never observe the flag clear without also
//! observing the published value, or it may treat a still-live snapshot as
//! if it doesn't exist and reclaim a block out from under it.
//!
//! Historical note: this modeled `TxContext::begin_snapshot_registration`/
//! `end_snapshot_registration`/`registrations_in_flight`
//! (src/bat_sync/tx_context.rs) as they existed when this test was written —
//! a single global `AtomicUsize` counter every worker incremented/decremented,
//! checked via a separate `Acquire` load from `live_tx`'s `SkipMap` state.
//! That design was replaced by `TxContext::in_flight_bound` (see that field's
//! doc): a per-worker slot where the "flag" and the "value" are the *same*
//! atomic word (`NOT_IN_FLIGHT` sentinel vs. an actual bound), so a single
//! `Release`-store/`Acquire`-load pair on one location is sufficient on its
//! own — the two-atomics hazard this file models no longer has a live call
//! site. Kept as a standing regression guard against reintroducing this
//! specific shape (separate flag + value, `Relaxed` on either side of the
//! pairing) anywhere else in the codebase, and as a worked example of why
//! `Relaxed` is unsound for it.
//!
//! Modeled in isolation rather than against any real type — this reproduces
//! just the two atomics and the exact begin/publish/end/check shape and
//! orderings, independent of whatever real code may or may not still use it.
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

#[test]
fn release_acquire_ordering_is_sound() {
    check(Relaxed, Release, Acquire);
}
