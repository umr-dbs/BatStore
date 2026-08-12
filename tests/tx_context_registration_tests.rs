use super::*;

/// Regression test for the per-worker `in_flight_bound`/`live_tx` pairing
/// (see `begin_snapshot_registration`/`end_snapshot_registration`/
/// `in_flight_bound`'s docs above, and `TrackerHandleSt::free_block`): once a
/// GC-style observer sees a registration is under way (has drawn its
/// `ts_start`), `live_min_snapshot()` read at that point (or any point
/// after) must already cover that registration's snapshot — there's no
/// window to wait out, since the per-worker slot is published *before* the
/// real `ts_start` is even drawn (see `begin_snapshot_registration`'s doc),
/// unlike the old single global counter this replaced, which only reached
/// zero (and so only became safe to trust) *after* the whole registration
/// had fully completed.
///
/// Deliberately single-registration: a registration that starts *after* the
/// observer's check is safe to miss regardless — its `ts_start` is drawn
/// from the same monotonic clock strictly later, so it's provably >= any
/// bound already in play. An earlier version of this test raced 8 readers
/// against 1 observer and compared against all of them, which produced
/// exactly that false positive (a late-starting, fast reader legitimately
/// not yet reflected) roughly 1 run in 6 — not a bug in `TxContext`, just an
/// over-strict test invariant. `registering` is a test-only signal
/// (Release/Acquire, correctly synchronized — unlike the thing under test,
/// this one isn't supposed to race) that lets the observer wait until the
/// registration has genuinely started before checking.
///
/// This exercises the pairing/logic under real concurrency (catches e.g. a
/// broken begin/end pairing, or the `free_block`/`live_min_snapshot` contract
/// regressing) but does not, on its own, prove the `Release`/`Acquire`
/// orderings are load-bearing — x86's TSO masks the specific memory-ordering
/// race this fix addresses, so a real reordering regression here wouldn't
/// reliably reproduce on this hardware. See
/// `tests/loom_registration_ordering.rs` for a model-checked test of the
/// ordering shape (a single `Release`-store/`Acquire`-load pair on one
/// atomic — the same pattern each per-worker slot now uses on its own).
///
/// One long-lived reader thread across all 2,000 iterations, not one fresh
/// thread per iteration: `in_flight_bound` is indexed by `WorkerId`, drawn
/// once per *thread* and cached for that thread's lifetime
/// (`worker::worker_id_for`) — matching `WorkerRegistry`'s documented model
/// of a fixed pool of long-lived worker threads, never handed back. Spawning
/// a fresh thread per iteration against `TxContext::new(1)` (as an earlier
/// version of this test did) draws a fresh, never-reused `WorkerId` each
/// time, exceeding `max_workers` on the second iteration and panicking
/// *inside* `begin_snapshot_registration` — before `registering` is ever set,
/// so the observer's wait loop spins forever instead of seeing the panic.
/// That's a test-structure bug, not a soundness one: real callers (a fixed
/// benchmark worker pool) never rotate threads per transaction the way that
/// pattern did.
///
/// The reader thread — not the main/observer thread — calls `end_snapshot`
/// for its own registration (via the `proceed` handshake below): `live_tx`'s
/// per-worker-slot design resolves *which* slot `on_tx_completed` touches
/// from the calling thread's own `WorkerId`, exactly like `on_tx_start`
/// already did — so, like every real caller (`DbTransaction`, `traversal_
/// write_olc`), begin and end must happen on the same thread. An earlier
/// version of this test had the main thread call `ctx.end_snapshot(v)`
/// directly, which — having never registered a `WorkerId` against this
/// `TxContext` itself — tried to acquire a second one against `max_workers
/// == 1` and panicked; not a soundness bug, just this test doing something
/// no real caller does.
#[test]
fn in_flight_registration_is_immediately_visible_to_live_min_snapshot() {
    let ctx = TxContext::new(1);
    ctx.block_reclaim_enabled.store(true, Relaxed);

    let registering = AtomicBool::new(false);
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let (registered_tx, registered_rx) = std::sync::mpsc::channel::<Version>();
    let (proceed_tx, proceed_rx) = std::sync::mpsc::channel::<()>();

    std::thread::scope(|scope| {
        let ctx_ref = &ctx;
        let registering_ref = &registering;
        // `move`: `go_rx`/`registered_tx`/`proceed_rx` (single-consumer/
        // producer channel ends, only ever used by this thread) are owned by
        // the closure; `ctx_ref`/`registering_ref` are `&_` (Copy), so moving
        // *them* just copies the reference, leaving the outer bindings usable
        // below.
        let reader = scope.spawn(move || {
            for _ in go_rx.iter() {
                let v = ctx_ref.draw_snapshot_version_with(|ts_start| {
                    registering_ref.store(true, Release);
                    ctx_ref.on_tx_start(ts_start);
                    ts_start
                });
                registered_tx.send(v).unwrap();
                proceed_rx.recv().unwrap();
                ctx_ref.end_snapshot(v);
            }
        });

        for _ in 0..2_000 {
            registering.store(false, Relaxed);
            go_tx.send(()).unwrap();

            while !registering.load(Acquire) {
                std::thread::yield_now();
            }
            let min = ctx.live_min_snapshot();
            let v = registered_rx.recv().unwrap();

            assert!(
                matches!(min, Some(m) if m <= v),
                "live_min_snapshot() ({min:?}) doesn't cover a snapshot ({v}) \
                 whose registration was already under way"
            );

            proceed_tx.send(()).unwrap();
        }

        drop(go_tx);
        reader.join().unwrap();
    });
}

/// `live_tx`'s replacement for the old `mv_gc::query_tracer::TransactionTrace`
/// (a shared, contended `SkipMap`) is one slot per worker: a plain start/end
/// pair must publish while live and clear once completed.
#[test]
fn on_tx_start_then_completed_leaves_no_live_registration() {
    let ctx = TxContext::new(1);
    ctx.block_reclaim_enabled.store(true, Relaxed);

    assert_eq!(ctx.live_min_snapshot(), None);
    let ts = ctx.draw_snapshot_version_with(|ts_start| {
        ctx.on_tx_start(ts_start);
        ts_start
    });
    assert_eq!(ctx.live_min_snapshot(), Some(ts));

    ctx.end_snapshot(ts);
    assert_eq!(ctx.live_min_snapshot(), None);
}

/// Ordinary nested snapshot registrations must preserve the outer value.
#[test]
fn nested_registration_on_the_same_worker_keeps_the_outer_one_published() {
    let ctx = TxContext::new(1);
    ctx.block_reclaim_enabled.store(true, Relaxed);

    let outer = ctx.draw_snapshot_version_with(|ts_start| {
        ctx.on_tx_start(ts_start);
        ts_start
    });
    assert_eq!(ctx.live_min_snapshot(), Some(outer));

    let inner = ctx.draw_snapshot_version_with(|ts_start| {
        ctx.on_tx_start(ts_start);
        ts_start
    });
    assert!(
        inner > outer,
        "the global clock is monotonic, so the nested draw must be strictly newer"
    );
    assert_eq!(
        ctx.live_min_snapshot(),
        Some(outer),
        "the outer (still-running) transaction's snapshot must stay published, \
         not get overwritten by the nested traversal's throwaway one"
    );

    ctx.end_snapshot(inner);
    assert_eq!(
        ctx.live_min_snapshot(),
        Some(outer),
        "completing the nested registration must not clear the still-live outer one"
    );

    ctx.end_snapshot(outer);
    assert_eq!(ctx.live_min_snapshot(), None);
}

#[test]
fn reclamation_pin_protects_without_advancing_the_clock() {
    let ctx = TxContext::new(1);
    ctx.set_block_reclaim_enabled(true);
    let before = ctx.current_version();

    ctx.with_reclamation_pin(|| {
        assert_eq!(ctx.live_min_snapshot(), Some(before));
        assert_eq!(ctx.current_version(), before);
    });

    assert_eq!(ctx.live_min_snapshot(), None);
    assert_eq!(ctx.current_version(), before);
}

#[test]
fn reclamation_pin_nested_in_transaction_uses_outer_snapshot() {
    let ctx = TxContext::new(1);
    ctx.set_block_reclaim_enabled(true);
    let outer = ctx.begin_snapshot();
    let clock_after_begin = ctx.current_version();

    ctx.with_reclamation_pin(|| {
        assert_eq!(ctx.live_min_snapshot(), Some(outer));
        assert_eq!(ctx.current_version(), clock_after_begin);
    });

    assert_eq!(ctx.live_min_snapshot(), Some(outer));
    ctx.end_snapshot(outer);
}
