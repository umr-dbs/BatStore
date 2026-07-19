use super::*;

/// Regression test for the `registrations_in_flight`/`live_tx` pairing (see
/// `begin_snapshot_registration`/`end_snapshot_registration`/
/// `registrations_in_flight`'s docs above, and `TrackerHandleSt::free_block`):
/// once a GC-style observer sees `registrations_in_flight() == 0` for a
/// registration it already knows is under way, `live_min_snapshot()` read
/// immediately after must cover that registration's snapshot.
///
/// Deliberately single-registration: a registration that starts *after* the
/// observer's `registrations_in_flight()` check is safe to miss regardless —
/// its `ts_start` is drawn from the same monotonic clock strictly later, so
/// it's provably >= any bound already in play. An earlier version of this
/// test raced 8 readers against 1 observer and compared against all of them,
/// which produced exactly that false positive (a late-starting, fast reader
/// legitimately not yet reflected) roughly 1 run in 6 — not a bug in
/// `TxContext`, just an over-strict test invariant. `registering` is a
/// test-only signal (Release/Acquire, correctly synchronized — unlike the
/// thing under test, this one isn't supposed to race) that lets the observer
/// wait until the registration has genuinely started before polling.
///
/// This exercises the pairing/logic under real concurrency (catches e.g. a
/// broken begin/end pairing, or the `free_block`/`live_min_snapshot` contract
/// regressing) but does not, on its own, prove the `Release`/`Acquire`
/// orderings are load-bearing — x86's TSO masks the specific memory-ordering
/// race this fix addresses, so a real reordering regression here wouldn't
/// reliably reproduce on this hardware. See
/// `tests/loom_registration_ordering.rs` for a model-checked test of the
/// ordering itself.
#[test]
fn registrations_in_flight_reaching_zero_implies_visible_insert() {
    let ctx = TxContext::new(1);
    ctx.block_reclaim_enabled.store(true, Relaxed);

    for _ in 0..2_000 {
        let registering = AtomicBool::new(false);

        std::thread::scope(|scope| {
            let reader = scope.spawn(|| {
                ctx.draw_snapshot_version_with(|ts_start| {
                    registering.store(true, Release);
                    ctx.on_tx_start(ts_start);
                    ts_start
                })
            });

            while !registering.load(Acquire) {
                std::thread::yield_now();
            }
            while ctx.registrations_in_flight() > 0 {
                std::thread::yield_now();
            }
            let min = ctx.live_min_snapshot();
            let v = reader.join().unwrap();

            assert!(
                matches!(min, Some(m) if m <= v),
                "registrations_in_flight() read 0 but live_min_snapshot() ({min:?}) \
                 doesn't cover a snapshot ({v}) whose registration was already under way"
            );

            ctx.end_snapshot(v);
        });
    }
}
