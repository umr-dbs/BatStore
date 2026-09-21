use super::*;

#[test]
fn pruning_preserves_commit_while_snapshot_registration_is_in_flight() {
    let ctx = TxContext::new(2);
    let writer = ctx.worker_id();
    assert_eq!(ctx.global_clock.next_timestamp(), 1);
    assert_eq!(ctx.commit_tx(writer), 2);

    let (bound_tx, bound_rx) = std::sync::mpsc::channel();
    let (draw_tx, draw_rx) = std::sync::mpsc::channel();
    let (snapshot_tx, snapshot_rx) = std::sync::mpsc::channel();
    let (publish_tx, publish_rx) = std::sync::mpsc::channel();

    std::thread::scope(|scope| {
        let ctx_ref = &ctx;
        let reader = scope.spawn(move || {
            let reader_worker = ctx_ref.begin_snapshot_registration();
            bound_tx
                .send(ctx_ref.in_flight_bound[reader_worker as usize].load(Acquire))
                .unwrap();
            draw_rx.recv().unwrap();
            let snapshot = ctx_ref.global_clock.next_timestamp();
            snapshot_tx.send(snapshot).unwrap();
            publish_rx.recv().unwrap();
            ctx_ref.on_tx_start(snapshot);
            ctx_ref.end_snapshot_registration(reader_worker);
            ctx_ref.end_snapshot(snapshot);
        });

        assert_eq!(bound_rx.recv().unwrap(), 3);
        assert_eq!(ctx.global_clock.next_timestamp(), 3);
        assert_eq!(ctx.commit_tx(writer), 4);
        draw_tx.send(()).unwrap();
        let snapshot = snapshot_rx.recv().unwrap();
        assert_eq!(snapshot, 5);
        assert_eq!(ctx.global_clock.next_timestamp(), 6);
        assert_eq!(ctx.commit_tx(writer), 7);
        assert_eq!(ctx.commit_logs[writer as usize].lcb(snapshot), 4);

        for _ in 0..512 {
            ctx.global_clock.next_timestamp();
            ctx.commit_tx(writer);
            assert_eq!(ctx.commit_logs[writer as usize].lcb(snapshot), 4);
        }

        publish_tx.send(()).unwrap();
        reader.join().unwrap();

        ctx.global_clock.next_timestamp();
        ctx.commit_tx(writer);
        assert_eq!(ctx.commit_logs[writer as usize].len(), 1);
    });
}

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

/// `live_tx`'s replacement for the old `bat_gc::query_tracer::TransactionTrace`
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
