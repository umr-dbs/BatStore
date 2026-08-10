//! Regression coverage for the OOM seen while running
//! `compare_wal_backends_tpcc`: `mv_test::RESTART_GLOBAL`/
//! `ROOT_RESTARTS_BY_TABLE` are process-lifetime `static`s that
//! `record_restart`/`record_root_restart_for_table` only ever insert into,
//! never clear on their own — so a driver calling `run_tpcc` more than once
//! in the same process (exactly what the backend-comparison loop does) made
//! them grow without bound across runs once `RESTART_TRACE` (meant to
//! default off) had been left `true`. Two things went wrong, so two things
//! are checked here, both deliberately cheap (no full benchmark run/data
//! population) so this stays fast in every `cargo test`.

use crate::mv_test;

/// The actual bug: `RESTART_TRACE`'s own doc says "Off by default", but the
/// `const` had been left `true` in checked-in code, so every write-traversal
/// restart unconditionally paid to record itself into the global maps
/// below — on a real benchmark run, fast/contended enough (see
/// `tests/tpcc_wal_backend_bench.rs`'s lock-free WAL backend), that's
/// millions of heap-allocated `String` keys per run, accumulating forever
/// across repeated in-process runs.
#[test]
fn restart_trace_defaults_to_off() {
    assert!(
        !mv_test::RESTART_TRACE,
        "RESTART_TRACE must default to false - leaving it true makes every \
         write-traversal restart record itself into a process-lifetime \
         global map that's never cleared, which OOM'd a real benchmark run"
    );
}

fn record_some_restarts() {
    // A separate, joined thread: `record_restart`'s doc is explicit that its
    // data sits in that thread's own TLS until the thread exits (`Drop`
    // merges it into the global map) - recording on the test's own thread
    // and reading `restart_trace_footprint()` back immediately would just
    // see zero, without exercising the merge at all.
    std::thread::spawn(|| {
        mv_test::record_restart(0xAAAA, &"custkey-1", "leaf_write_lock");
        mv_test::record_restart(0xAAAA, &"custkey-2", "leaf_write_lock");
        mv_test::record_root_restart_for_table(0xBEEF);
    })
    .join()
    .unwrap();
}

/// Defense in depth for whenever `RESTART_TRACE` is deliberately flipped
/// back on for an investigation (its documented purpose): `run_tpcc` now
/// calls `reset_restart_trace()` at the start of every run, so a second run
/// in the same process reflects only its own data, not the first run's on
/// top of it. No-op assertion when `RESTART_TRACE` is off (the default) -
/// `record_restart` itself is a no-op then, so there's nothing to reset.
#[test]
fn restart_trace_reset_clears_previous_run_data() {
    if !mv_test::RESTART_TRACE {
        return;
    }

    mv_test::reset_restart_trace();
    record_some_restarts();
    let after_first = mv_test::restart_trace_footprint();
    assert!(after_first > 0, "expected recorded restarts to show up in the global footprint");

    mv_test::reset_restart_trace();
    record_some_restarts();
    let after_second = mv_test::restart_trace_footprint();

    assert_eq!(
        after_second, after_first,
        "footprint should reflect only the latest run after reset, not accumulate across runs"
    );
}
