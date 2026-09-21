//! Regression coverage for the OOM seen while running
//! `compare_wal_backends_tpcc`: `bat_test::RESTART_GLOBAL`/
//! `ROOT_RESTARTS_BY_TABLE` are process-lifetime `static`s that
//! `record_restart`/`record_root_restart_for_table` only ever insert into,
//! never clear on their own — so a driver calling `run_tpcc` more than once
//! in the same process (exactly what the backend-comparison loop does) made
//! them grow without bound across runs once `RESTART_TRACE` (meant to
//! default off) had been left `true`. Two things went wrong, so two things
//! are checked here, both deliberately cheap (no full benchmark run/data
//! population) so this stays fast in every `cargo test`.

use crate::bat_test;

#[test]
fn restart_trace_defaults_to_off() {
    assert!(
        !bat_test::RESTART_TRACE,
        "RESTART_TRACE must default to false - leaving it true makes every \
         write-traversal restart record itself into a process-lifetime \
         global map that's never cleared, which OOM'd a real benchmark run"
    );
}

fn record_some_restarts() {
    std::thread::spawn(|| {
        bat_test::record_restart(0xAAAA, &"custkey-1", "leaf_write_lock");
        bat_test::record_restart(0xAAAA, &"custkey-2", "leaf_write_lock");
        bat_test::record_root_restart_for_table(0xBEEF);
    })
    .join()
    .unwrap();
}

#[test]
fn restart_trace_reset_clears_previous_run_data() {
    if !bat_test::RESTART_TRACE {
        return;
    }

    bat_test::reset_restart_trace();
    record_some_restarts();
    let after_first = bat_test::restart_trace_footprint();
    assert!(
        after_first > 0,
        "expected recorded restarts to show up in the global footprint"
    );

    bat_test::reset_restart_trace();
    record_some_restarts();
    let after_second = bat_test::restart_trace_footprint();

    assert_eq!(
        after_second, after_first,
        "footprint should reflect only the latest run after reset, not accumulate across runs"
    );
}
