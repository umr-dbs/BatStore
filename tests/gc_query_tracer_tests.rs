use crate::mv_gc::query_tracer::TransactionTrace;

#[test]
fn on_tx_start_then_completed_does_not_leak() {
    let trace = TransactionTrace::new();
    for v in 0..100_000u64 {
        trace.on_tx_start(v);
        trace.on_tx_completed(v);
    }
    assert_eq!(trace.len(), 0, "map should be empty after every start is matched by a completed");
}

#[test]
fn concurrent_same_value_registrations_stack_safely() {
    let trace = TransactionTrace::new();
    trace.on_tx_start(42);
    trace.on_tx_start(42);
    assert_eq!(trace.refcount(42), Some(2));
    trace.on_tx_completed(42);
    assert_eq!(trace.refcount(42), Some(1));
    trace.on_tx_completed(42);
    assert_eq!(trace.refcount(42), None);
}
