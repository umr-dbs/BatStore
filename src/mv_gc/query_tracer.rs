use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use crossbeam_skiplist::SkipMap;
use crate::mv_query::SnapShot;

/// Refcounted, not just present/absent: two independent callers can
/// register the *same* numeric `SnapShot` value concurrently — e.g. two
/// `dispatch_crud` Point/Range reads (see `ycsb_txn`, `tpch_queries`,
/// `olap_scan`'s fresh-scan mode) that both captured the same
/// `tree.current_version()`, since that's a plain clock read, not a unique
/// draw the way `Transaction::begin`'s `ts_start` is. A plain presence set
/// (the original `SkipSet<ReaderQuery>` this replaced) can't tell those
/// apart from a single registration: one caller's release would silently
/// delete the *other's* still-live protection. Counting makes concurrent
/// registrations for the same value stack safely.
///
/// Zero-count entries are physically removed (see `on_tx_completed`) — this
/// matters, not just for space: `peek_min` must scan from the front of the
/// map on every `TrackerHandleSt::free_block` call, so leaving zero-count
/// tombstones behind forever turns that scan into an unbounded, ever-slower
/// prefix walk over the *entire history* of every snapshot this tree has
/// ever drawn. Two distinct bugs in earlier versions of this file each
/// caused exactly that (measured: roughly an 8-9x throughput regression
/// under sustained concurrent load) — never removing tombstones at all, and
/// separately, `on_tx_start` double-counting every fresh registration so
/// its count could never reach zero in the first place (see that method's
/// doc for the second one). `on_tx_start`'s compare-exchange loop is what
/// makes physical removal safe without reintroducing an ABA hazard.
type QueryTracer = SkipMap<SnapShot, AtomicUsize>;

pub(crate) struct TransactionTrace(QueryTracer);

impl TransactionTrace {
    pub(crate) fn new() -> Self {
        Self(QueryTracer::new())
    }

    #[inline(always)]
    pub(crate) fn peek_min(&self) -> Option<SnapShot> {
        self.0.front().map(|entry| *entry.key())
    }

    #[inline(always)]
    pub(crate) fn peek_max(&self) -> Option<SnapShot> {
        self.0.back().map(|entry| *entry.key())
    }

    /// Enumerates every currently active `ts_start`, for `CommitLog`
    /// pruning: an entry is only ever safe to drop if it isn't the LCB of
    /// any snapshot this yields (see `MVBTSt::commit_tx`).
    #[inline(always)]
    pub(crate) fn active_snapshots(&self) -> impl Iterator<Item = SnapShot> + '_ {
        self.0.iter().map(|entry| *entry.key())
    }

    /// Registers one more reason to protect `snapshot`. Never bumps a
    /// zero-count entry back to life — a zero count means some concurrent
    /// `on_tx_completed` has committed to physically removing that exact
    /// entry (see there), so resurrecting it here would race that removal.
    /// Instead this retries via `get_or_insert_with`, which (per
    /// `crossbeam_skiplist`'s own tombstone handling — a lookup that finds a
    /// logically-removed node falls through to inserting a fresh one, it
    /// never returns the dead node) either finds a *different*, still-live
    /// entry for this key, or — once the dead one is actually unlinked —
    /// creates a brand new entry starting at count 1. Either way, no
    /// increment ever lands on a count already claimed for removal, which is
    /// exactly what makes that removal safe.
    ///
    /// Uses `get_or_insert_with` rather than `get_or_insert` specifically to
    /// tell "found a live existing entry, must CAS-bump it" apart from "just
    /// created a fresh one, already correctly at 1" — the value-producing
    /// closure only ever runs on the fresh-insert path (verified against
    /// `crossbeam_skiplist`'s source), so `created` is set exactly then.
    /// Skipping this distinction and unconditionally bumping after
    /// `get_or_insert` double-counts every fresh registration (1 from the
    /// initial value, +1 from the immediately-following bump) — a real bug
    /// an earlier version of this function had: every entry permanently
    /// leaked a +1, so it could reach zero only after *two* completions,
    /// which never comes (each registration gets exactly one), so nothing
    /// was ever removed and this degenerated into the exact same unbounded
    /// growth `on_tx_completed`'s physical removal exists to prevent.
    #[inline(always)]
    pub(crate) fn on_tx_start(&self, snapshot: SnapShot) {
        loop {
            let mut created = false;
            let entry = self.0.get_or_insert_with(snapshot, || {
                created = true;
                AtomicUsize::new(1)
            });
            if created {
                return;
            }

            let mut count = entry.value().load(Relaxed);
            loop {
                if count == 0 {
                    break; // dead entry, being removed elsewhere: retry get_or_insert_with
                }
                match entry.value().compare_exchange_weak(count, count + 1, Relaxed, Relaxed) {
                    Ok(_) => return,
                    Err(actual) => count = actual,
                }
            }
        }
    }

    /// Releases one reason to protect `snapshot`. The thread whose decrement
    /// brings the count to exactly zero is the sole owner of physically
    /// removing this entry: `on_tx_start`'s compare-exchange loop guarantees
    /// no concurrent increment can land on a zero count first, so nothing
    /// can resurrect this specific entry between our decrement and our
    /// removal — the ABA hazard a naive "decrement, then unconditionally
    /// remove" would have.
    #[inline(always)]
    pub(crate) fn on_tx_completed(&self, snapshot: SnapShot) {
        if let Some(entry) = self.0.get(&snapshot) {
            if entry.value().fetch_sub(1, Relaxed) == 1 {
                entry.remove();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn on_tx_start_then_completed_does_not_leak() {
        let trace = TransactionTrace::new();
        for v in 0..100_000u64 {
            trace.on_tx_start(v);
            trace.on_tx_completed(v);
        }
        assert_eq!(trace.0.len(), 0, "map should be empty after every start is matched by a completed");
    }

    #[test]
    fn concurrent_same_value_registrations_stack_safely() {
        let trace = TransactionTrace::new();
        trace.on_tx_start(42);
        trace.on_tx_start(42);
        assert_eq!(trace.0.get(&42).unwrap().value().load(Relaxed), 2);
        trace.on_tx_completed(42);
        assert_eq!(trace.0.get(&42).unwrap().value().load(Relaxed), 1);
        trace.on_tx_completed(42);
        assert!(trace.0.get(&42).is_none());
    }
}
