//! Investigates whether the intermittent `debug_assert!` failure at
//! `mv_tree::smo::split` (around line 803: "Active records = N, required
//! >= M") genuinely requires two threads' writes to physically overlap in
//! time on the same leaf, or whether something else is responsible.
//!
//! Established so far (see conversation history): reaching the version-split
//! branch at all requires `unsafe_degree()` (`mv_tree/smo.rs`, called on the
//! leaf via `next_curr_guard` at `mv_query/olc_query.rs:161`) to have read
//! `active > 2 * filling_20_percent(n)` on *that exact leaf* — for `n=8`,
//! `active >= 5`. The assertion later wants that same leaf's active count,
//! rescanned independently inside `split()` (`smo.rs:797-801`), to still be
//! `>= filling_40_percent(n) = 4`. Only the *parent* guard gets upgraded to
//! an exclusive write lock before a split (`curr_guard.upgrade_write_lock()`
//! at `olc_query.rs:162/166`) — the leaf itself (`next_curr_guard`/`simba`)
//! stays a plain, never-upgraded, never-revalidated `Reader` the entire
//! time. So *if* another thread's independently-write-locked `dispatch_crud`
//! mutates that same physical leaf between the `unsafe_degree()` read and
//! the later rescan, active can genuinely drop from a confirmed `>=5` to
//! `3` — a real race, not a rounding artifact (already ruled out separately
//! for the key-split halving case).
//!
//! This file tests that claim directly by running the *exact same*
//! insert→update→delete-even-keys workload from
//! `tree_wal_consistency_tests::concurrent_insert_update_delete_matches_wal_and_recovery`
//! under three different concurrency conditions, repeated many times each:
//!
//! 1. [`sequential_workload_never_violates_the_split_invariant`] — one
//!    thread, zero concurrency. If the assertion can still fire here, it's a
//!    deterministic logic/arithmetic bug — full stop, the race theory is
//!    wrong.
//! 2. [`serialized_concurrent_workload_never_violates_the_split_invariant`]
//!    — six real threads, six real `WorkerId`s, but every single
//!    `dispatch_crud` call is individually serialized through a shared
//!    mutex, so no two threads' tree mutations can ever be physically in
//!    flight at the same time. If the assertion can still fire here, the
//!    cause is *not* physical overlap on a leaf's memory — it would have to
//!    be something about merely having several workers/threads involved
//!    (e.g. per-worker OSIC state), not a data race in the literal sense.
//! 3. [`concurrent_unsynchronized_workload_reproduces_the_failure`] — the
//!    original, fully unsynchronized pattern, `#[ignore]`d on purpose (it's
//!    *expected* to fail intermittently, so it doesn't belong in the normal
//!    always-green suite) — run explicitly on demand as the baseline this
//!    investigation is comparing against.
//!
//! If (1) and (2) both stay reliably green across many repeated iterations
//! while (3) reliably reproduces the failure, that's about as direct as
//! evidence gets that this specific assertion requires genuine time-overlap
//! between two threads' writes to the same physical leaf.

use std::sync::Mutex;

use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_tree::mvbt::MVBTSt;

const FAN: usize = 8;
type TestTree = MVBTSt<FAN, FAN, u64, u64>;

const THREADS: u64 = 6;
const KEYS_PER_THREAD: u64 = 300;
const ITERATIONS: usize = 30;

/// One "logical thread's" share of the original failing workload: insert,
/// update, then (for even keys) delete, over its own disjoint key range
/// `[t*KEYS_PER_THREAD, (t+1)*KEYS_PER_THREAD)`. No WAL involved — the
/// assertion under investigation is purely about in-memory leaf-page
/// bookkeeping during a structural split, unrelated to durability.
fn run_range(tree: &TestTree, t: u64) {
    for i in 0..KEYS_PER_THREAD {
        let key = t * KEYS_PER_THREAD + i;
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Insert(key, key * 3 + 1)),
            CRUDOperationResult::Inserted(_)
        ));
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Update(key, key * 3 + 2)),
            CRUDOperationResult::Updated(_)
        ));
        if key % 2 == 0 {
            assert!(matches!(
                tree.dispatch_crud(CRUDOperation::Delete(key)),
                CRUDOperationResult::Deleted(_)
            ));
        }
    }
}

/// Same work, but every individual `dispatch_crud` call is wrapped by
/// `lock` first — used by the serialized variant so no two threads' calls
/// can ever be physically in flight against the tree at once, even though
/// six real OS threads with six real `WorkerId`s are still doing the work.
fn run_range_serialized(tree: &TestTree, t: u64, lock: &Mutex<()>) {
    for i in 0..KEYS_PER_THREAD {
        let key = t * KEYS_PER_THREAD + i;
        {
            let _guard = lock.lock().unwrap();
            assert!(matches!(
                tree.dispatch_crud(CRUDOperation::Insert(key, key * 3 + 1)),
                CRUDOperationResult::Inserted(_)
            ));
        }
        {
            let _guard = lock.lock().unwrap();
            assert!(matches!(
                tree.dispatch_crud(CRUDOperation::Update(key, key * 3 + 2)),
                CRUDOperationResult::Updated(_)
            ));
        }
        if key % 2 == 0 {
            let _guard = lock.lock().unwrap();
            assert!(matches!(
                tree.dispatch_crud(CRUDOperation::Delete(key)),
                CRUDOperationResult::Deleted(_)
            ));
        }
    }
}

/// Condition 1: zero concurrency at all. If the split invariant can still
/// be violated here, it's a deterministic bug independent of threading.
#[test]
fn sequential_workload_never_violates_the_split_invariant() {
    for _ in 0..ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        for t in 0..THREADS {
            run_range(&tree, t);
        }
    }
}

/// Condition 2: real threads/workers, but every tree access serialized so
/// no two threads' mutations can ever overlap in wall-clock time. If the
/// invariant can still be violated here, physical overlap on a leaf's
/// memory is not the cause.
#[test]
fn serialized_concurrent_workload_never_violates_the_split_invariant() {
    for _ in 0..ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        let lock = Mutex::new(());
        std::thread::scope(|scope| {
            for t in 0..THREADS {
                let tree = &tree;
                let lock = &lock;
                scope.spawn(move || run_range_serialized(tree, t, lock));
            }
        });
    }
}

/// Condition 3 (diagnostic baseline, intentionally `#[ignore]`d): the
/// original, fully unsynchronized concurrent pattern. Expected to fail
/// intermittently — that's the whole point of this investigation — so it
/// does not belong in the normal always-green suite. Run explicitly:
/// `cargo test --bin cMVBT -- --ignored concurrent_unsynchronized_workload_reproduces_the_failure`
#[test]
// #[ignore]
fn concurrent_unsynchronized_workload_reproduces_the_failure() {
    for _ in 0..ITERATIONS {
        let tree = TestTree::make_standard(RootIndexType::default());
        std::thread::scope(|scope| {
            for t in 0..THREADS {
                let tree = &tree;
                scope.spawn(move || run_range(tree, t));
            }
        });
    }
}
