use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use crate::mv_record_model::tx_stamp::WorkerId;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::visibility::SnapshotCache;

/// A tree's fixed, bounded pool of OSIC workers (§3.1): each worker gets its
/// own `CommitLog`/`SnapshotCache` slot, so the pool size directly bounds the
/// memory OSIC needs and the cost of a commit-log prune. Workers are handed
/// out once per (tree, thread) and never returned — matching the paper's
/// model of a fixed set of long-lived worker threads, one per core, rather
/// than one per arbitrary short-lived task.
pub struct WorkerRegistry {
    /// A process-wide unique id for the tree this registry belongs to — see
    /// `worker_id_for` for why this, and not the tree's address, is what the
    /// per-thread cache must be keyed by.
    db_uid: u64,
    max_workers: usize,
    next: AtomicUsize,
}

static NEXT_DB_UID: AtomicU64 = AtomicU64::new(0);

impl WorkerRegistry {
    pub fn new(max_workers: usize) -> Self {
        Self {
            db_uid: NEXT_DB_UID.fetch_add(1, Relaxed),
            max_workers,
            next: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    fn acquire(&self) -> WorkerId {
        let id = self.next.fetch_add(1, Relaxed);
        assert!(
            id < self.max_workers,
            "WorkerRegistry: exceeded max_workers ({}) — increase max_workers on tree construction",
            self.max_workers
        );
        id as WorkerId
    }
}

// Both caches below keep the two most recently used trees inline per thread:
// every real deployment of this tree (main
// benchmark harnesses — `main_load`, TPC-C, YCSB — and any production use)
// creates exactly one tree that lives for the whole process, so "does this
// call's tree match the one we already have cached" is a single comparison,
// not a scan. A thread that touches a *different* tree next (the only way
// this actually happens today: `cargo test`'s thread pool reusing one OS
// thread across many single-tree `#[test]`s, never interleaved — each test
// builds its own tree, uses it to completion, and moves on) just overwrites
// the one slot — correct either way, since `tree_uid` is a process-wide
// unique, never-reused id (see below); it only costs that thread one fresh
// `WorkerRegistry::acquire()`/`SnapshotCache::new()` the first time it comes
// back to a tree it had evicted, exactly like a genuinely new thread's first
// touch. A prior version of this cache was a 16-slot searchable array sized
// for the `#[test]`-reuse case — that's solving a test-harness inconvenience
// with production-hot-path complexity. Two MRU slots handle the realistic
// alternating-tree case while keeping the common primary hit to one comparison;
// a lazy overflow vector preserves identities/state for rarer additional trees.

thread_local! {
    // Keyed by each tree's process-wide unique `tree_uid`, *not* its address:
    // a dropped tree's stack/heap slot can be reused by an unrelated later
    // tree, and a thread that cached a WorkerId for the old occupant would
    // otherwise silently (and wrongly) reuse it for the new one — bypassing
    // that new tree's own `WorkerRegistry` counter entirely and risking two
    // different threads believing they own the same WorkerId. A `tree_uid`
    // is never reused, so this can't happen. (This also avoids the old
    // process-global `clock::THREAD_ID`/`STATE`'s cross-tree contamination bug.)
    //
    // Plain `Cell`, not `RefCell`: `(u64, WorkerId)` is `Copy`, so reading
    // and writing it is a bare load/store — no runtime borrow-check needed
    // at all. (`RefCell` would only earn its keep if we needed to hand out
    // `&mut` into the cell's *contents* while still holding a `&` to the
    // cell itself, the way `SNAPSHOT_CACHES` below does; a `Copy` value never
    // needs that, `get`/`set` round-trip it by value instead.) `u64::MAX`
    // marks an empty slot: real `tree_uid`s come from a `fetch_add` counter
    // starting at 0, so this value is unreachable as a genuine one.
    static WORKER_CACHE: Cell<(u64, WorkerId, u64, WorkerId)> =
        Cell::new((u64::MAX, 0, u64::MAX, 0));

    // Cold-path storage for threads that touch more than two databases.
    // Keeping it separate preserves WORKER_CACHE's single-comparison primary
    // hit and avoids constructing or borrowing a Vec in the normal case.
    static WORKER_CACHE_OVERFLOW: SafeCell<Vec<(u64, WorkerId)>> =
        SafeCell::new(Vec::new());
}

/// Returns this thread's `WorkerId` for `registry`'s tree, assigning one on
/// first use. The two-entry MRU avoids consuming another permanent registry
/// slot when a thread alternates between two trees. A third distinct tree
/// evicts the least-recently-used entry and draws a fresh `WorkerId` if that
/// tree is encountered again later. Such entries are retained in a lazily
/// populated overflow vector so a registry slot is never acquired twice by
/// the same thread/database pair.
pub(crate) fn worker_id_for(registry: &WorkerRegistry) -> WorkerId {
    WORKER_CACHE.with(|cache| {
        let mut slot = cache.get();
        if slot.0 == registry.db_uid {
            return slot.1;
        }

        if slot.2 == registry.db_uid {
            std::mem::swap(&mut slot.0, &mut slot.2);
            std::mem::swap(&mut slot.1, &mut slot.3);
        } else {
            let requested = WORKER_CACHE_OVERFLOW.with(|overflow| {
                let overflow = overflow.get_mut();
                if let Some(index) = overflow.iter().position(|entry| entry.0 == registry.db_uid) {
                    let requested = overflow[index];
                    overflow[index] = (slot.2, slot.3);
                    requested
                } else {
                    if slot.2 != u64::MAX {
                        overflow.push((slot.2, slot.3));
                    }
                    (registry.db_uid, registry.acquire())
                }
            });
            slot.2 = slot.0;
            slot.3 = slot.1;
            slot.0 = requested.0;
            slot.1 = requested.1;
        }
        cache.set(slot);
        slot.1
    })
}

thread_local! {
    // Same keying rationale (and same two-slot-over-searchable-collection
    // reasoning) as `WORKER_CACHE` above — see the module-level doc. Kept as
    // a separate cell (rather than folded into `WORKER_CACHE`) since it
    // holds a whole `SnapshotCache`, not just a `u16` id.
    //
    // `SafeCell`, not `RefCell`: a thread-local is by construction only ever
    // touched by the one thread that owns it — there is no real concurrent
    // aliasing to guard against here, only Rust's `thread_local!::with()`
    // API always handing out `&T`, never `&mut T`, even to that sole owner.
    // `RefCell`'s runtime borrow-check exists for a *different* hazard this
    // module doesn't have: reentrant `.with()` calls on the same thread
    // aliasing a live `&mut` — checked here by inspection instead, the same
    // way the rest of this crate already trusts `SafeCell` for its
    // concurrency-critical block/node access (see `mv_sync::safe_cell`):
    // `with_snapshot_cache`'s only two callers (`tx_context.rs`'s
    // `is_visible_stamp`/`with_snapshot_cache_and_logs`) pass a closure that
    // never calls back into `with_snapshot_cache` (or `worker_id_for`)
    // itself.
    // Two-entry MRU: the normal hot path still performs exactly the same one
    // primary-UID comparison as before. Only a primary miss checks the second
    // slot, avoiding allocation churn when one thread alternates between two
    // databases without adding work to the overwhelmingly common hit path.
    static SNAPSHOT_CACHE: SafeCell<(u64, SnapshotCache, u64, SnapshotCache)> =
        SafeCell::new((u64::MAX, SnapshotCache::new(0), u64::MAX, SnapshotCache::new(0)));

    // As above, this is never accessed on a primary or secondary inline hit.
    static SNAPSHOT_CACHE_OVERFLOW: SafeCell<Vec<(u64, SnapshotCache)>> =
        SafeCell::new(Vec::new());
}

/// Gives `f` this thread's own `SnapshotCache` for `registry`'s tree,
/// creating it (sized to `max_workers`) on first use. This is the paper's
/// "thread-local snapshot cache" (Listing 1) literally: unlike a `WorkerId`,
/// which is just a small `Copy` value, the cache is a real piece of mutable
/// per-worker state — keeping it in thread-local storage rather than in a
/// `Vec` on the tree indexed by `WorkerId` means no thread ever *could*
/// (accidentally or otherwise) touch another worker's cache, instead of
/// merely relying on every call site happening to pass its own id.
///
/// More than two databases spill into lazy cold-path storage, following the
/// same MRU policy as `worker_id_for`. This retains warmed LCB entries without
/// putting a collection access on the normal one-database path.
pub(crate) fn with_snapshot_cache<R>(registry: &WorkerRegistry, f: impl FnOnce(&mut SnapshotCache) -> R) -> R {
    SNAPSHOT_CACHE.with(|cache| {
        let slot = cache.get_mut();

        if slot.0 != registry.db_uid {
            if slot.2 == registry.db_uid {
                std::mem::swap(&mut slot.0, &mut slot.2);
                std::mem::swap(&mut slot.1, &mut slot.3);
            } else {
                let requested = SNAPSHOT_CACHE_OVERFLOW.with(|overflow| {
                    let overflow = overflow.get_mut();
                    if let Some(index) = overflow.iter().position(|entry| entry.0 == registry.db_uid) {
                        let requested = overflow.swap_remove(index);
                        if slot.2 != u64::MAX {
                            overflow.push((slot.2, std::mem::replace(&mut slot.3, SnapshotCache::new(0))));
                        }
                        requested
                    } else {
                        if slot.2 != u64::MAX {
                            overflow.push((slot.2, std::mem::replace(&mut slot.3, SnapshotCache::new(0))));
                        }
                        (registry.db_uid, SnapshotCache::new(registry.max_workers()))
                    }
                });
                slot.2 = slot.0;
                slot.3 = std::mem::replace(&mut slot.1, requested.1);
                slot.0 = requested.0;
            }
        }

        f(&mut slot.1)
    })
}
