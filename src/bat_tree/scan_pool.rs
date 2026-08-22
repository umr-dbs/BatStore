//! A tree-owned pool of scan-worker threads, shared by every concurrent
//! caller that dispatches sub-range scan jobs into it — as opposed to one
//! query privately owning `fanout` threads for the duration of its own
//! call. Generic over any `MVBTSt`, so any `bat_db::Database` table can be
//! assigned one (see `Database::enable_scan_pool`) — not just
//! `bat_bench::tpcc_schema::TpccDatabase`'s `ORDER_LINE` table, which is
//! merely this pool's first caller (`bat_bench::parallel_scan`'s
//! `q1_parallel`/`q6_parallel`).
//!
//! ## Why a shared queue, not per-query-exclusive workers
//!
//! A naive design gives one query all `fanout` worker threads for the
//! duration of its own call (one dedicated channel pair per worker,
//! blocking on every one of them) — which only works if exactly one caller
//! ever holds the pool at a time; a `std::sync::mpsc::Receiver` isn't even
//! `Sync`, so it couldn't be shared across concurrent callers as written.
//! Once a pool is owned by a tree/database and reachable from several
//! concurrent queries, that has to become a real shared work queue instead:
//! every worker pulls from one MPMC channel (`crossbeam_channel`, unlike
//! `std::sync::mpsc`, has a genuinely `Clone` + multi-consumer `Receiver`),
//! and each submitted job carries its own private result channel. A
//! caller's `dispatch` therefore only ever blocks on *its own* jobs'
//! results — other callers' jobs submitted in the meantime interleave
//! through the same workers instead of queuing behind one caller's
//! exclusive hold on the whole pool.
//!
//! ## Ownership
//!
//! Threads are spawned detached (`thread::spawn`, each looping on the
//! shared job queue — see `spawn`), not scoped to a `std::thread::Scope`,
//! so the pool can outlive whichever call enabled it — the same
//! `Arc`-clone-instead-of-borrow pattern `bat_db::Database::
//! start_vacuum_thread` already uses for its own background thread, and
//! for the same reason: a scoped thread must be joined before its scope
//! returns, which is incompatible with a pool meant to live for a
//! database's whole run. Dropping a `ScanWorkerPool` drops its one job
//! sender; every worker's blocking `recv()` then returns `Err` once the
//! channel disconnects and the thread exits on its own — no explicit stop
//! flag needed, and (matching `stop_vacuum_thread`'s own doc) nothing
//! here waits for that exit.
//!
//! ## No `WorkerId` cost, so this can freely oversubscribe
//!
//! Every distinct OS thread that ever calls `tree.worker_id()` permanently
//! claims one slot from that tree's fixed, never-growing `WorkerRegistry`
//! (see that struct's doc) — the reason every terminal/OLAP thread this
//! codebase spawns elsewhere has to be budgeted against `max_workers`. This
//! pool's own worker loop (`spawn`, below) never calls it: a job is just an
//! opaque `FnOnce()` this loop runs, and it's on *callers* to build that
//! closure using `bat_sync::worker::READ_ONLY_SCAN_WORKER_ID` instead of a
//! real registered id whenever it might run on one of these threads (see
//! that constant's doc for why that's sound for a pure-reader job — which
//! every job submitted here must be: nothing on this path ever writes).
//! Do that, and a pool's worker count can be sized however large is useful
//! — including past the tree's own `max_workers` — without ever touching,
//! let alone exhausting, that fixed budget.

use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use triomphe::Arc;

use crate::bat_query::interval::{Interval, RangeSplit};
use crate::bat_tree::mvbt::MVBTSt;

type BoxedJob = Box<dyn FnOnce() + Send>;

/// Below this, a "pool" of 1 buys nothing over the sequential path — the
/// one worker just serializes every query behind whatever it's already
/// doing, identical to not having a pool at all — so `spawn` floors every
/// request up to this, the smallest count that can actually do anything in
/// parallel.
const MIN_WORKERS: usize = 2;

pub struct ScanWorkerPool<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> {
    tree: Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    job_tx: crossbeam_channel::Sender<BoxedJob>,
    num_workers: usize,
    /// Jobs a worker has picked up (`recv()`'d) but not yet finished
    /// running — `queue_len()` alone misses these, since a job leaves the
    /// channel the instant a worker receives it, well before it's done.
    /// Combined into `load()`/`has_spare_capacity()` so "is this pool
    /// busy" reflects work actually in flight, not just what's still
    /// waiting in the queue.
    in_flight: std::sync::Arc<AtomicUsize>,
    /// How many distinct callers (e.g. OLAP threads) this pool is expected
    /// to serve at once — set once, at `spawn` time, by whoever enabled the
    /// pool and happens to know that count already (see `fair_query_fanout`'s
    /// doc for how it's used and `TpccDatabase::enable_scan_pool`'s for
    /// where a real caller gets this from). `None` when that count isn't
    /// known.
    expected_concurrent_queries: Option<usize>,
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> ScanWorkerPool<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Spawns `num_workers.max(MIN_WORKERS)` detached threads sharing one
    /// job queue. Keeps its own clone of `tree` for `dispatch`/
    /// `try_dispatch`'s job-building and inline-fallback use.
    /// `expected_concurrent_queries` — see that field's and
    /// `fair_query_fanout`'s docs — is a one-time hint, not something that
    /// can be changed later; a caller whose own concurrency changes over a
    /// run (e.g. `num_olap_threads` gets clamped) should pass the count it
    /// actually ends up running with, not the one it originally asked for.
    pub fn spawn(
        tree: Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>>,
        num_workers: usize,
        expected_concurrent_queries: Option<usize>,
    ) -> Self {
        let num_workers = num_workers.max(MIN_WORKERS);
        let (job_tx, job_rx) = crossbeam_channel::unbounded::<BoxedJob>();
        let in_flight = std::sync::Arc::new(AtomicUsize::new(0));
        for _ in 0..num_workers {
            let job_rx = job_rx.clone();
            let in_flight = in_flight.clone();
            std::thread::spawn(move || {
                while let Ok(job) = job_rx.recv() {
                    in_flight.fetch_add(1, Relaxed);
                    job();
                    in_flight.fetch_sub(1, Relaxed);
                }
            });
        }
        Self { tree, job_tx, num_workers, in_flight, expected_concurrent_queries }
    }

    pub fn num_workers(&self) -> usize {
        self.num_workers
    }

    /// How many workers one query should ask for: this pool's total
    /// capacity divided evenly across however many callers
    /// (`expected_concurrent_queries`) are expected to be querying it at
    /// once, rather than every query grabbing the same fixed slice
    /// regardless of how crowded the pool actually gets — so with, say, 4
    /// concurrent OLAP threads sharing a 32-worker pool, each asks for 8;
    /// with 64 sharing it, each asks for 2; with 128 (more callers than the
    /// pool can meaningfully divide among), `None` — see below.
    ///
    /// Returns `None` when `expected_concurrent_queries` is known but the
    /// fair share would come out below 2 (too many expected callers for
    /// this pool to divide meaningfully) — the caller should skip the pool
    /// entirely for that query and just scan the whole range directly on
    /// its own thread, since paying dispatch overhead for a single job
    /// with no parallelism to show for it is worse than not bothering (see
    /// `parallel_scan::MIN_ROWS_FOR_SCAN_POOL`'s doc for the same reasoning
    /// applied to scan *size* instead of concurrency).
    ///
    /// `expected_concurrent_queries` unknown (`None`/`Some(0)`): `Some(2)`
    /// — the smallest width that's still genuinely parallel (matches this
    /// pool's own `MIN_WORKERS` floor), used as a conservative default
    /// when there's no better information to divide by.
    pub fn fair_query_fanout(&self) -> Option<usize> {
        match self.expected_concurrent_queries {
            None | Some(0) => Some(2),
            Some(expected_queries) => {
                let share = self.num_workers / expected_queries;
                (share >= 2).then_some(share)
            }
        }
    }

    /// Jobs already submitted but not yet picked up by any worker — see
    /// `load`/`has_spare_capacity` for the "is this pool busy" check that
    /// actually matters to callers.
    pub fn queue_len(&self) -> usize {
        self.job_tx.len()
    }

    /// Total outstanding demand on this pool right now: jobs a worker is
    /// actively running plus jobs still waiting in the queue behind them.
    pub fn load(&self) -> usize {
        self.in_flight.load(Relaxed) + self.queue_len()
    }

    /// Whether submitting more work right now would have to wait rather
    /// than start immediately — i.e. every worker is already occupied and/or
    /// the queue already has a backlog at least as large as the pool
    /// itself. Used by `try_dispatch` to decide whether to enqueue at all.
    pub fn has_spare_capacity(&self) -> bool {
        self.load() < self.num_workers
    }

    /// Submits one job per `range`, running `make_job(&tree, range)` on
    /// whichever worker picks it up, and collects every result in `ranges`'
    /// order — blocking only on *this call's own* jobs (see module doc),
    /// not on the pool as a whole.
    pub fn dispatch<R: Send + 'static>(
        &self,
        ranges: Vec<Interval<Key>>,
        make_job: impl Fn(&MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>, Interval<Key>) -> R + Send + Sync + 'static,
    ) -> Vec<R> {
        let make_job = std::sync::Arc::new(make_job);
        let result_rxs: Vec<_> = ranges
            .into_iter()
            .map(|range| {
                let tree = self.tree.clone();
                let make_job = make_job.clone();
                let (result_tx, result_rx) = crossbeam_channel::bounded(1);
                let job: BoxedJob = Box::new(move || {
                    let _ = result_tx.send(make_job(&tree, range));
                });
                self.job_tx.send(job).expect("ScanWorkerPool: every worker thread has exited");
                result_rx
            })
            .collect();

        result_rxs
            .into_iter()
            .map(|rx| rx.recv().expect("ScanWorkerPool: worker died before reporting a result"))
            .collect()
    }

    /// Same as `dispatch`, except when the pool has no spare capacity right
    /// now (`has_spare_capacity` is `false`): runs every range directly on
    /// the calling thread instead of enqueueing anything, so this call
    /// never blocks waiting behind other concurrent callers' work — it just
    /// degrades to sequential execution on the caller's own thread instead
    /// of waiting in line for the pool to free up.
    pub fn try_dispatch<R: Send + 'static>(
        &self,
        ranges: Vec<Interval<Key>>,
        make_job: impl Fn(&MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>, Interval<Key>) -> R + Send + Sync + 'static,
    ) -> Vec<R> {
        if self.has_spare_capacity() {
            self.dispatch(ranges, make_job)
        } else {
            ranges.into_iter().map(|range| make_job(&self.tree, range)).collect()
        }
    }

    /// Dispatches `reducer` across `partition(fanout)`'s ranges — this
    /// query's fair share of the pool — falling back to running `reducer`
    /// once over the whole `full_range`, on the calling thread, when no fair
    /// share is available right now (`fair_query_fanout` returning `None`,
    /// same reasoning as `dispatch_evenly`'s identical fallback). Unlike
    /// `dispatch_evenly`, this needs no `RangeSplit` bound on `Key`: it's for
    /// a caller that already has (or needs) its own domain-aware partitioner
    /// — e.g. `bat_bench::parallel_scan::partition_order_line_range`, for a
    /// bit-packed key `RangeSplit`'s generic numeric bisection isn't safe
    /// over (see that trait's doc). Centralizes the "ask for a fair share,
    /// dispatch across it, or just run inline" decision so a caller with its
    /// own partitioner doesn't have to re-derive that control flow itself —
    /// see `iter_query::RangeQueryIter`'s own `*_parallel` methods for the
    /// equivalent, `RangeSplit`-based version of the same idea.
    pub fn dispatch_by_fair_share<R: Send + 'static>(
        &self,
        full_range: Interval<Key>,
        partition: impl FnOnce(usize) -> Vec<Interval<Key>>,
        reducer: impl Fn(&MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>, Interval<Key>) -> R + Send + Sync + 'static,
    ) -> Vec<R> {
        match self.fair_query_fanout() {
            Some(fanout) => self.try_dispatch(partition(fanout), reducer),
            None => vec![reducer(&self.tree, full_range)],
        }
    }
}

/// A separate `impl` block, bounded by `Key: RangeSplit` — unlike every
/// other method above, `dispatch_evenly` needs to actually split a range
/// itself rather than being handed pre-split ones, so it's only available
/// for key types that opted into `RangeSplit` (see that trait's doc for
/// why this is opt-in rather than automatic for any `Key`). This keeps
/// that requirement local to this one convenience method instead of
/// forcing every `Key` this whole module is ever used with — including
/// ones a caller partitions by hand, like `bat_bench::parallel_scan`'s
/// `ORDER_LINE`-specific `partition_order_line_range` already does — to
/// satisfy a bound they don't need.
impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + RangeSplit + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> ScanWorkerPool<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// The generic, no-partitioner-required entry point: splits `range`
    /// into this query's fair share of the pool (`fair_query_fanout`) via
    /// `Key::split_evenly`, then dispatches exactly like `try_dispatch`.
    /// For any caller that doesn't have (or need) a domain-aware
    /// partitioner of its own — most straightforward, non-bit-packed key
    /// spaces — this is a complete "just run my scan through the pool"
    /// call, no `partition_order_line_range`-style helper required.
    ///
    /// Returns `None` if either this pool has no fair share to offer right
    /// now (`fair_query_fanout`) or `Key::split_evenly` can't divide
    /// `range` at all (including a `Key` type that never opted into real
    /// splitting, per `RangeSplit`'s default) — in both cases the caller
    /// should just scan `range` sequentially itself instead. Also `None`
    /// if `Key::approx_len(range)` reports fewer than
    /// `MIN_LEN_FOR_SPLIT_DISPATCH` values — parallelizing a scan that
    /// small would spend more on channel/oneshot overhead than it could
    /// possibly save (an unknown length, `approx_len` returning `None`, is
    /// *not* treated as "too small" — see that method's doc).
    ///
    /// Split out as [`Self::evenly_split_ranges`] so a caller that needs to
    /// know *whether* this would dispatch before committing to build (or
    /// move) a reducer closure — e.g. `RangeQueryIter`'s own `*_parallel`
    /// methods, which fall back to consuming `self` sequentially rather
    /// than through this call — can check that first instead of losing the
    /// closure it already handed over on a `None` outcome.
    pub fn dispatch_evenly<R: Send + 'static>(
        &self,
        range: Interval<Key>,
        reducer: impl Fn(&MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>, Interval<Key>) -> R + Send + Sync + 'static,
    ) -> Option<Vec<R>> {
        let ranges = self.evenly_split_ranges(range)?;
        Some(self.try_dispatch(ranges, reducer))
    }

    /// The sub-ranges `dispatch_evenly` would dispatch `range` across right
    /// now, or `None` for exactly the reasons documented on that method —
    /// without needing a reducer at all.
    pub fn evenly_split_ranges(&self, range: Interval<Key>) -> Option<Vec<Interval<Key>>> {
        if Key::approx_len(range).is_some_and(|len| len < MIN_LEN_FOR_SPLIT_DISPATCH) {
            return None;
        }
        let fanout = self.fair_query_fanout()?;
        Key::split_evenly(range, fanout)
    }
}

/// Below this many values (per `RangeSplit::approx_len`), `dispatch_evenly`
/// skips the pool entirely and reports `None` — the same measurement and
/// reasoning as `bat_bench::parallel_scan::MIN_ROWS_FOR_SCAN_POOL` (that
/// constant gates a *different* decision, whether to enable a whole pool
/// for a run at all, based on estimated population; this one gates a
/// single call's own range, based on what it actually covers), kept as its
/// own constant here since it's this module's generic entry point that
/// needs it, not anything TPC-C-specific.
pub const MIN_LEN_FOR_SPLIT_DISPATCH: u64 = 65_536;
