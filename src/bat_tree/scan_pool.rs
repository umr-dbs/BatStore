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
//! enable_idle_compaction` already uses for its own background thread, and
//! for the same reason: a scoped thread must be joined before its scope
//! returns, which is incompatible with a pool meant to live for a
//! database's whole run. Dropping a `ScanWorkerPool` drops its one job
//! sender; every worker's blocking `recv()` then returns `Err` once the
//! channel disconnects and the thread exits on its own — no explicit stop
//! flag needed, and (matching `disable_idle_compaction`'s own doc) nothing
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

use crate::bat_query::interval::Interval;
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
    pub fn spawn(tree: Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>>, num_workers: usize) -> Self {
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
        Self { tree, job_tx, num_workers, in_flight }
    }

    pub fn num_workers(&self) -> usize {
        self.num_workers
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
}
