//! Fixed-size, long-lived worker pool for fanning a single analytical scan
//! (CH-benCHmark [`q1_parallel`]/[`q6_parallel`]) out across several threads
//! at once, instead of one OLAP thread walking the whole `ORDER_LINE` table
//! alone. Follow-up to `docs/bigtree_size_benchmark.md`'s and
//! `docs/range_scan_visibility_check.md`'s OLAP-performance passes: a
//! per-table `bat_test::SCAN_TRACE` breakdown on `htap_q1` showed
//! `ORDER_LINE`'s own visited/matched ratio is a modest ~1.65x (nowhere near
//! `Warehouse`/`District`'s garbage-heavy 5-6x under `BigTreeSize`) — so the
//! bottleneck for these two queries specifically is the sheer volume of live
//! data one thread has to touch per full-table pass, not garbage. That's a
//! data-parallel problem, not a compaction problem.
//!
//! ## Threading constraint
//!
//! Every distinct OS thread that ever calls into a tree permanently consumes
//! one `WorkerId` slot from that tree's fixed pool (sized to `max_workers`
//! at construction, never grows — see `tpcc_driver.rs`'s module doc). So
//! this pool's worker threads must be spawned exactly once per OLAP thread's
//! whole run and reused for every `q1_parallel`/`q6_parallel` call — never a
//! fresh `thread::spawn` per query, which would mint a never-before-seen OS
//! thread (and therefore a fresh permanent `WorkerId`) on every single call
//! and exhaust the pool within seconds under a tight OLAP loop.
//!
//! [`ScanWorkerPool::spawn`] therefore takes a [`std::thread::Scope`] and
//! spawns genuinely scoped threads borrowing `&TpccDatabase` directly — no
//! `Arc` cloning, and `std::thread::scope` itself guarantees every one of
//! them is joined (and any panic propagated) before the scope block
//! returns, so there is no separate manual join/shutdown path to get wrong.
//!
//! ## Partitioning
//!
//! `ORDER_LINE`'s key packs `w_id` in its most-significant bits
//! (`tpcc_schema::k_order_line`), so the whole-table range splits into
//! `num_warehouses` disjoint, contiguous sub-intervals by pure bit
//! arithmetic — no tree traversal needed to find the split points. Each of
//! the pool's `fanout` workers gets a contiguous, near-equal block of
//! warehouses (the last block absorbing any remainder); a worker with no
//! warehouses left (`fanout > num_warehouses`) gets a genuinely empty
//! interval and returns instantly, rather than being skipped — every
//! dispatch always sends and receives exactly `fanout` jobs.

use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::Scope;

use crate::bat_bench::tpcc_schema::{Table, TpccDatabase, TpccKey, k_order_line, order_line_table_range};
use crate::bat_bench::tpcc_txn::TpccTxn;
use crate::bat_bench::tpch_queries::OrderLineSummary;
use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_record_model::version_info::Version;

enum Job {
    Q1 {
        range: Interval<TpccKey>,
        ts_start: Version,
        delivered_before: i64,
    },
    Q6 {
        range: Interval<TpccKey>,
        ts_start: Version,
        date_lo: i64,
        date_hi: i64,
        max_qty: u8,
    },
}

enum JobResult {
    Q1(Box<[OrderLineSummary; 16]>),
    Q6(f64),
}

fn empty_groups() -> [OrderLineSummary; 16] {
    std::array::from_fn(|i| OrderLineSummary {
        ol_number: i as u8,
        ..Default::default()
    })
}

/// One worker's share of one query — same per-record logic as the
/// sequential `tpch_queries::q1`/`q6`, just over a sub-`range` instead of
/// the whole table, and with this thread's own `WorkerId` (`db.db.worker_id()`,
/// assigned automatically on first use — see `bat_sync::worker::worker_id_for`)
/// rather than a `TpccTxn`'s. `register_reader_si: false`: this thread
/// doesn't own the snapshot — the caller's own `TpccTxn` (still alive for
/// the whole dispatch, see `ScanWorkerPool::q1`/`q6`) already registered and
/// is protecting `ts_start` from GC, exactly the same trust relationship
/// `TpccTxn::range_for_each` already relies on for its own non-owning scans.
fn run_job(db: &TpccDatabase, job: Job) -> JobResult {
    let tree = db.tree_for(Table::OrderLine);
    let worker_id = db.db.worker_id();
    match job {
        Job::Q1 {
            range,
            ts_start,
            delivered_before,
        } => {
            let mut groups = empty_groups();
            RangeQueryIter::new(&tree, ts_start, range, false, worker_id).for_each_ref(|key, row| {
                let ol = row.as_order_line();
                let Some(delivered) = ol.ol_delivery_d else {
                    return;
                };
                if delivered > delivered_before {
                    return;
                }
                let g = &mut groups
                    [crate::bat_bench::tpcc_schema::decode_order_line_number(key) as usize];
                g.count += 1;
                g.sum_qty += ol.ol_quantity as u64;
                g.sum_amount += ol.ol_amount;
            });
            JobResult::Q1(Box::new(groups))
        }
        Job::Q6 {
            range,
            ts_start,
            date_lo,
            date_hi,
            max_qty,
        } => {
            let mut revenue = 0.0;
            RangeQueryIter::new(&tree, ts_start, range, false, worker_id).for_each_ref(|_, row| {
                let ol = row.as_order_line();
                if ol.ol_delivery_d.is_some_and(|delivered| {
                    delivered >= date_lo && delivered < date_hi && ol.ol_quantity < max_qty
                }) {
                    revenue += ol.ol_amount;
                }
            });
            JobResult::Q6(revenue)
        }
    }
}

/// Splits `order_line_table_range()`'s full domain into exactly `fanout`
/// contiguous, disjoint sub-ranges by warehouse id (1-indexed, see
/// `tpcc_driver.rs`'s `1..=num_warehouses` population loop) — see this
/// module's doc for why that's a safe, traversal-free split axis. The first
/// and last blocks use the table's own `TpccKey::MIN`/`MAX` sentinels rather
/// than a computed bound, so the partition can never leave out a
/// differently-encoded edge row even if the key layout ever changes.
///
/// Requires `num_warehouses >= 1` (already asserted by `tpcc_driver::run_tpcc`
/// for every real caller) — at `0` every block's `count` is `0`, so every
/// worker would get the "no warehouses left" empty interval and the scan
/// would wrongly see zero rows instead of falling back to the full range.
fn partition_order_line_range(num_warehouses: u32, fanout: usize) -> Vec<Interval<TpccKey>> {
    let fanout = fanout.max(1);
    let n = num_warehouses as usize;
    let base = n / fanout;
    let rem = n % fanout;

    let full = order_line_table_range();
    let mut ranges = Vec::with_capacity(fanout);
    let mut next_w_id = 1u32;
    for i in 0..fanout {
        let count = base + if i < rem { 1 } else { 0 };
        if count == 0 {
            // No warehouses left for this worker — a genuinely empty,
            // inverted interval (`lower > upper`) rather than skipping it,
            // so every dispatch still sends/receives exactly `fanout` jobs.
            // `RangeQueryIter`/`try_for_each_ref` both already treat
            // `lower > upper` as "immediately exhausted" (see
            // `iter_query.rs::refill`), so this costs nothing.
            ranges.push(Interval::new(full.upper, full.lower));
            continue;
        }
        let lo_w = next_w_id;
        let hi_w = next_w_id + count as u32 - 1;
        next_w_id = hi_w + 1;

        let lower = if i == 0 {
            full.lower
        } else {
            k_order_line(lo_w, 0, 0, 0)
        };
        let upper = if i == fanout - 1 {
            full.upper
        } else {
            k_order_line(hi_w + 1, 0, 0, 0) - 1
        };
        ranges.push(Interval::new(lower, upper));
    }
    ranges
}

pub struct ScanWorkerPool {
    job_txs: Vec<Sender<Job>>,
    result_rxs: Vec<Receiver<JobResult>>,
}

impl ScanWorkerPool {
    /// Spawns `fanout` scoped worker threads, borrowing `db` directly for
    /// as long as `scope` lives (the calling OLAP thread's whole run — see
    /// `olap_scan::run_olap_worker`). Spawned exactly once per OLAP thread,
    /// not per query — see this module's threading-constraint doc.
    pub fn spawn<'scope>(scope: &'scope Scope<'scope, '_>, db: &'scope TpccDatabase, fanout: usize) -> Self {
        let fanout = fanout.max(1);
        let mut job_txs = Vec::with_capacity(fanout);
        let mut result_rxs = Vec::with_capacity(fanout);
        for _ in 0..fanout {
            let (job_tx, job_rx) = channel::<Job>();
            let (result_tx, result_rx) = channel::<JobResult>();
            scope.spawn(move || {
                while let Ok(job) = job_rx.recv() {
                    if result_tx.send(run_job(db, job)).is_err() {
                        break; // caller dropped its result_rx — pool is shutting down
                    }
                }
            });
            job_txs.push(job_tx);
            result_rxs.push(result_rx);
        }
        Self { job_txs, result_rxs }
    }

    pub fn fanout(&self) -> usize {
        self.job_txs.len()
    }

    fn dispatch<F: Fn(Interval<TpccKey>) -> Job>(&self, num_warehouses: u32, make_job: F) -> Vec<JobResult> {
        let ranges = partition_order_line_range(num_warehouses, self.fanout());
        for (tx, range) in self.job_txs.iter().zip(ranges) {
            tx.send(make_job(range)).expect("scan worker thread died");
        }
        self.result_rxs
            .iter()
            .map(|rx| rx.recv().expect("scan worker thread died"))
            .collect()
    }

    /// CH-benCHmark Q1, fanned out across this pool's workers. The caller
    /// must already hold `ts_start` registered/protected via its own,
    /// still-open `TpccTxn` for the whole duration of this call — see
    /// [`q1_parallel`].
    fn q1(&self, num_warehouses: u32, ts_start: Version, delivered_before: i64) -> [OrderLineSummary; 16] {
        let mut total = empty_groups();
        for result in self.dispatch(num_warehouses, |range| Job::Q1 {
            range,
            ts_start,
            delivered_before,
        }) {
            let JobResult::Q1(partial) = result else {
                unreachable!("q1() only ever dispatches Job::Q1")
            };
            for i in 0..16 {
                total[i].count += partial[i].count;
                total[i].sum_qty += partial[i].sum_qty;
                total[i].sum_amount += partial[i].sum_amount;
            }
        }
        total
    }

    /// CH-benCHmark Q6, fanned out across this pool's workers. Same
    /// snapshot-protection precondition as [`Self::q1`].
    fn q6(&self, num_warehouses: u32, ts_start: Version, date_lo: i64, date_hi: i64, max_qty: u8) -> f64 {
        self.dispatch(num_warehouses, |range| Job::Q6 {
            range,
            ts_start,
            date_lo,
            date_hi,
            max_qty,
        })
        .into_iter()
        .map(|result| {
            let JobResult::Q6(revenue) = result else {
                unreachable!("q6() only ever dispatches Job::Q6")
            };
            revenue
        })
        .sum()
    }
}

/// Parallel drop-in replacement for `tpch_queries::q1`, splitting the scan
/// across `pool` instead of running it on the calling thread alone. Opens
/// its own `TpccTxn` exactly like the sequential version so `ts_start` stays
/// registered (and therefore GC-protected) for `pool.q1`'s whole blocking
/// call — the pool's own workers scan with `register_reader_si: false`,
/// relying entirely on this transaction's registration (see `run_job`'s doc).
pub fn q1_parallel(
    db: &TpccDatabase,
    pool: &ScanWorkerPool,
    num_warehouses: u32,
    delivered_before: i64,
) -> (Vec<OrderLineSummary>, Version) {
    let tx = TpccTxn::begin(db);
    let ts_start = tx.ts_start();
    let groups = pool.q1(num_warehouses, ts_start, delivered_before);
    tx.commit();

    let mut out: Vec<_> = groups.into_iter().filter(|g| g.count > 0).collect();
    out.sort_by_key(|g| g.ol_number);
    (out, ts_start)
}

/// Parallel drop-in replacement for `tpch_queries::q6` — see [`q1_parallel`]'s doc.
pub fn q6_parallel(
    db: &TpccDatabase,
    pool: &ScanWorkerPool,
    num_warehouses: u32,
    date_lo: i64,
    date_hi: i64,
    max_qty: u8,
) -> (f64, Version) {
    let tx = TpccTxn::begin(db);
    let ts_start = tx.ts_start();
    let revenue = pool.q6(num_warehouses, ts_start, date_lo, date_hi, max_qty);
    tx.commit();
    (revenue, ts_start)
}
