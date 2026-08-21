//! Query-specific fanout for CH-benCHmark [`q1_parallel`]/[`q6_parallel`],
//! built on top of `bat_tree::scan_pool::ScanWorkerPool` — a fanned-out scan
//! over `ORDER_LINE` instead of one thread walking the whole table alone.
//! Follow-up to `docs/bigtree_size_benchmark.md`'s and
//! `docs/range_scan_visibility_check.md`'s OLAP-performance passes: a
//! per-table `bat_test::SCAN_TRACE` breakdown on `htap_q1` showed
//! `ORDER_LINE`'s own visited/matched ratio is a modest ~1.65x (nowhere near
//! `Warehouse`/`District`'s garbage-heavy 5-6x under `BigTreeSize`) — so the
//! bottleneck for these two queries specifically is the sheer volume of live
//! data one thread has to touch per full-table pass, not garbage. That's a
//! data-parallel problem, not a compaction problem.
//!
//! This module owns only the parts that are specific to `q1`/`q6`: how to
//! split `ORDER_LINE`'s domain into sub-ranges (`partition_order_line_range`)
//! and the per-record reducer each sub-range job runs. The pool itself —
//! spawning/owning worker threads, queuing jobs, collecting results — is a
//! generic `bat_db::Database` feature (any table on any `Database`, not
//! just `TpccDatabase`'s `ORDER_LINE`, can have one assigned), reached here
//! through `TpccDatabase::enable_scan_pool`'s thin, `Table`-keyed wrapper
//! over `bat_db::Database::enable_scan_pool`. So the pool can be shared by
//! whichever concurrent queries want to use it, not spawned fresh per OLAP
//! thread or per query — see `bat_tree::scan_pool::ScanWorkerPool`'s doc
//! for why it's a shared queue rather than one query's own dedicated
//! worker set.
//!
//! ## Partitioning
//!
//! `ORDER_LINE`'s key packs `w_id` in its most-significant bits
//! (`tpcc_schema::k_order_line`), so the whole-table range splits into
//! `num_warehouses` disjoint, contiguous sub-intervals by pure bit
//! arithmetic — no tree traversal needed to find the split points. Each
//! call always splits into exactly `QUERY_FANOUT` (or `pool.num_workers()`,
//! whichever is smaller) contiguous, near-equal shares of warehouses (the
//! last block absorbing any remainder); a worker with no warehouses left
//! gets a genuinely empty interval and returns instantly, rather than
//! being skipped.
//!
//! `QUERY_FANOUT` is deliberately *not* `pool.num_workers()`: a query that
//! always requested the pool's *entire* capacity would leave nothing for
//! any other concurrently-querying OLAP thread to grab — the first query
//! to dispatch would occupy every worker, and every other one would see
//! `has_spare_capacity() == false` and fall back to running inline for as
//! long as the first query's jobs are in flight, defeating the whole point
//! of a *shared* pool. Asking for a small, fixed slice per query instead
//! means several queries' slices can fit in the pool at once — see
//! `bat_tree::scan_pool::ScanWorkerPool`'s doc for the "shared queue, not
//! per-query-exclusive workers" rationale this completes. `q1_parallel`/
//! `q6_parallel` go through `ScanWorkerPool::try_dispatch`, not `dispatch`,
//! so a pool with no spare slice available right now runs every one of a
//! query's blocks on the calling thread instead of queuing behind other
//! callers — see that method's doc.

use crate::bat_bench::tpcc_schema::{
    TpccDatabase, TpccKey, TpccScanWorkerPool as ScanWorkerPool, TpccTree, k_order_line, order_line_table_range,
};
use crate::bat_bench::tpcc_txn::TpccTxn;
use crate::bat_bench::tpch_queries::OrderLineSummary;
use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_record_model::version_info::Version;
use crate::bat_sync::worker::READ_ONLY_SCAN_WORKER_ID;

/// How many sub-ranges one `q1_parallel`/`q6_parallel` call asks the pool
/// for — see this module's "Partitioning" doc for why this is a small fixed
/// number rather than `pool.num_workers()`. `2` is the smallest width that's
/// still genuinely parallel (matches `ScanWorkerPool::spawn`'s own 2-worker
/// floor), so several concurrent queries' slices can coexist in a pool
/// sized a couple of workers per OLAP thread without any of them starving
/// for capacity.
const QUERY_FANOUT: usize = 2;

fn empty_groups() -> [OrderLineSummary; 16] {
    std::array::from_fn(|i| OrderLineSummary {
        ol_number: i as u8,
        ..Default::default()
    })
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

/// Parallel drop-in replacement for `tpch_queries::q1`, splitting the scan
/// across `pool` instead of running it on the calling thread alone. Opens
/// its own `TpccTxn` exactly like the sequential version so `ts_start` stays
/// registered (and therefore GC-protected) for the whole dispatch — each
/// sub-range job scans with `register_reader_si: false`, relying entirely
/// on this transaction's registration, exactly the same trust relationship
/// `TpccTxn::range_for_each` already relies on for its own non-owning
/// scans. Uses `READ_ONLY_SCAN_WORKER_ID` rather than this thread's real
/// `WorkerId` for the same reason `ScanWorkerPool`'s worker threads do (see
/// that constant's doc): a job here might run on one of the pool's own
/// worker threads (which never register a `WorkerId` at all) or, via
/// `try_dispatch`'s busy fallback, inline on this call's own already-
/// registered OLAP thread — either way the sentinel is correct, so the
/// closure doesn't need to know or care which.
pub fn q1_parallel(
    db: &TpccDatabase,
    pool: &ScanWorkerPool,
    num_warehouses: u32,
    delivered_before: i64,
) -> (Vec<OrderLineSummary>, Version) {
    let tx = TpccTxn::begin(db);
    let ts_start = tx.ts_start();

    let ranges = partition_order_line_range(num_warehouses, QUERY_FANOUT.min(pool.num_workers()));
    let partials = pool.try_dispatch(ranges, move |tree: &TpccTree, range| {
        let mut groups = empty_groups();
        RangeQueryIter::new(tree, ts_start, range, false, READ_ONLY_SCAN_WORKER_ID).for_each_ref(|key, row| {
            let ol = row.as_order_line();
            let Some(delivered) = ol.ol_delivery_d else {
                return;
            };
            if delivered > delivered_before {
                return;
            }
            let g = &mut groups[crate::bat_bench::tpcc_schema::decode_order_line_number(key) as usize];
            g.count += 1;
            g.sum_qty += ol.ol_quantity as u64;
            g.sum_amount += ol.ol_amount;
        });
        groups
    });
    tx.commit();

    let mut total = empty_groups();
    for partial in partials {
        for i in 0..16 {
            total[i].count += partial[i].count;
            total[i].sum_qty += partial[i].sum_qty;
            total[i].sum_amount += partial[i].sum_amount;
        }
    }

    let mut out: Vec<_> = total.into_iter().filter(|g| g.count > 0).collect();
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

    let ranges = partition_order_line_range(num_warehouses, QUERY_FANOUT.min(pool.num_workers()));
    let revenue: f64 = pool
        .try_dispatch(ranges, move |tree: &TpccTree, range| {
            let mut revenue = 0.0;
            RangeQueryIter::new(tree, ts_start, range, false, READ_ONLY_SCAN_WORKER_ID).for_each_ref(|_, row| {
                let ol = row.as_order_line();
                if ol.ol_delivery_d.is_some_and(|delivered| {
                    delivered >= date_lo && delivered < date_hi && ol.ol_quantity < max_qty
                }) {
                    revenue += ol.ol_amount;
                }
            });
            revenue
        })
        .into_iter()
        .sum();
    tx.commit();
    (revenue, ts_start)
}
