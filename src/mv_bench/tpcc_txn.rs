//! The five standard TPC-C transaction profiles (spec §2.4-§2.8), executed
//! against a [`TpccDatabase`] (14 independent per-table trees sharing one
//! transactional core, see `mv_bench::tpcc_schema` module docs) through
//! [`TpccTxn`] so each business transaction is one atomic, snapshot-isolated
//! unit even though it touches several tables (Warehouse/District/Customer/
//! Order/...).
//!
//! Not an audited TPC-C kit: population sizes, a couple of `NURand` run
//! constants, and the first-name tie-break are simplified (see
//! `tpcc_random.rs`/`tpcc_schema.rs`). The transaction logic, random
//! distributions (% remote, % rollback, % by-name), and atomicity are
//! faithful to the spec.

use std::cell::RefCell;
use std::fmt::Display;

use crate::mv_bench::tpcc_random::*;
use crate::mv_bench::tpcc_schema::*;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_db::TransactionState;
use crate::mv_db::transaction::{
    delete_on_tree, insert_on_tree, point_on_tree, range_min_on_tree, range_on_tree, update_on_tree,
};
use crate::mv_query::interval::Interval;
use crate::mv_query::iter_query::RangeQueryIter;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::Version;

type Res<'a> = CRUDOperationResult<'a, TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>;

/// A multi-table OSIC transaction over a [`TpccDatabase`], one fixed
/// snapshot shared by every read/write it issues across any of its 14
/// tables, committed exactly once at the end.
///
/// Unlike the pre-size-class-dispatch design, this does *not* wrap
/// `mv_db::DbTransaction`: `Table::Warehouse`/`Table::District` resolve to a
/// `TreeClass::Big` tree with a different `NUM_RECORDS` than the other 12
/// (see `tpcc_schema::TreeClass`'s doc), and `DbTransaction<FAN_OUT,
/// NUM_RECORDS, ..>`'s `tree(&self, table)` lookup is uniformly typed —
/// it can't resolve a `TableId` to two different concrete tree types. So
/// `TpccTxn` calls the same `mv_db::transaction::{insert_on_tree, ...}`
/// free functions `DbTransaction`'s own methods are built on directly
/// (reusing their exact insert/update/self-overwrite/WAL-tagging logic, not
/// a hand-rolled duplicate of it — see those functions' own doc), against
/// whichever concrete tree `table.class()` says to use, and owns its own
/// snapshot/write-tracking/abort state instead of delegating it.
///
/// `written` is the one piece that has to be genuinely different from
/// `DbTransaction`'s: it records every write in true chronological order
/// *across both tree classes*, not per-class. `abort`/`Drop` walk it in
/// reverse (LIFO — see `mv_db::transaction::DbTransaction::abort`'s doc for
/// why forward order can expose a partially-unwound transaction to a
/// concurrent one) — if a big-class and a standard-class write were tracked
/// in two separate lists instead, reverting "each list in its own reverse
/// order" would not reproduce the transaction's *true* combined reverse
/// order whenever the two classes were interleaved (e.g. New-Order writes
/// District — big — then Stock/OrderLine/Orders — standard — in that
/// order), which is exactly the ordering `DbTransaction::abort`'s doc warns
/// is load-bearing.
pub struct TpccTxn<'a> {
    db: &'a TpccDatabase,
    worker_id: WorkerId,
    ts_start: Version,
    committed: TransactionState,
    written: RefCell<Vec<(Table, TpccKey)>>,
}

/// `BigTreeOp` implementors for each `TpccTxn` operation — see
/// `tpcc_schema::BigTreeOp`'s doc for why these exist (one static dispatch
/// site, `TpccDatabase::dispatch_big`, instead of a 5-way match per
/// operation) and each one just forwards to the exact same
/// `mv_db::transaction::{point_on_tree, ..}` free function `TpccTxn`'s
/// `TreeClass::Standard` arm already calls, so `Big`-class tables get
/// identical semantics, normalized back to `Res`/`Option<RecordPointResult>`
/// (see `normalize`'s doc).
struct PointOp { worker_id: WorkerId, ts_start: Version, key: TpccKey }
impl BigTreeOp for PointOp {
    type Output = Res<'static>;
    fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(self, tree: &crate::mv_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>) -> Res<'static> {
        normalize(point_on_tree(tree, self.worker_id, self.ts_start, self.key))
    }
}

struct RangeOp { worker_id: WorkerId, ts_start: Version, range: Interval<TpccKey> }
impl BigTreeOp for RangeOp {
    type Output = Res<'static>;
    fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(self, tree: &crate::mv_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>) -> Res<'static> {
        normalize(range_on_tree(tree, self.worker_id, self.ts_start, self.range))
    }
}

struct RangeMinOp { worker_id: WorkerId, ts_start: Version, range: Interval<TpccKey> }
impl BigTreeOp for RangeMinOp {
    type Output = Option<RecordPointResult<TpccKey, TpccRow>>;
    fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(self, tree: &crate::mv_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>) -> Self::Output {
        range_min_on_tree(tree, self.worker_id, self.ts_start, self.range)
    }
}

struct RangeVisitOp<'a> {
    worker_id: WorkerId,
    ts_start: Version,
    range: Interval<TpccKey>,
    visit: &'a mut dyn FnMut(TpccKey, &TpccRow),
}
impl BigTreeOp for RangeVisitOp<'_> {
    type Output = ();
    fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(
        self,
        tree: &crate::mv_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>,
    ) {
        RangeQueryIter::new(tree, self.ts_start, self.range, false, self.worker_id)
            .for_each_ref(self.visit);
    }
}

struct InsertOp { worker_id: WorkerId, ts_start: Version, key: TpccKey, payload: TpccRow }
impl BigTreeOp for InsertOp {
    type Output = (Res<'static>, bool);
    fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(self, tree: &crate::mv_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>) -> Self::Output {
        let (r, track) = insert_on_tree(tree, self.worker_id, self.ts_start, self.key, self.payload);
        (normalize(r), track)
    }
}

struct UpdateOp { worker_id: WorkerId, ts_start: Version, key: TpccKey, payload: TpccRow }
impl BigTreeOp for UpdateOp {
    type Output = (Res<'static>, bool);
    fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(self, tree: &crate::mv_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>) -> Self::Output {
        let (r, track) = update_on_tree(tree, self.worker_id, self.ts_start, self.key, self.payload);
        (normalize(r), track)
    }
}

struct DeleteOp { worker_id: WorkerId, ts_start: Version, key: TpccKey }
impl BigTreeOp for DeleteOp {
    type Output = (Res<'static>, bool);
    fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(self, tree: &crate::mv_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>) -> Self::Output {
        let (r, track) = delete_on_tree(tree, self.worker_id, self.ts_start, self.key);
        (normalize(r), track)
    }
}

struct WalCommitOp { stamp: TxStamp, ts_commit: Version }
impl BigTreeOp for WalCommitOp {
    type Output = ();
    fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(self, tree: &crate::mv_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>) {
        tree.wal_log_commit(self.stamp, self.ts_commit)
    }
}

struct AbortWriteOp { key: TpccKey, stamp: TxStamp }
impl BigTreeOp for AbortWriteOp {
    type Output = ();
    fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(self, tree: &crate::mv_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>) {
        tree.abort_write(self.key, self.stamp)
    }
}

impl<'a> TpccTxn<'a> {
    /// Draws `ts_start` from the database's shared `TxContext` and
    /// registers it as an active snapshot once, covering every table this
    /// transaction may go on to touch, standard- or big-class alike (both
    /// share the same `ctx` — see `TpccDatabase::make_big_trees`).
    pub fn begin(db: &'a TpccDatabase) -> Self {
        let worker_id = db.db.worker_id();
        let ts_start = db.db.begin_snapshot();
        Self { db, worker_id, ts_start, committed: TransactionState::InFlight, written: RefCell::new(Vec::new()) }
    }

    #[inline(always)]
    pub const fn ts_start(&self) -> Version {
        self.ts_start
    }

    #[inline(always)]
    pub const fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    /// Point read against this transaction's fixed snapshot, on `table`.
    pub fn point(&self, table: Table, key: TpccKey) -> Res<'_> {
        match table.class() {
            TreeClass::Standard => point_on_tree(&self.db.tree_for(table), self.worker_id, self.ts_start, key),
            TreeClass::Big => self.db.dispatch_big(table, PointOp { worker_id: self.worker_id, ts_start: self.ts_start, key }),
        }
    }

    /// Range read against this transaction's fixed snapshot, on `table`.
    /// `force_read_all` is kept only for call-site compatibility — the
    /// underlying `range_on_tree` is always eager; every real call site in
    /// this crate already passes `true`.
    pub fn range(&self, table: Table, range: Interval<TpccKey>, _force_read_all: bool) -> Res<'_> {
        match table.class() {
            TreeClass::Standard => range_on_tree(&self.db.tree_for(table), self.worker_id, self.ts_start, range),
            TreeClass::Big => self.db.dispatch_big(table, RangeOp { worker_id: self.worker_id, ts_start: self.ts_start, range }),
        }
    }

    /// Like `range`, but only the smallest-key match — see
    /// `mv_db::transaction::range_min_on_tree`'s doc. No normalization
    /// needed: unlike `Res`, `Option<RecordPointResult<..>>` carries no
    /// `NUM_RECORDS`/`FAN_OUT` at all.
    pub fn range_min(&self, table: Table, range: Interval<TpccKey>) -> Option<RecordPointResult<TpccKey, TpccRow>> {
        match table.class() {
            TreeClass::Standard => range_min_on_tree(&self.db.tree_for(table), self.worker_id, self.ts_start, range),
            TreeClass::Big => self.db.dispatch_big(table, RangeMinOp { worker_id: self.worker_id, ts_start: self.ts_start, range }),
        }
    }

    /// Streams a snapshot-consistent range directly into `visit`, avoiding
    /// one `RecordPointResult` and one retained payload handle per row.
    pub fn range_for_each(
        &self,
        table: Table,
        range: Interval<TpccKey>,
        mut visit: impl FnMut(TpccKey, &TpccRow),
    ) {
        match table.class() {
            TreeClass::Standard => RangeQueryIter::new(
                &self.db.tree_for(table), self.ts_start, range, false, self.worker_id)
                .for_each_ref(&mut visit),
            TreeClass::Big => self.db.dispatch_big(table, RangeVisitOp {
                worker_id: self.worker_id,
                ts_start: self.ts_start,
                range,
                visit: &mut visit,
            }),
        }
    }

    /// Zero-copy left fold over a snapshot-consistent table range.
    pub fn range_fold<Acc>(
        &self,
        table: Table,
        range: Interval<TpccKey>,
        initial: Acc,
        mut fold: impl FnMut(Acc, TpccKey, &TpccRow) -> Acc,
    ) -> Acc {
        let mut acc = Some(initial);
        self.range_for_each(table, range, |key, row| {
            acc = Some(fold(acc.take().unwrap(), key, row));
        });
        acc.unwrap()
    }

    /// Counts visible rows without constructing result objects.
    pub fn range_count(&self, table: Table, range: Interval<TpccKey>) -> usize {
        self.range_fold(table, range, 0usize, |count, _, _| count + 1)
    }

    pub fn insert(&self, table: Table, key: TpccKey, payload: TpccRow) -> Res<'_> {
        let (result, track) = match table.class() {
            TreeClass::Standard => insert_on_tree(&self.db.tree_for(table), self.worker_id, self.ts_start, key, payload),
            TreeClass::Big => self.db.dispatch_big(table, InsertOp { worker_id: self.worker_id, ts_start: self.ts_start, key, payload }),
        };
        if track {
            self.written.borrow_mut().push((table, key));
        }
        result
    }

    pub fn update(&self, table: Table, key: TpccKey, payload: TpccRow) -> Res<'_> {
        let (result, track) = match table.class() {
            TreeClass::Standard => update_on_tree(&self.db.tree_for(table), self.worker_id, self.ts_start, key, payload),
            TreeClass::Big => self.db.dispatch_big(table, UpdateOp { worker_id: self.worker_id, ts_start: self.ts_start, key, payload }),
        };
        if track {
            self.written.borrow_mut().push((table, key));
        }
        result
    }

    pub fn delete(&self, table: Table, key: TpccKey) -> Res<'_> {
        let (result, track) = match table.class() {
            TreeClass::Standard => delete_on_tree(&self.db.tree_for(table), self.worker_id, self.ts_start, key),
            TreeClass::Big => self.db.dispatch_big(table, DeleteOp { worker_id: self.worker_id, ts_start: self.ts_start, key }),
        };
        if track {
            self.written.borrow_mut().push((table, key));
        }
        result
    }

    /// Instant commit: appends `ts_commit` to this worker's (shared)
    /// `CommitLog` — making every write this transaction made, across every
    /// table and both tree classes, visible at once — then logs exactly
    /// **one** WAL Commit marker, through whichever table this transaction
    /// happened to write first (every table on this database, big-class
    /// included, shares the same `Arc<WalWriter>` — see
    /// `TpccDatabase::enable_wal`/`make_big_trees`). No-op if this
    /// transaction never wrote anything.
    pub fn commit(mut self) -> Option<Version> {
        if let TransactionState::InFlight = self.committed {
            self.committed = TransactionState::Committed;

            let ts_commit = self.db.db.ctx.commit_tx(self.worker_id);

            if let Some(&(table, _)) = self.written.borrow().first() {
                let stamp = TxStamp::new(self.worker_id, self.ts_start);
                match table.class() {
                    TreeClass::Standard => self.db.tree_for(table).wal_log_commit(stamp, ts_commit),
                    TreeClass::Big => self.db.dispatch_big(table, WalCommitOp { stamp, ts_commit }),
                }
            }

            self.db.db.end_snapshot(self.ts_start);
            Some(ts_commit)
        } else {
            None
        }
    }

    /// Reverts every write this transaction made, in true chronological
    /// reverse (LIFO) order across both tree classes — see this type's own
    /// doc for why that combined ordering (not two independently-reversed
    /// per-class lists) is the one that matters. Shared by `Drop` (the
    /// normal path: dropping an in-flight `TpccTxn` without `commit()`) and
    /// nothing else today, since `TpccTxn` — like the pre-size-class-dispatch
    /// design — exposes no separate public `abort()`; every real call site
    /// just lets an unwanted transaction fall out of scope.
    fn revert_all(&self) {
        let stamp = TxStamp::new(self.worker_id, self.ts_start);
        for &(table, key) in self.written.borrow().iter().rev() {
            match table.class() {
                TreeClass::Standard => self.db.tree_for(table).abort_write(key, stamp),
                TreeClass::Big => self.db.dispatch_big(table, AbortWriteOp { key, stamp }),
            }
        }
    }
}

impl<'a> Drop for TpccTxn<'a> {
    fn drop(&mut self) {
        // An explicit `commit()` already reverted-or-not and released the
        // snapshot itself (see `commit`'s `TransactionState` guard) — skip
        // here, not just belt-and-suspenders: re-running would double
        // `end_snapshot` this transaction's `ts_start`.
        if let TransactionState::InFlight = self.committed {
            self.revert_all();
            self.db.db.end_snapshot(self.ts_start);
        }
    }
}

impl Display for TpccTxn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TpccTxn(worker={}, ts_start={})", self.worker_id(), self.ts_start())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnOutcome {
    /// Committed successfully.
    Committed,
    /// Lost a first-writer-wins race against a concurrent transaction.
    Conflict,
    /// Business-logic rollback expected by the spec itself (New-Order's ~1%
    /// invalid-item case, or a by-name lookup that found no matching row).
    UserAbort,
}

/// `pub(crate)`: also reused by `mv_bench::tpch_queries` (its CH-benCHmark
/// queries read through the same `TpccTxn` point/range API).
pub(crate) fn one<'a>(res: Res<'a>) -> Option<RecordPointResult<TpccKey, TpccRow>> {
    match res {
        CRUDOperationResult::MatchedRecords(mut v) => v.pop(),
        other => panic!("tpcc: expected MatchedRecords (point), got {other}"),
    }
}

pub(crate) fn many<'a>(res: Res<'a>) -> Vec<RecordPointResult<TpccKey, TpccRow>> {
    match res {
        CRUDOperationResult::MatchedRecords(v) => v,
        other => panic!("tpcc: expected MatchedRecords (range), got {other}"),
    }
}

macro_rules! wtry {
    ($e:expr) => {
        match $e {
            CRUDOperationResult::Inserted(_)
            | CRUDOperationResult::Updated(_)
            | CRUDOperationResult::Deleted(_) => {}
            CRUDOperationResult::Conflict => return TxnOutcome::Conflict,
            other => panic!("tpcc: unexpected write result: {other}"),
        }
    };
}

/// Picks a warehouse id different from `home`, uniformly among the rest.
/// Callers only invoke this when `cfg.num_warehouses > 1`.
fn pick_remote_warehouse(cfg: &TpccConfig, home: u32) -> u32 {
    loop {
        let w = with_fast_rng(|rng| rng.u32(1..=cfg.num_warehouses));
        if w != home {
            return w;
        }
    }
}

/// Picks the `(n+1)/2`-th (1-based, rounding up) entry of a list already
/// sorted by the tie-break key (here: the customer-name-index key, which
/// sorts by `first_code` then `c_id` within a fixed last-name prefix) — the
/// spec's rule for "the customer near the middle of the list, ordered by
/// first name".
fn pick_middle_by_name(matches: &[RecordPointResult<TpccKey, TpccRow>]) -> u32 {
    let mid = (matches.len() + 1) / 2 - 1;
    decode_customer_name_idx_c_id(matches[mid].key)
}

// ---------------------------------------------------------------------
// New-Order (spec §2.4): ~45% of the mix.
// ---------------------------------------------------------------------

pub fn new_order(db: &TpccDatabase, cfg: &TpccConfig, home_w_id: u32, allow_remote: bool) -> TxnOutcome {
    let d_id = with_fast_rng(|rng| rng.u8(1..=cfg.districts_per_warehouse));
    let c_id = nu_rand_customer_id(cfg.customers_per_district);
    let ol_cnt = with_fast_rng(|rng| rng.u8(5..=15));
    // Spec: ~1% of New-Order transactions roll back on an intentionally
    // invalid item id, chosen among that transaction's own order lines.
    let invalid_line = if with_fast_rng(|rng| rng.u32(1..=100)) == 1 {
        Some(with_fast_rng(|rng| rng.u8(0..ol_cnt)))
    } else {
        None
    };

    struct Line { i_id: u32, supply_w_id: u32, qty: u8 }
    let lines: Vec<Line> = (0..ol_cnt).map(|i| {
        let i_id = if Some(i) == invalid_line { cfg.num_items + 1 } else { nu_rand_item_id(cfg.num_items) };
        let remote = allow_remote && cfg.num_warehouses > 1 && with_fast_rng(|rng| rng.u32(1..=100)) == 1;
        let supply_w_id = if remote { pick_remote_warehouse(cfg, home_w_id) } else { home_w_id };
        let qty = with_fast_rng(|rng| rng.u8(1..=10));
        Line { i_id, supply_w_id, qty }
    }).collect();
    let all_local = lines.iter().all(|l| l.supply_w_id == home_w_id);

    let tx = TpccTxn::begin(db);

    let Some(warehouse) = one(tx.point(Table::Warehouse, k_warehouse(home_w_id))) else {
        drop(tx); return TxnOutcome::Conflict; // tree not populated for this key: treat defensively
    };
    let w_tax = warehouse.payload.as_warehouse().w_tax;

    let Some(district) = one(tx.point(Table::District, k_district(home_w_id, d_id))) else {
        drop(tx); return TxnOutcome::Conflict;
    };
    let mut d_row = district.payload.as_district().clone();
    let o_id = d_row.d_next_o_id;
    let d_tax = d_row.d_tax;

    let Some(customer) = one(tx.point(Table::Customer, k_customer(home_w_id, d_id, c_id))) else {
        drop(tx); return TxnOutcome::Conflict;
    };
    let c_discount = customer.payload.as_customer().c_discount;

    // Read-only validation pass over every line's item *before* any write,
    // so the ~1% expected rollback never leaves a partial write behind.
    let mut priced = Vec::with_capacity(lines.len());
    for line in &lines {
        match one(tx.point(Table::Item, k_item(line.i_id))) {
            Some(item) => priced.push((line, item.payload.as_item().i_price)),
            None => { drop(tx); return TxnOutcome::UserAbort; }
        }
    }

    d_row.d_next_o_id = o_id + 1;
    wtry!(tx.update(Table::District, k_district(home_w_id, d_id), TpccRow::District(Box::new(d_row))));

    for (ol_number, (line, i_price)) in priced.into_iter().enumerate() {
        let ol_number = (ol_number + 1) as u8;

        let Some(stock) = one(tx.point(Table::Stock, k_stock(line.supply_w_id, line.i_id))) else {
            return TxnOutcome::Conflict;
        };
        let mut s_row = stock.payload.as_stock().clone();
        s_row.s_quantity = if s_row.s_quantity - line.qty as i32 >= 10 {
            s_row.s_quantity - line.qty as i32
        } else {
            s_row.s_quantity - line.qty as i32 + 91
        };
        s_row.s_ytd += line.qty as f64;
        s_row.s_order_cnt += 1;
        if line.supply_w_id != home_w_id {
            s_row.s_remote_cnt += 1;
        }
        wtry!(tx.update(Table::Stock, k_stock(line.supply_w_id, line.i_id), TpccRow::Stock(Box::new(s_row))));

        let ol_amount = line.qty as f64 * i_price * (1.0 - c_discount) * (1.0 + w_tax + d_tax);
        wtry!(tx.insert(Table::OrderLine, k_order_line(home_w_id, d_id, o_id, ol_number), TpccRow::OrderLine(Box::new(OrderLine {
            ol_i_id: line.i_id,
            ol_supply_w_id: line.supply_w_id,
            ol_delivery_d: None,
            ol_quantity: line.qty,
            ol_amount,
            ol_dist_info: rnd_astring(24, 24),
        }))));
    }

    wtry!(tx.insert(Table::Orders, k_order(home_w_id, d_id, o_id), TpccRow::Order(Box::new(Order {
        o_c_id: c_id,
        o_entry_d: now_millis(),
        o_carrier_id: None,
        o_ol_cnt: ol_cnt,
        o_all_local: all_local,
    }))));
    wtry!(tx.insert(Table::NewOrder, k_new_order(home_w_id, d_id, o_id), TpccRow::NewOrder(NewOrderMarker { no_o_id: o_id })));

    match tx.update(Table::CustLastOrder, k_cust_last_order(home_w_id, d_id, c_id), TpccRow::CustLastOrder(o_id)) {
        CRUDOperationResult::Updated(_) => {}
        CRUDOperationResult::Conflict => return TxnOutcome::Conflict,
        // This customer has never had an order before: whenever
        // customers_per_district > initial_orders_per_district, load time
        // only seeds a CustLastOrder row for the (shuffled) subset of
        // customers who received one of the initial orders — everyone else
        // gets their row created here, on their actual first order.
        CRUDOperationResult::ZeroAffected(_) => {
            wtry!(tx.insert(Table::CustLastOrder, k_cust_last_order(home_w_id, d_id, c_id), TpccRow::CustLastOrder(o_id)));
        }
        other => panic!("tpcc: unexpected cust_last_order write result: {other}"),
    }

    tx.commit();
    TxnOutcome::Committed
}

// ---------------------------------------------------------------------
// Payment (spec §2.5): ~43% of the mix.
// ---------------------------------------------------------------------

pub fn payment(db: &TpccDatabase, cfg: &TpccConfig, home_w_id: u32, allow_remote: bool, history_seq: &std::sync::atomic::AtomicU64) -> TxnOutcome {
    let d_id = with_fast_rng(|rng| rng.u8(1..=cfg.districts_per_warehouse));
    let amount = with_fast_rng(|rng| rng.u32(100..=500_000)) as f64 / 100.0;

    let remote = allow_remote && cfg.num_warehouses > 1 && with_fast_rng(|rng| rng.u32(1..=100)) <= 15;
    let (c_w_id, c_d_id) = if remote {
        (pick_remote_warehouse(cfg, home_w_id), with_fast_rng(|rng| rng.u8(1..=cfg.districts_per_warehouse)))
    } else {
        (home_w_id, d_id)
    };
    let by_last_name = with_fast_rng(|rng| rng.u32(1..=100)) <= 60;

    let tx = TpccTxn::begin(db);

    let Some(warehouse) = one(tx.point(Table::Warehouse, k_warehouse(home_w_id))) else {
        drop(tx); return TxnOutcome::Conflict;
    };
    let mut w_row = warehouse.payload.as_warehouse().clone();
    w_row.w_ytd += amount;
    let w_name = w_row.w_name.clone();
    wtry!(tx.update(Table::Warehouse, k_warehouse(home_w_id), TpccRow::Warehouse(Box::new(w_row))));

    let Some(district) = one(tx.point(Table::District, k_district(home_w_id, d_id))) else {
        return TxnOutcome::Conflict;
    };
    let mut d_row = district.payload.as_district().clone();
    d_row.d_ytd += amount;
    let d_name = d_row.d_name.clone();
    wtry!(tx.update(Table::District, k_district(home_w_id, d_id), TpccRow::District(Box::new(d_row))));

    let c_id = if by_last_name {
        let last_code = c_last_code_for_run();
        let (lo, hi) = k_customer_name_idx_prefix_bounds(c_w_id, c_d_id, last_code);
        let mut matches = many(tx.range(Table::CustomerNameIdx, Interval::new(lo, hi), true));
        matches.sort_by_key(|r| r.key);
        if matches.is_empty() {
            return TxnOutcome::UserAbort;
        }
        pick_middle_by_name(&matches)
    } else {
        nu_rand_customer_id(cfg.customers_per_district)
    };

    let Some(customer) = one(tx.point(Table::Customer, k_customer(c_w_id, c_d_id, c_id))) else {
        return TxnOutcome::Conflict;
    };
    let mut c_row = customer.payload.as_customer().clone();
    c_row.c_balance -= amount;
    c_row.c_ytd_payment += amount;
    c_row.c_payment_cnt += 1;
    if c_row.c_credit_bad {
        let note = format!("{c_id} {c_d_id} {c_w_id} {d_id} {home_w_id} {amount:.2} | {}", c_row.c_data);
        c_row.c_data = note.chars().take(500).collect();
    }
    wtry!(tx.update(Table::Customer, k_customer(c_w_id, c_d_id, c_id), TpccRow::Customer(Box::new(c_row))));

    let h_data = format!("{w_name}    {d_name}");
    let h_key = k_history(history_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    wtry!(tx.insert(Table::History, h_key, TpccRow::History(Box::new(History {
        h_c_id: c_id,
        h_c_d_id: c_d_id,
        h_c_w_id: c_w_id,
        h_d_id: d_id,
        h_w_id: home_w_id,
        h_date: now_millis(),
        h_amount: amount,
        h_data,
    }))));

    tx.commit();
    TxnOutcome::Committed
}

// ---------------------------------------------------------------------
// Order-Status (spec §2.6): ~4% of the mix. Read-only.
// ---------------------------------------------------------------------

pub fn order_status(db: &TpccDatabase, cfg: &TpccConfig, home_w_id: u32) -> TxnOutcome {
    let d_id = with_fast_rng(|rng| rng.u8(1..=cfg.districts_per_warehouse));
    let by_last_name = with_fast_rng(|rng| rng.u32(1..=100)) <= 60;

    let tx = TpccTxn::begin(db);

    let c_id = if by_last_name {
        let last_code = c_last_code_for_run();
        let (lo, hi) = k_customer_name_idx_prefix_bounds(home_w_id, d_id, last_code);
        let mut matches = many(tx.range(Table::CustomerNameIdx, Interval::new(lo, hi), true));
        matches.sort_by_key(|r| r.key);
        if matches.is_empty() {
            return TxnOutcome::UserAbort;
        }
        pick_middle_by_name(&matches)
    } else {
        nu_rand_customer_id(cfg.customers_per_district)
    };

    if one(tx.point(Table::Customer, k_customer(home_w_id, d_id, c_id))).is_none() {
        return TxnOutcome::Conflict;
    }

    let Some(last_order) = one(tx.point(Table::CustLastOrder, k_cust_last_order(home_w_id, d_id, c_id))) else {
        tx.commit();
        return TxnOutcome::Committed; // no order yet for this customer
    };
    let o_id = last_order.payload.as_cust_last_order();

    let _order = one(tx.point(Table::Orders, k_order(home_w_id, d_id, o_id)));
    let (lo, hi) = k_order_line_bounds(home_w_id, d_id, o_id);
    let _lines = many(tx.range(Table::OrderLine, Interval::new(lo, hi), true));

    tx.commit();
    TxnOutcome::Committed
}

// ---------------------------------------------------------------------
// Delivery (spec §2.7): ~4% of the mix. One sub-transaction per district —
// exactly the "find and delete oldest new-order" queue pattern that stresses
// tombstone/version-chain accumulation in the referenced benchmarks.
// ---------------------------------------------------------------------

pub struct DeliveryOutcome {
    pub delivered_districts: u32,
    pub empty_districts: u32,
    pub conflicts: u32,
}

pub fn delivery(db: &TpccDatabase, cfg: &TpccConfig, home_w_id: u32) -> DeliveryOutcome {
    let carrier_id = with_fast_rng(|rng| rng.u32(1..=10));
    let mut out = DeliveryOutcome { delivered_districts: 0, empty_districts: 0, conflicts: 0 };

    for d_id in 1..=cfg.districts_per_warehouse {
        match deliver_one_district(db, home_w_id, d_id, carrier_id) {
            TxnOutcome::Committed => out.delivered_districts += 1,
            TxnOutcome::Conflict => out.conflicts += 1,
            TxnOutcome::UserAbort => out.empty_districts += 1,
        }
    }
    out
}

fn deliver_one_district(db: &TpccDatabase, w_id: u32, d_id: u8, carrier_id: u32) -> TxnOutcome {
    let tx = TpccTxn::begin(db);

    let (lo, hi) = k_new_order_district_bounds(w_id, d_id);
    // `range_min`, not `range` + sort + take the smallest: ascending o_id
    // within a fixed (w_id,d_id) prefix means the *oldest* queued new-order
    // is exactly the smallest key in this range, so there's no need to
    // collect every currently-queued row just to read off its minimum.
    let Some(oldest) = tx.range_min(Table::NewOrder, Interval::new(lo, hi)) else {
        drop(tx);
        return TxnOutcome::UserAbort;
    };
    let o_id = match &*oldest.payload {
        TpccRow::NewOrder(m) => m.no_o_id,
        _ => unreachable!("NEW_ORDER-range scan returned a non-NewOrder row"),
    };

    // Unlike every other write in this module, this one has no first-writer-
    // wins protection to race against: New-Order's district-counter update
    // is what serializes concurrent New-Order transactions, but Delivery's
    // "find the oldest queued new-order" is a plain range scan with no
    // equivalent guard, so two concurrent Delivery calls can both pick the
    // very same queued row before either deletes it. Whichever loses that
    // race sees the row already gone — a real outcome of this queue
    // pattern (the one the referenced benchmarks stress deliberately), not
    // a bug — so it's treated the same as losing an OSIC conflict.
    match tx.delete(Table::NewOrder, oldest.key) {
        CRUDOperationResult::Deleted(_) => {}
        CRUDOperationResult::Conflict | CRUDOperationResult::ZeroAffected(_) => return TxnOutcome::Conflict,
        other => panic!("tpcc: unexpected delete result: {other}"),
    }

    let order_key = k_order(w_id, d_id, o_id);
    let Some(order_rec) = one(tx.point(Table::Orders, order_key)) else {
        return TxnOutcome::Conflict;
    };
    let mut order_row = order_rec.payload.as_order().clone();
    let c_id = order_row.o_c_id;
    order_row.o_carrier_id = Some(carrier_id);
    wtry!(tx.update(Table::Orders, order_key, TpccRow::Order(Box::new(order_row))));

    let (ol_lo, ol_hi) = k_order_line_bounds(w_id, d_id, o_id);
    let lines = many(tx.range(Table::OrderLine, Interval::new(ol_lo, ol_hi), true));
    let mut total = 0.0f64;
    let now = now_millis();
    for line in &lines {
        let mut ol = line.payload.as_order_line().clone();
        total += ol.ol_amount;
        ol.ol_delivery_d = Some(now);
        wtry!(tx.update(Table::OrderLine, line.key, TpccRow::OrderLine(Box::new(ol))));
    }

    let cust_key = k_customer(w_id, d_id, c_id);
    let Some(cust_rec) = one(tx.point(Table::Customer, cust_key)) else {
        return TxnOutcome::Conflict;
    };
    let mut c_row = cust_rec.payload.as_customer().clone();
    c_row.c_balance += total;
    c_row.c_delivery_cnt += 1;
    wtry!(tx.update(Table::Customer, cust_key, TpccRow::Customer(Box::new(c_row))));

    tx.commit();
    TxnOutcome::Committed
}

// ---------------------------------------------------------------------
// Stock-Level (spec §2.8): ~4% of the mix. Read-only.
// ---------------------------------------------------------------------

pub fn stock_level(db: &TpccDatabase, cfg: &TpccConfig, home_w_id: u32, threshold: i32) -> TxnOutcome {
    let d_id = with_fast_rng(|rng| rng.u8(1..=cfg.districts_per_warehouse));
    let tx = TpccTxn::begin(db);

    let Some(district) = one(tx.point(Table::District, k_district(home_w_id, d_id))) else {
        return TxnOutcome::Conflict;
    };
    let next_o_id = district.payload.as_district().d_next_o_id;
    let hi_o = next_o_id.saturating_sub(1);
    let lo_o = hi_o.saturating_sub(19).max(1);

    let mut item_ids = std::collections::HashSet::new();
    for o_id in lo_o..=hi_o {
        let (lo, hi) = k_order_line_bounds(home_w_id, d_id, o_id);
        for line in many(tx.range(Table::OrderLine, Interval::new(lo, hi), true)) {
            item_ids.insert(line.payload.as_order_line().ol_i_id);
        }
    }

    let mut low_stock = 0u32;
    for i_id in item_ids {
        if let Some(s) = one(tx.point(Table::Stock, k_stock(home_w_id, i_id))) {
            if s.payload.as_stock().s_quantity < threshold {
                low_stock += 1;
            }
        }
    }
    let _ = low_stock;

    tx.commit();
    TxnOutcome::Committed
}
