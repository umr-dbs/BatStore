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
use rand::prelude::*;

use crate::mv_bench::tpcc_random::*;
use crate::mv_bench::tpcc_schema::*;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationInnerReason::{KeyAlreadyDeleted, KeyAlreadyExists, KeyDoesNotExist};
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_page_model::leaf_page::LeafPage;
use crate::mv_query::iter_query::RangeQueryIter;
use crate::mv_record_model::record_point::{RecordPoint, RecordPointResult};
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::{Version, VersionInfo};
use crate::mv_utils::interval::Interval;

type Res<'a> = CRUDOperationResult<'a, TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>;

/// A multi-table OSIC transaction over a [`TpccDatabase`]: one fixed
/// snapshot (`ts_start`), drawn once from the database's *shared*
/// `TxContext` at `begin` and reused by every read/write this transaction
/// issues against any of its 14 tables, committed exactly once at the end —
/// the direct, multi-table analogue of `mv_query::transaction::Transaction`
/// (which this mirrors method-for-method, just resolving `self.tree` to
/// `self.db.tree_for(table)` per call instead of a single fixed tree). See
/// that type's doc for the shared OSIC/WAL semantics (fire-and-forget
/// logging) and abort behavior (dropping without `commit()` reverts every
/// write, across every table touched — see `Drop`) — identical here, just
/// spanning several trees instead of one.
pub struct TpccTxn<'a> {
    db: &'a TpccDatabase,
    worker_id: WorkerId,
    ts_start: Version,
    committed: bool,
    /// (table, key) pairs this transaction has actually written — see
    /// `mv_query::transaction::Transaction::written`'s doc, which this
    /// mirrors.
    written: RefCell<Vec<(Table, TpccKey)>>,
}

impl<'a> TpccTxn<'a> {
    /// Draws `ts_start` from the database's shared `TxContext` and registers
    /// it as an active snapshot *once*, covering every table this
    /// transaction may go on to touch — not just whichever table happens to
    /// be read/written first (see `mv_gc::tracker_handle::TrackerHandleSt::free_block`'s
    /// doc for why a per-table registration would be unsound once several
    /// tables share one commit log / active-snapshot registry).
    pub fn begin(db: &'a TpccDatabase) -> Self {
        let worker_id = db.ctx.worker_id();
        let ts_start = db.ctx.begin_snapshot();

        Self { db, worker_id, ts_start, committed: false, written: RefCell::new(Vec::new()) }
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
        let tree = self.db.tree_for(table);
        tree.key_point_read_from_root(
            tree.retrieve_root_for(self.ts_start),
            key,
            self.worker_id,
            self.ts_start)
    }

    /// Range read against this transaction's fixed snapshot, on `table`.
    pub fn range(&self, table: Table, range: Interval<TpccKey>, force_read_all: bool) -> Res<'_> {
        let scan = RangeQueryIter::new(
            self.db.tree_for(table),
            self.ts_start,
            range,
            false,
            self.worker_id);

        if force_read_all {
            CRUDOperationResult::MatchedRecords(scan.collect())
        } else {
            CRUDOperationResult::MatchedRecordIter(scan)
        }
    }

    /// First-writer-wins check, on `table`: the physically newest version at
    /// `key`, if any, must be visible to this transaction's snapshot —
    /// otherwise a concurrent transaction this one can't see got there
    /// first.
    ///
    /// Skips over an invalid entry (its writing transaction aborted) rather
    /// than checking its visibility — see the identical note on
    /// `mv_query::transaction::Transaction::newest_visible_to_me`, which
    /// this mirrors: an invalid stamp is never visible to anyone, so
    /// treating it as "the newest entry" would make every future write to
    /// this key see a permanent false conflict.
    fn newest_visible_to_me(&self, table: Table, leaf_page: &LeafPage<TPCC_NUM_RECORDS, TpccKey, TpccRow>, key: TpccKey) -> bool {
        let is_visible
            = |stamp| self.db.tree_for(table).is_visible_stamp(self.worker_id, self.ts_start, stamp);

        leaf_page.as_records()
            .iter()
            .rfind(|r| r.key() == key && !r.version().insertion_stamp().is_invalid())
            .map(|record| is_visible(record.version().insertion_stamp()))
            .unwrap_or(true)
    }

    /// Logs `build`'s write under this transaction's fixed `stamp`, against
    /// `table`'s own WAL (fire-and-forget, see the type doc). No-op when
    /// `table` has no WAL attached.
    #[inline(always)]
    fn log_write(&self, table: Table, stamp: TxStamp, build: impl FnOnce(Version) -> CRUDOperation<TpccKey, TpccRow>) {
        self.db.tree_for(table).wal_log_write(stamp, build);
    }

    pub fn insert(&self, table: Table, key: TpccKey, payload: TpccRow) -> Res<'_> {
        let tree = self.db.tree_for(table);
        let leaf_guard = tree.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        if !self.newest_visible_to_me(table, leaf_page, key) {
            return CRUDOperationResult::Conflict;
        }

        if leaf_page.as_records()
            .iter()
            .rfind(|r| r.key == key)
            .map(|r| r.version.is_live())
            .unwrap_or(false)
        {
            return CRUDOperationResult::ZeroAffected(KeyAlreadyExists);
        }

        let stamp = TxStamp::new(self.worker_id, self.ts_start);
        self.log_write(table, stamp, |_| CRUDOperation::Insert(key, payload.clone()));

        let current_len = leaf_page.len();

        leaf_page.push_uncommitted(
            RecordPoint::new(key, VersionInfo::new(stamp), payload),
            current_len);

        leaf_page.commit_delta(1, 0);

        self.written.borrow_mut().push((table, key));
        CRUDOperationResult::Inserted(stamp.ts_start())
    }

    pub fn update(&self, table: Table, key: TpccKey, payload: TpccRow) -> Res<'_> {
        let tree = self.db.tree_for(table);
        let leaf_guard = tree.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        if !self.newest_visible_to_me(table, leaf_page, key) {
            return CRUDOperationResult::Conflict;
        }

        let stamp = TxStamp::new(self.worker_id, self.ts_start);
        self.log_write(table, stamp, |_| CRUDOperation::Update(key, payload.clone()));

        let current_len = leaf_page.len();

        leaf_page.push_uncommitted(
            RecordPoint::new(key, VersionInfo::new(stamp), payload),
            current_len);

        leaf_page.commit_delta(1, 0);

        match leaf_page.delete_after_update(key, stamp) {
            Ok(Some(..)) => {
                leaf_page.commit_delta(-1, 1);
                self.written.borrow_mut().push((table, key));
                CRUDOperationResult::Updated(stamp.ts_start())
            }
            Ok(None) => {
                leaf_page.commit_delta(-1, 0);
                leaf_page.undo_uncommitted(current_len);
                CRUDOperationResult::ZeroAffected(KeyDoesNotExist)
            }
            Err(()) => {
                leaf_page.commit_delta(-1, 0);
                leaf_page.undo_uncommitted(current_len);
                CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
            }
        }
    }

    pub fn delete(&self, table: Table, key: TpccKey) -> Res<'_> {
        let tree = self.db.tree_for(table);
        let leaf_guard = tree.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        if !self.newest_visible_to_me(table, leaf_page, key) {
            return CRUDOperationResult::Conflict;
        }

        let stamp = TxStamp::new(self.worker_id, self.ts_start);
        self.log_write(table, stamp, |_| CRUDOperation::Delete(key));

        match leaf_page.delete(key, stamp) {
            Ok(Some(..)) => {
                leaf_page.commit_delta(-1, 1);
                self.written.borrow_mut().push((table, key));
                CRUDOperationResult::Deleted(stamp.ts_start())
            }
            Ok(None) => CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
            Err(()) => CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted),
        }
    }

    /// Instant commit: appends `ts_commit` to this worker's (shared)
    /// `CommitLog` — making every write this transaction made, across every
    /// table it touched, visible at once — and returns immediately.
    pub fn commit(mut self) -> Version {
        self.committed = true;
        let ts_commit = self.db.ctx.commit_tx(self.worker_id);
        self.db.ctx.end_snapshot(self.ts_start);
        ts_commit
    }
}

impl<'a> Drop for TpccTxn<'a> {
    fn drop(&mut self) {
        // Dropped without calling `commit` (e.g. the caller gave up after a
        // `Conflict`, or a business-logic `UserAbort`) — abort every write
        // this transaction made, on whichever table it made it on, before
        // releasing the registered snapshot (see the type doc and
        // `MVBTSt::abort_write`).
        if !self.committed {
            let stamp = TxStamp::new(self.worker_id, self.ts_start);
            for (table, key) in self.written.borrow().iter() {
                self.db.tree_for(*table).abort_write(*key, stamp);
            }
            self.db.ctx.end_snapshot(self.ts_start);
        }
    }
}

impl Display for TpccTxn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TpccTxn(worker={}, ts_start={})", self.worker_id, self.ts_start)
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
        let w = rand::rng().random_range(1..=cfg.num_warehouses);
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
    let d_id = rand::rng().random_range(1..=cfg.districts_per_warehouse);
    let c_id = nu_rand_customer_id(cfg.customers_per_district);
    let ol_cnt = rand::rng().random_range(5..=15u8);
    // Spec: ~1% of New-Order transactions roll back on an intentionally
    // invalid item id, chosen among that transaction's own order lines.
    let invalid_line = if rand::rng().random_range(1..=100) == 1 {
        Some(rand::rng().random_range(0..ol_cnt))
    } else {
        None
    };

    struct Line { i_id: u32, supply_w_id: u32, qty: u8 }
    let lines: Vec<Line> = (0..ol_cnt).map(|i| {
        let i_id = if Some(i) == invalid_line { cfg.num_items + 1 } else { nu_rand_item_id(cfg.num_items) };
        let remote = allow_remote && cfg.num_warehouses > 1 && rand::rng().random_range(1..=100) == 1;
        let supply_w_id = if remote { pick_remote_warehouse(cfg, home_w_id) } else { home_w_id };
        let qty = rand::rng().random_range(1..=10u8);
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
    let d_id = rand::rng().random_range(1..=cfg.districts_per_warehouse);
    let amount = rand::rng().random_range(100..=500_000) as f64 / 100.0;

    let remote = allow_remote && cfg.num_warehouses > 1 && rand::rng().random_range(1..=100) <= 15;
    let (c_w_id, c_d_id) = if remote {
        (pick_remote_warehouse(cfg, home_w_id), rand::rng().random_range(1..=cfg.districts_per_warehouse))
    } else {
        (home_w_id, d_id)
    };
    let by_last_name = rand::rng().random_range(1..=100) <= 60;

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
    let d_id = rand::rng().random_range(1..=cfg.districts_per_warehouse);
    let by_last_name = rand::rng().random_range(1..=100) <= 60;

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
    let carrier_id = rand::rng().random_range(1..=10u32);
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
    let mut queued = many(tx.range(Table::NewOrder, Interval::new(lo, hi), true));
    if queued.is_empty() {
        drop(tx);
        return TxnOutcome::UserAbort;
    }
    // Ascending key == ascending o_id within a fixed (w_id,d_id) prefix.
    queued.sort_by_key(|r| r.key);
    let oldest = &queued[0];
    let o_id = match &oldest.payload {
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
    let d_id = rand::rng().random_range(1..=cfg.districts_per_warehouse);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
    use crate::mv_root::index_root::RootIndexType;

    fn sample_warehouse() -> TpccRow {
        TpccRow::Warehouse(Box::new(Warehouse {
            w_name: "W1".into(), w_street_1: "s1".into(), w_street_2: "s2".into(),
            w_city: "city".into(), w_state: "CA".into(), w_zip: "123451111".into(),
            w_tax: 0.05, w_ytd: 300_000.0,
        }))
    }

    fn sample_district() -> TpccRow {
        TpccRow::District(Box::new(District {
            d_name: "D1".into(), d_street_1: "s1".into(), d_street_2: "s2".into(),
            d_city: "city".into(), d_state: "CA".into(), d_zip: "123451111".into(),
            d_tax: 0.05, d_ytd: 30_000.0, d_next_o_id: 1,
        }))
    }

    /// The cross-table analogue of `mv_query::transaction::tests::
    /// multi_op_transaction_sees_own_writes_and_isolates_others`: one
    /// `TpccTxn` writes to *two different tables* (Warehouse, District) —
    /// exactly the shared-snapshot-registration fix the multi-table refactor
    /// exists for (see `TpccTxn::begin`'s doc) — and both writes must become
    /// visible to other transactions atomically, as one unit, not one table
    /// at a time.
    #[test]
    fn cross_table_transaction_is_atomic_across_tables() {
        let db = TpccDatabase::new(RootIndexType::default());
        let w_key = k_warehouse(1);
        let d_key = k_district(1, 1);

        let tx1 = TpccTxn::begin(&db);
        assert!(matches!(tx1.insert(Table::Warehouse, w_key, sample_warehouse()), CRUDOperationResult::Inserted(_)));
        assert!(matches!(tx1.insert(Table::District, d_key, sample_district()), CRUDOperationResult::Inserted(_)));

        // Own writes, across both tables, are visible within the same
        // still-open transaction.
        assert!(matches!(tx1.point(Table::Warehouse, w_key), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));
        assert!(matches!(tx1.point(Table::District, d_key), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));

        let db_ref = &db;

        // A transaction on a different worker, snapshotting before tx1
        // commits, must see NEITHER table's write — if the shared snapshot
        // registration were broken (e.g. only registered against one
        // table), this could observe a partially-committed transaction.
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let tx2 = TpccTxn::begin(db_ref);
                assert!(matches!(tx2.point(Table::Warehouse, w_key), CRUDOperationResult::MatchedRecords(r) if r.is_empty()));
                assert!(matches!(tx2.point(Table::District, d_key), CRUDOperationResult::MatchedRecords(r) if r.is_empty()));
                tx2.commit();
            }).join().unwrap();
        });

        tx1.commit();

        // A transaction on yet another worker, snapshotting after tx1's
        // commit, must now see both writes.
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let tx3 = TpccTxn::begin(db_ref);
                assert!(matches!(tx3.point(Table::Warehouse, w_key), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));
                assert!(matches!(tx3.point(Table::District, d_key), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));
                tx3.commit();
            }).join().unwrap();
        });
    }

    /// First-writer-wins must still hold per-table under the shared `ctx`:
    /// a concurrent transaction's commit on `Table::District`, after tx1's
    /// snapshot was drawn, must make tx1 lose the race on that same table.
    #[test]
    fn first_writer_wins_conflict_holds_per_table_under_shared_ctx() {
        let db = TpccDatabase::new(RootIndexType::default());
        let d_key = k_district(1, 1);
        assert!(matches!(db.district.dispatch_crud(CRUDOperation::Insert(d_key, sample_district())),
            CRUDOperationResult::Inserted(_)));

        let tx1 = TpccTxn::begin(&db);

        let db_ref = &db;
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let tx2 = TpccTxn::begin(db_ref);
                assert!(matches!(tx2.update(Table::District, d_key, sample_district()), CRUDOperationResult::Updated(_)));
                tx2.commit();
            }).join().unwrap();
        });

        assert!(matches!(tx1.update(Table::District, d_key, sample_district()), CRUDOperationResult::Conflict));
    }

    /// The cross-table analogue of `mv_query::transaction::tests::
    /// dropped_transaction_reverts_its_earlier_writes_on_conflict`: one
    /// `TpccTxn` writes to *two different tables*, then loses a
    /// first-writer-wins race on a later op and drops without `commit()` —
    /// both of its earlier writes, across both tables, must be reverted, not
    /// left stuck as if committed (see `Drop`'s doc).
    #[test]
    fn dropped_tpcc_txn_reverts_writes_across_tables_on_conflict() {
        let db = TpccDatabase::new(RootIndexType::default());
        let w_key = k_warehouse(1);
        let d_key = k_district(1, 1);

        let tx1 = TpccTxn::begin(&db);
        assert!(matches!(tx1.insert(Table::Warehouse, w_key, sample_warehouse()), CRUDOperationResult::Inserted(_)));
        assert!(matches!(tx1.insert(Table::District, d_key, sample_district()), CRUDOperationResult::Inserted(_)));

        // A concurrent transaction on another worker inserts and commits a
        // second district key *after* tx1's snapshot was already taken.
        let d_key2 = k_district(1, 2);
        let db_ref = &db;
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let tx2 = TpccTxn::begin(db_ref);
                assert!(matches!(tx2.insert(Table::District, d_key2, sample_district()), CRUDOperationResult::Inserted(_)));
                tx2.commit();
            }).join().unwrap();
        });

        // tx1's snapshot predates tx2's insert, so tx1's own attempt to
        // write the same key must lose the race.
        assert!(matches!(tx1.insert(Table::District, d_key2, sample_district()), CRUDOperationResult::Conflict));

        // tx1 is dropped here without commit — both of its earlier writes
        // (Warehouse and District tables) must be reverted.
        drop(tx1);

        let tx3 = TpccTxn::begin(&db);
        assert!(matches!(tx3.point(Table::Warehouse, w_key), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
            "warehouse write by since-aborted tx1 must not be visible");
        assert!(matches!(tx3.point(Table::District, d_key), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
            "district write by since-aborted tx1 must not be visible");
        tx3.commit();
    }
}
