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

use std::fmt::Display;

use crate::mv_bench::tpcc_random::*;
use crate::mv_bench::tpcc_schema::*;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_db::{DbTransaction, TableId};
use crate::mv_query::interval::Interval;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_record_model::tx_stamp::WorkerId;
use crate::mv_record_model::version_info::Version;

type Res<'a> = CRUDOperationResult<'a, TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>;

/// A multi-table OSIC transaction over a [`TpccDatabase`], one fixed
/// snapshot shared by every read/write it issues across any of its 14
/// tables, committed exactly once at the end — a thin, `Table`-addressed
/// wrapper over [`mv_db::DbTransaction`] (`self.inner`), which does all the
/// actual work: this type just resolves each `Table` to the `TableId`
/// `TpccDatabase` cached for it (see `TpccDatabase::table_ids`) and
/// delegates. See `DbTransaction`'s doc for the shared OSIC/WAL semantics
/// (fire-and-forget logging, one Commit marker per transaction regardless
/// of how many tables it touched) and abort behavior (dropping without
/// `commit()` reverts every write, across every table touched) — inherited
/// here automatically, since dropping `self.inner` runs `DbTransaction`'s
/// own `Drop` impl.
pub struct TpccTxn<'a> {
    db: &'a TpccDatabase,
    inner: DbTransaction<'a, TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>,
}

impl<'a> TpccTxn<'a> {
    pub fn begin(db: &'a TpccDatabase) -> Self {
        Self { db, inner: DbTransaction::begin(&db.db) }
    }

    #[inline(always)]
    fn resolve(&self, table: Table) -> TableId {
        self.db.table_ids[table as usize]
    }

    #[inline(always)]
    pub const fn ts_start(&self) -> Version {
        self.inner.ts_start()
    }

    #[inline(always)]
    pub const fn worker_id(&self) -> WorkerId {
        self.inner.worker_id()
    }

    /// Point read against this transaction's fixed snapshot, on `table`.
    pub fn point(&self, table: Table, key: TpccKey) -> Res<'_> {
        self.inner.point(self.resolve(table), key)
    }

    /// Range read against this transaction's fixed snapshot, on `table`.
    /// `force_read_all` is kept only for call-site compatibility —
    /// `DbTransaction::range` is always eager (see its doc); every real
    /// call site in this crate already passes `true`.
    pub fn range(&self, table: Table, range: Interval<TpccKey>, _force_read_all: bool) -> Res<'_> {
        self.inner.range(self.resolve(table), range)
    }

    /// Like `range`, but only the smallest-key match — see
    /// `DbTransaction::range_min`'s doc.
    pub fn range_min(&self, table: Table, range: Interval<TpccKey>) -> Option<RecordPointResult<TpccKey, TpccRow>> {
        self.inner.range_min(self.resolve(table), range)
    }

    pub fn insert(&self, table: Table, key: TpccKey, payload: TpccRow) -> Res<'_> {
        self.inner.insert(self.resolve(table), key, payload)
    }

    pub fn update(&self, table: Table, key: TpccKey, payload: TpccRow) -> Res<'_> {
        self.inner.update(self.resolve(table), key, payload)
    }

    pub fn delete(&self, table: Table, key: TpccKey) -> Res<'_> {
        self.inner.delete(self.resolve(table), key)
    }

    /// Instant commit — see `DbTransaction::commit`'s doc: exactly one WAL
    /// Commit marker for the whole transaction (every table on this
    /// database shares one WAL), not one marker per touched table.
    pub fn commit(self) -> Option<Version> {
        self.inner.commit()
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
