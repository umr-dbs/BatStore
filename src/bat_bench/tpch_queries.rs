//! CH-benCHmark-style analytical queries (Cole et al., "The Mixed Workload
//! CH-benCHmark", DBTest 2011): a well-known way to combine TPC-C and TPC-H
//! by adding TPC-H's 3 missing dimension tables (SUPPLIER/NATION/REGION,
//! see `tpcc_schema` module docs and `tpcc_load::populate_suppliers`/
//! `populate_regions_and_nations`) to the standard TPC-C schema, then running
//! TPC-H-style analytical queries directly against it.
//!
//! This isn't a SQL engine — there's no join operator, optimizer, or query
//! planner, just this tree's point/range scans. Every query here is
//! hand-written as a scan (or nested scans) plus in-memory grouping/joining,
//! which is exactly how each is implemented: small dimension tables
//! (REGION: 5 rows, NATION: 25, SUPPLIER: ~10,000) are loaded once into
//! direct-index arrays, then joined in memory against scans of the large
//! fact tables (ORDERS/ORDER_LINE).
//!
//! Only 4 of CH-benCHmark's 22 queries are implemented, chosen to cover a
//! representative spread of query shapes: [`q1`] (pure aggregation, no
//! joins), [`q6`] (filtered aggregation, no joins), [`q4`] (a correlated
//! semi-join / "exists" check), and [`q5`] (a multi-way dimension-table
//! join with grouping). Every query runs under one `TpccTxn` snapshot, so
//! its reads are mutually consistent even though nothing else here provides
//! atomicity — the same justification `tpcc_txn`'s read-only Order-Status/
//! Stock-Level transactions already rely on.
//!
//! All 4 queries here are adaptations, not literal ports of the published
//! CH-benCHmark SQL — see each function's doc comment for the specific
//! simplifications relative to the original TPC-H query it's modeled on.

use crate::bat_bench::tpcc_schema::*;
use crate::bat_bench::tpcc_txn::{TpccTxn, many, one};
use crate::bat_query::interval::Interval;
use crate::bat_record_model::version_info::Version;

// Every query below returns its result alongside the snapshot (`ts_start`)
// it read under, letting a caller measure HTAP-style staleness: how many
// logical-clock versions (`GlobalClock` advances on every transaction begin
// *and* commit, so this counts logical ticks, not a raw commit count)
// elapsed between this query's snapshot and whatever's freshest by the time
// it's read the result — i.e. `tree.current_version() - ts_start` right
// after a query returns is "how stale is this analytical answer, in
// versions, the moment I have it."

/// Per-`ol_number` group produced by [`q1`].
#[derive(Clone, Copy, Debug, Default)]
pub struct OrderLineSummary {
    pub ol_number: u8,
    pub count: u64,
    pub sum_qty: u64,
    pub sum_amount: f64,
}

impl OrderLineSummary {
    pub fn avg_qty(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_qty as f64 / self.count as f64
        }
    }

    pub fn avg_amount(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_amount / self.count as f64
        }
    }
}

/// CH-benCHmark Q1 ("Pricing Summary Report", adapted from TPC-H Q1): groups
/// every delivered order-line by its position within the order (`ol_number`,
/// 1..=15 — this schema's stand-in for TPC-H `lineitem`'s
/// `l_returnflag`/`l_linestatus`, since `order_line` has no such column),
/// aggregating count/sum(quantity)/sum(amount). One full `ORDER_LINE`
/// table scan.
pub fn q1(db: &TpccDatabase, delivered_before: i64) -> (Vec<OrderLineSummary>, Version) {
    let mut tx = TpccTxn::begin(db);
    let ts_start = tx.ts_start();
    let mut groups: [OrderLineSummary; 16] = std::array::from_fn(|i| OrderLineSummary {
        ol_number: i as u8,
        ..Default::default()
    });

    tx.range_for_each(Table::OrderLine, order_line_table_range(), |key, row| {
        let ol = row.as_order_line();
        let Some(delivered) = ol.ol_delivery_d else {
            return;
        };
        if delivered > delivered_before {
            return;
        }
        let g = &mut groups[decode_order_line_number(key) as usize];
        g.count += 1;
        g.sum_qty += ol.ol_quantity as u64;
        g.sum_amount += ol.ol_amount;
    });
    tx.commit();

    let mut out: Vec<_> = groups.into_iter().filter(|g| g.count > 0).collect();
    out.sort_by_key(|g| g.ol_number);
    (out, ts_start)
}

/// CH-benCHmark Q6 ("Forecasting Revenue Change", adapted from TPC-H Q6):
/// total revenue (`sum(ol_amount)`) from order-lines delivered within
/// `[date_lo, date_hi)` whose quantity is below `max_qty`. One full
/// `ORDER_LINE` table scan.
pub fn q6(db: &TpccDatabase, date_lo: i64, date_hi: i64, max_qty: u8) -> (f64, Version) {
    let mut tx = TpccTxn::begin(db);
    let ts_start = tx.ts_start();
    let mut revenue = 0.0;
    tx.range_for_each(Table::OrderLine, order_line_table_range(), |_, row| {
        let ol = row.as_order_line();
        if ol.ol_delivery_d.is_some_and(|delivered| {
            delivered >= date_lo && delivered < date_hi && ol.ol_quantity < max_qty
        }) {
            revenue += ol.ol_amount;
        }
    });
    tx.commit();
    (revenue, ts_start)
}

/// Per-`o_ol_cnt` group produced by [`q4`].
#[derive(Clone, Copy, Debug, Default)]
pub struct OrderPriorityCount {
    pub o_ol_cnt: u8,
    pub order_count: u64,
}

/// CH-benCHmark Q4 ("Order Priority Checking", adapted from TPC-H Q4): among
/// orders entered within `[date_lo, date_hi)`, counts (grouped by
/// `o_ol_cnt`) those with at least one order-line whose `ol_delivery_d` is
/// later than `o_entry_d + late_slack_millis`, or never delivered at all —
/// this schema's stand-in for TPC-H's `l_commitdate < l_receiptdate` check,
/// since `order_line` has no separate commit-date column. One full `ORDERS`
/// table scan, plus one `ORDER_LINE` range scan per order entered in range
/// (a correlated semi-join / "exists" check, done as a nested loop since
/// there's no join operator here — see module docs).
pub fn q4(
    db: &TpccDatabase,
    date_lo: i64,
    date_hi: i64,
    late_slack_millis: i64,
) -> (Vec<OrderPriorityCount>, Version) {
    let mut tx = TpccTxn::begin(db);
    let ts_start = tx.ts_start();
    let orders = many(tx.range(Table::Orders, orders_table_range(), true));

    let mut counts = [0u64; 256];
    for order_rec in &orders {
        let order = order_rec.payload.as_order();
        if order.o_entry_d < date_lo || order.o_entry_d >= date_hi {
            continue;
        }
        let (w_id, d_id, o_id) = decode_order_key(order_rec.key);
        let (lo, hi) = k_order_line_bounds(w_id, d_id, o_id);
        let lines = many(tx.range(Table::OrderLine, Interval::new(lo, hi), true));
        let late = lines
            .iter()
            .any(|l| match l.payload.as_order_line().ol_delivery_d {
                Some(d) => d > order.o_entry_d + late_slack_millis,
                None => true,
            });
        if late {
            counts[order.o_ol_cnt as usize] += 1;
        }
    }
    tx.commit();

    let out = counts
        .into_iter()
        .enumerate()
        .filter(|(_, order_count)| *order_count != 0)
        .map(|(o_ol_cnt, order_count)| OrderPriorityCount {
            o_ol_cnt: o_ol_cnt as u8,
            order_count,
        })
        .collect();
    (out, ts_start)
}

/// Per-nation revenue produced by [`q5`].
#[derive(Clone, Debug, Default)]
pub struct NationRevenue {
    pub n_name: String,
    pub revenue: f64,
}

/// CH-benCHmark Q5 ("Local Supplier Volume", adapted from TPC-H Q5): total
/// revenue (`sum(ol_amount)`) fulfilled by suppliers in `region_name`, for
/// orders entered within `[date_lo, date_hi)`, grouped by supplier nation
/// and sorted by revenue descending. Joins ORDERS -> ORDER_LINE -> STOCK
/// (via `ol_supply_w_id`/`ol_i_id`) -> SUPPLIER (via `Stock::s_su_suppkey`)
/// -> NATION -> REGION.
///
/// Simplification vs. the published CH-benCHmark SQL: the original also
/// requires the *customer's* nation (derived there via an
/// `ascii(substr(c_state,1,1)) = su_nationkey` trick, since TPC-C's
/// `CUSTOMER` has no nation column) to match the supplier's nation. This
/// port drops that predicate and groups purely by supplying nation — it
/// still exercises the same join shape and the same region/date filter +
/// group-by + aggregate as the original, without bolting an obscure
/// data-model hack onto this schema.
pub fn q5(
    db: &TpccDatabase,
    region_name: &str,
    date_lo: i64,
    date_hi: i64,
) -> (Vec<NationRevenue>, Version) {
    let mut tx = TpccTxn::begin(db);
    let ts_start = tx.ts_start();

    // Small dimension tables loaded once into memory — see module docs.
    let regions = many(tx.range(Table::Region, region_table_range(), true));
    let Some(region_id) = regions
        .iter()
        .find(|r| r.payload.as_region().r_name == region_name)
        .map(|r| decode_region_id(r.key))
    else {
        tx.commit();
        return (Vec::new(), ts_start);
    };

    let nations = many(tx.range(Table::Nation, nation_table_range(), true));
    let mut nation_names: [Option<String>; 256] = std::array::from_fn(|_| None);
    for nation in nations
        .iter()
        .filter(|n| n.payload.as_nation().n_regionkey == region_id)
    {
        nation_names[decode_nation_id(nation.key) as usize] =
            Some(nation.payload.as_nation().n_name.clone());
    }

    let suppliers = many(tx.range(Table::Supplier, supplier_table_range(), true));
    let supplier_capacity = suppliers
        .iter()
        .map(|supplier| decode_supplier_id(supplier.key) as usize)
        .max()
        .map_or(0, |max_id| max_id + 1);
    let mut supplier_nation = vec![None; supplier_capacity];
    for supplier in &suppliers {
        supplier_nation[decode_supplier_id(supplier.key) as usize] =
            Some(supplier.payload.as_supplier().s_nationkey);
    }

    let orders = many(tx.range(Table::Orders, orders_table_range(), true));
    let mut revenue = [None; 256];
    for order_rec in &orders {
        let order = order_rec.payload.as_order();
        if order.o_entry_d < date_lo || order.o_entry_d >= date_hi {
            continue;
        }
        let (w_id, d_id, o_id) = decode_order_key(order_rec.key);
        let (lo, hi) = k_order_line_bounds(w_id, d_id, o_id);
        for line in many(tx.range(Table::OrderLine, Interval::new(lo, hi), true)) {
            let ol = line.payload.as_order_line();
            let Some(stock) = one(tx.point(Table::Stock, k_stock(ol.ol_supply_w_id, ol.ol_i_id)))
            else {
                continue;
            };
            let su_id = stock.payload.as_stock().s_su_suppkey;
            let Some(nation_id) = supplier_nation.get(su_id as usize).copied().flatten() else {
                continue;
            };
            if nation_names[nation_id as usize].is_none() {
                continue; // supplier's nation isn't in the requested region
            }
            *revenue[nation_id as usize].get_or_insert(0.0) += ol.ol_amount;
        }
    }
    tx.commit();

    let mut out: Vec<_> = revenue
        .into_iter()
        .enumerate()
        .filter_map(|(nation_id, revenue)| {
            revenue
                .zip(nation_names[nation_id].as_ref())
                .map(|(revenue, name)| NationRevenue {
                    n_name: name.clone(),
                    revenue,
                })
        })
        .collect();
    out.sort_by(|a, b| b.revenue.partial_cmp(&a.revenue).unwrap());
    (out, ts_start)
}
