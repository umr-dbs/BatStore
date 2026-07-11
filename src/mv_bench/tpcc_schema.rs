//! TPC-C schema for the cMVBT benchmark harness.
//!
//! All nine TPC-C tables (plus two maintained secondary indexes) live in a
//! *single* `MVBTSt` tree, keyed by a composite `u64` and valued by the
//! [`TpccRow`] enum. Sharing one tree (one `GlobalClock`, one set of
//! `CommitLog`s) is what makes a `mv_query::transaction::Transaction` spanning
//! several "tables" (e.g. NewOrder touching Warehouse/District/Customer/
//! Order/NewOrder/OrderLine/Stock) atomic and snapshot-isolated as a whole,
//! matching how the referenced benchmarks (TPC-C + OLAP scans, e.g. Alhomssi
//! & Leis, VLDB'23) treat a business transaction as one unit.
//!
//! Also carries CH-benCHmark's (Cole et al., "The Mixed Workload CH-benCHmark",
//! DBTest 2011) three TPC-H-derived dimension tables — SUPPLIER, NATION,
//! REGION — in the same shared tree, feeding the analytical queries in
//! `mv_bench::tpch_queries`.
//!
//! Key layout: the top 4 bits select the table, the low 60 bits pack that
//! table's primary-key columns MSB-first (so a range scan of a byte-ordered
//! key range matches the natural column order, e.g. scanning all districts
//! of a warehouse or all order-lines of an order).
//!
//! Several row fields (addresses, `i_data`, `s_dist`, ...) are never read by
//! the 5 transaction profiles, same as in the real spec — they exist for
//! realistic row footprint (page density, leaf fan-out) rather than being
//! touched by transaction logic, so `dead_code` is silenced module-wide.
#![allow(dead_code)]

use std::fmt::{Display, Formatter};

use crate::mv_query::transaction::Transaction;
use crate::mv_tree::mvbt::{MVBTSt, FAN_OUT, NUM_RECORDS};

pub type TpccKey = u64;

/// The single shared tree backing every TPC-C table (see module docs for
/// why one tree, not one per table). Reuses the base tree's page-capacity
/// constants for consistency with the rest of the codebase, even though
/// `TpccRow` is larger than the default `u64` payload the constants were
/// tuned for.
pub const TPCC_FAN_OUT: usize       = FAN_OUT;
pub const TPCC_NUM_RECORDS: usize   = 71;

pub type TpccTree = MVBTSt<TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>;
pub type TpccTxn<'a> = Transaction<'a, TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>;

/// Database scale/shape — the TPC-C spec's standard cardinalities, with
/// warehouse count and a few sizes made configurable for quicker benchmark
/// runs than a full audited scale factor would take to load.
#[derive(Clone, Copy, Debug)]
pub struct TpccConfig {
    pub num_warehouses: u32,
    pub districts_per_warehouse: u8,
    pub customers_per_district: u32,
    pub num_items: u32,
    pub initial_orders_per_district: u32,
    /// The highest-`o_id` slice of `initial_orders_per_district` that gets a
    /// NEW_ORDER row at load time (spec: last 900 of 3,000).
    pub initial_new_orders: u32,
    /// CH-benCHmark's SUPPLIER table size (see `mv_bench::tpch_queries`
    /// module docs): fixed independent of warehouse count, matching
    /// CH-benCHmark's own choice of a TPC-H SF1-sized (10,000-row) supplier
    /// pool regardless of scale factor.
    pub num_suppliers: u32,
}

impl Default for TpccConfig {
    fn default() -> Self {
        Self {
            num_warehouses: 4,
            districts_per_warehouse: 10,
            customers_per_district: 3_000,
            num_items: 100_000,
            initial_orders_per_district: 3_000,
            initial_new_orders: 900,
            num_suppliers: 10_000,
        }
    }
}

/// Table tags occupy the top 4 bits of the 64-bit key (16 slots, 14 used).
mod tag {
    pub const WAREHOUSE: u64 = 0;
    pub const DISTRICT: u64 = 1;
    pub const CUSTOMER: u64 = 2;
    pub const CUSTOMER_NAME_IDX: u64 = 3;
    pub const HISTORY: u64 = 4;
    pub const NEW_ORDER: u64 = 5;
    pub const ORDERS: u64 = 6;
    pub const ORDER_LINE: u64 = 7;
    pub const ITEM: u64 = 8;
    pub const STOCK: u64 = 9;
    /// (w_id, d_id, c_id) -> most recent o_id; a maintained secondary index
    /// standing in for the ORDER table's real secondary index on
    /// (o_w_id,o_d_id,o_c_id,o_id), so OrderStatus can find "the customer's
    /// most recent order" in O(1) instead of a descending scan.
    pub const CUST_LAST_ORDER: u64 = 10;
    /// CH-benCHmark's TPC-H-derived dimension tables (`mv_bench::tpch_queries`
    /// module docs): SUPPLIER links to STOCK via `Stock::s_su_suppkey`,
    /// NATION/REGION are the standard fixed TPC-H reference tables.
    pub const SUPPLIER: u64 = 11;
    pub const NATION: u64 = 12;
    pub const REGION: u64 = 13;
}

const TAG_SHIFT: u32 = 60;
const FIELD_MASK: u64 = (1u64 << TAG_SHIFT) - 1;

// Bit widths for primary-key columns, generous but not maximal: sized for
// benchmark-scale runs (hundreds of warehouses, tens of millions of orders),
// not the TPC-C spec's audited maximums.
const D_ID_BITS: u32 = 4;   // districts/warehouse (spec: 10)
const C_ID_BITS: u32 = 16;  // customers/district (spec: 3,000)
const O_ID_BITS: u32 = 32;  // orders/district over the whole run (grows unboundedly)
const OL_NO_BITS: u32 = 4;  // order-lines/order (spec: 5-15)
const I_ID_BITS: u32 = 24;  // items (spec: 100,000)
const LAST_CODE_BITS: u32 = 10; // C_LAST syllable code, exactly 0..=999
const FIRST_CODE_BITS: u32 = 16; // ordinal surrogate for c_first, tie-break only

#[inline(always)]
const fn with_tag(t: u64, bits: u64) -> TpccKey {
    debug_assert!(bits <= FIELD_MASK);
    (t << TAG_SHIFT) | (bits & FIELD_MASK)
}

#[inline(always)]
const fn table_bounds(t: u64) -> (TpccKey, TpccKey) {
    (with_tag(t, 0), with_tag(t, FIELD_MASK))
}

pub fn warehouse_table_range() -> crate::mv_utils::interval::Interval<TpccKey> {
    let (lo, hi) = table_bounds(tag::WAREHOUSE);
    crate::mv_utils::interval::Interval::new(lo, hi)
}

pub fn district_table_range() -> crate::mv_utils::interval::Interval<TpccKey> {
    let (lo, hi) = table_bounds(tag::DISTRICT);
    crate::mv_utils::interval::Interval::new(lo, hi)
}

pub fn order_line_table_range() -> crate::mv_utils::interval::Interval<TpccKey> {
    let (lo, hi) = table_bounds(tag::ORDER_LINE);
    crate::mv_utils::interval::Interval::new(lo, hi)
}

pub fn stock_table_range() -> crate::mv_utils::interval::Interval<TpccKey> {
    let (lo, hi) = table_bounds(tag::STOCK);
    crate::mv_utils::interval::Interval::new(lo, hi)
}

pub fn orders_table_range() -> crate::mv_utils::interval::Interval<TpccKey> {
    let (lo, hi) = table_bounds(tag::ORDERS);
    crate::mv_utils::interval::Interval::new(lo, hi)
}

pub fn supplier_table_range() -> crate::mv_utils::interval::Interval<TpccKey> {
    let (lo, hi) = table_bounds(tag::SUPPLIER);
    crate::mv_utils::interval::Interval::new(lo, hi)
}

pub fn nation_table_range() -> crate::mv_utils::interval::Interval<TpccKey> {
    let (lo, hi) = table_bounds(tag::NATION);
    crate::mv_utils::interval::Interval::new(lo, hi)
}

pub fn region_table_range() -> crate::mv_utils::interval::Interval<TpccKey> {
    let (lo, hi) = table_bounds(tag::REGION);
    crate::mv_utils::interval::Interval::new(lo, hi)
}

/// Combined "hot tables" range used by the Fig.10-style OLAP scan (scans
/// warehouse+district, the two smallest, most frequently updated tables).
/// Since WAREHOUSE (tag 0) and DISTRICT (tag 1) are adjacent tags, one
/// contiguous range covers exactly both and nothing else.
pub fn warehouse_and_district_range() -> crate::mv_utils::interval::Interval<TpccKey> {
    let (lo, _) = table_bounds(tag::WAREHOUSE);
    let (_, hi) = table_bounds(tag::DISTRICT);
    crate::mv_utils::interval::Interval::new(lo, hi)
}

// ---------------------------------------------------------------------
// Key builders
// ---------------------------------------------------------------------

#[inline(always)]
pub const fn k_warehouse(w_id: u32) -> TpccKey {
    with_tag(tag::WAREHOUSE, w_id as u64)
}

#[inline(always)]
pub const fn k_district(w_id: u32, d_id: u8) -> TpccKey {
    with_tag(tag::DISTRICT, ((w_id as u64) << D_ID_BITS) | d_id as u64)
}

#[inline(always)]
pub const fn k_customer(w_id: u32, d_id: u8, c_id: u32) -> TpccKey {
    with_tag(tag::CUSTOMER,
        ((w_id as u64) << (D_ID_BITS + C_ID_BITS))
            | ((d_id as u64) << C_ID_BITS)
            | c_id as u64)
}

#[inline(always)]
pub const fn k_customer_name_idx(w_id: u32, d_id: u8, last_code: u16, first_code: u16, c_id: u32) -> TpccKey {
    with_tag(tag::CUSTOMER_NAME_IDX,
        ((w_id as u64) << (D_ID_BITS + LAST_CODE_BITS + FIRST_CODE_BITS + C_ID_BITS))
            | ((d_id as u64) << (LAST_CODE_BITS + FIRST_CODE_BITS + C_ID_BITS))
            | ((last_code as u64) << (FIRST_CODE_BITS + C_ID_BITS))
            | ((first_code as u64) << C_ID_BITS)
            | c_id as u64)
}

/// `[lower, upper]` bounds covering every `(first_code, c_id)` for a fixed
/// `(w_id, d_id, last_code)` prefix — used by Payment/OrderStatus's "by last
/// name" lookup.
pub const fn k_customer_name_idx_prefix_bounds(w_id: u32, d_id: u8, last_code: u16) -> (TpccKey, TpccKey) {
    (k_customer_name_idx(w_id, d_id, last_code, 0, 0),
     k_customer_name_idx(w_id, d_id, last_code, u16::MAX, (1 << C_ID_BITS) - 1))
}

/// Extracts `c_id` back out of a customer-name-index key (the low
/// `C_ID_BITS` bits), used once a range scan over
/// `k_customer_name_idx_prefix_bounds` has picked the matching entry.
#[inline(always)]
pub const fn decode_customer_name_idx_c_id(key: TpccKey) -> u32 {
    (key & ((1u64 << C_ID_BITS) - 1)) as u32
}

#[inline(always)]
pub const fn k_item(i_id: u32) -> TpccKey {
    with_tag(tag::ITEM, i_id as u64)
}

#[inline(always)]
pub const fn k_stock(w_id: u32, i_id: u32) -> TpccKey {
    with_tag(tag::STOCK, ((w_id as u64) << I_ID_BITS) | i_id as u64)
}

#[inline(always)]
pub const fn k_order(w_id: u32, d_id: u8, o_id: u32) -> TpccKey {
    with_tag(tag::ORDERS,
        ((w_id as u64) << (D_ID_BITS + O_ID_BITS))
            | ((d_id as u64) << O_ID_BITS)
            | o_id as u64)
}

/// Decodes an ORDERS-table key back into `(w_id, d_id, o_id)` — the inverse
/// of `k_order`, used by `tpch_queries` after a full-table scan to recover
/// each order's identity for its follow-up `order_line` range scan.
#[inline(always)]
pub const fn decode_order_key(key: TpccKey) -> (u32, u8, u32) {
    let bits = key & FIELD_MASK;
    let o_id = (bits & ((1u64 << O_ID_BITS) - 1)) as u32;
    let d_id = ((bits >> O_ID_BITS) & ((1u64 << D_ID_BITS) - 1)) as u8;
    let w_id = (bits >> (O_ID_BITS + D_ID_BITS)) as u32;
    (w_id, d_id, o_id)
}

#[inline(always)]
pub const fn k_new_order(w_id: u32, d_id: u8, o_id: u32) -> TpccKey {
    with_tag(tag::NEW_ORDER,
        ((w_id as u64) << (D_ID_BITS + O_ID_BITS))
            | ((d_id as u64) << O_ID_BITS)
            | o_id as u64)
}

/// `[lower, upper]` bounds covering every `o_id` for a fixed `(w_id, d_id)` —
/// the Delivery transaction's "find the oldest queued new-order" scan.
pub const fn k_new_order_district_bounds(w_id: u32, d_id: u8) -> (TpccKey, TpccKey) {
    (k_new_order(w_id, d_id, 0), k_new_order(w_id, d_id, u32::MAX))
}

#[inline(always)]
pub const fn k_order_line(w_id: u32, d_id: u8, o_id: u32, ol_number: u8) -> TpccKey {
    with_tag(tag::ORDER_LINE,
        ((w_id as u64) << (D_ID_BITS + O_ID_BITS + OL_NO_BITS))
            | ((d_id as u64) << (O_ID_BITS + OL_NO_BITS))
            | ((o_id as u64) << OL_NO_BITS)
            | ol_number as u64)
}

/// `[lower, upper]` bounds covering every `ol_number` (1..=15) of one order.
pub const fn k_order_line_bounds(w_id: u32, d_id: u8, o_id: u32) -> (TpccKey, TpccKey) {
    (k_order_line(w_id, d_id, o_id, 0), k_order_line(w_id, d_id, o_id, u8::MAX))
}

/// Extracts `ol_number` (the low `OL_NO_BITS` bits) back out of an
/// ORDER_LINE key — `tpch_queries::q1` groups by this without going through
/// `k_order_line`'s inputs first (it scans the whole table directly).
#[inline(always)]
pub const fn decode_order_line_number(key: TpccKey) -> u8 {
    (key & ((1u64 << OL_NO_BITS) - 1)) as u8
}

#[inline(always)]
pub const fn k_cust_last_order(w_id: u32, d_id: u8, c_id: u32) -> TpccKey {
    with_tag(tag::CUST_LAST_ORDER,
        ((w_id as u64) << (D_ID_BITS + C_ID_BITS))
            | ((d_id as u64) << C_ID_BITS)
            | c_id as u64)
}

#[inline(always)]
pub fn k_history(seq: u64) -> TpccKey {
    with_tag(tag::HISTORY, seq)
}

#[inline(always)]
pub const fn k_supplier(su_id: u32) -> TpccKey {
    with_tag(tag::SUPPLIER, su_id as u64)
}

#[inline(always)]
pub const fn decode_supplier_id(key: TpccKey) -> u32 {
    (key & FIELD_MASK) as u32
}

#[inline(always)]
pub const fn k_nation(n_id: u8) -> TpccKey {
    with_tag(tag::NATION, n_id as u64)
}

#[inline(always)]
pub const fn decode_nation_id(key: TpccKey) -> u8 {
    (key & FIELD_MASK) as u8
}

#[inline(always)]
pub const fn k_region(r_id: u8) -> TpccKey {
    with_tag(tag::REGION, r_id as u64)
}

#[inline(always)]
pub const fn decode_region_id(key: TpccKey) -> u8 {
    (key & FIELD_MASK) as u8
}

// ---------------------------------------------------------------------
// Row payloads
// ---------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Warehouse {
    pub w_name: String,
    pub w_street_1: String,
    pub w_street_2: String,
    pub w_city: String,
    pub w_state: String,
    pub w_zip: String,
    pub w_tax: f64,
    pub w_ytd: f64,
}

#[derive(Clone, Debug)]
pub struct District {
    pub d_name: String,
    pub d_street_1: String,
    pub d_street_2: String,
    pub d_city: String,
    pub d_state: String,
    pub d_zip: String,
    pub d_tax: f64,
    pub d_ytd: f64,
    pub d_next_o_id: u32,
}

#[derive(Clone, Debug)]
pub struct Customer {
    pub c_first: String,
    pub c_middle: String,
    pub c_last: String,
    pub c_street_1: String,
    pub c_street_2: String,
    pub c_city: String,
    pub c_state: String,
    pub c_zip: String,
    pub c_phone: String,
    pub c_since: i64,
    pub c_credit_bad: bool,
    pub c_credit_lim: f64,
    pub c_discount: f64,
    pub c_balance: f64,
    pub c_ytd_payment: f64,
    pub c_payment_cnt: u32,
    pub c_delivery_cnt: u32,
    pub c_data: String,
}

#[derive(Clone, Debug)]
pub struct History {
    pub h_c_id: u32,
    pub h_c_d_id: u8,
    pub h_c_w_id: u32,
    pub h_d_id: u8,
    pub h_w_id: u32,
    pub h_date: i64,
    pub h_amount: f64,
    pub h_data: String,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NewOrderMarker {
    pub no_o_id: u32,
}

#[derive(Clone, Debug)]
pub struct Order {
    pub o_c_id: u32,
    pub o_entry_d: i64,
    pub o_carrier_id: Option<u32>,
    pub o_ol_cnt: u8,
    pub o_all_local: bool,
}

#[derive(Clone, Debug)]
pub struct OrderLine {
    pub ol_i_id: u32,
    pub ol_supply_w_id: u32,
    pub ol_delivery_d: Option<i64>,
    pub ol_quantity: u8,
    pub ol_amount: f64,
    pub ol_dist_info: String,
}

#[derive(Clone, Debug)]
pub struct Item {
    pub i_im_id: u32,
    pub i_name: String,
    pub i_price: f64,
    pub i_data: String,
}

#[derive(Clone, Debug)]
pub struct Stock {
    pub s_quantity: i32,
    pub s_dist: [String; 10],
    pub s_ytd: f64,
    pub s_order_cnt: u32,
    pub s_remote_cnt: u32,
    pub s_data: String,
    /// CH-benCHmark's addition linking STOCK to SUPPLIER (see
    /// `mv_bench::tpch_queries` module docs): which of the fixed supplier
    /// pool fulfills this `(w_id, i_id)`'s stock, assigned deterministically
    /// at load time (`tpcc_load::su_suppkey_for`).
    pub s_su_suppkey: u32,
}

/// CH-benCHmark's TPC-H-derived SUPPLIER table (standard TPC-H `supplier`
/// columns, minus the unused `s_suppkey`/`s_nationkey` foreign-key
/// decoration this port doesn't need beyond `s_nationkey` itself).
#[derive(Clone, Debug)]
pub struct Supplier {
    pub s_name: String,
    pub s_address: String,
    pub s_nationkey: u8,
    pub s_phone: String,
    pub s_acctbal: f64,
    pub s_comment: String,
}

/// Standard (fixed, 25-row) TPC-H NATION reference table.
#[derive(Clone, Debug)]
pub struct Nation {
    pub n_name: String,
    pub n_regionkey: u8,
    pub n_comment: String,
}

/// Standard (fixed, 5-row) TPC-H REGION reference table.
#[derive(Clone, Debug)]
pub struct Region {
    pub r_name: String,
    pub r_comment: String,
}

/// Payload for the single shared TPC-C tree. The larger row kinds
/// (`Customer`, `Stock`) are boxed so the enum itself — and thus every leaf
/// record, including the small ones (`Warehouse`, `NewOrderMarker`, ...) —
/// stays compact; the same pattern the base tree already uses for large
/// generic payloads (see `mv_test::PayloadIndirection`).
#[derive(Clone, Debug, Default)]
pub enum TpccRow {
    #[default]
    Empty,
    Warehouse(Box<Warehouse>),
    District(Box<District>),
    Customer(Box<Customer>),
    /// Secondary customer-name-index entries store nothing beyond the key
    /// (which already encodes `c_id`); the value is just a presence marker.
    CustomerNameIdx,
    History(Box<History>),
    NewOrder(NewOrderMarker),
    Order(Box<Order>),
    OrderLine(Box<OrderLine>),
    Item(Box<Item>),
    Stock(Box<Stock>),
    /// (w_id,d_id,c_id) -> most recent o_id, see `tag::CUST_LAST_ORDER`.
    CustLastOrder(u32),
    Supplier(Box<Supplier>),
    Nation(Box<Nation>),
    Region(Box<Region>),
}

impl Display for TpccRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TpccRow::Empty => write!(f, "Empty"),
            TpccRow::Warehouse(w) => write!(f, "Warehouse({})", w.w_name),
            TpccRow::District(d) => write!(f, "District({})", d.d_name),
            TpccRow::Customer(c) => write!(f, "Customer({} {})", c.c_first, c.c_last),
            TpccRow::CustomerNameIdx => write!(f, "CustomerNameIdx"),
            TpccRow::History(_) => write!(f, "History"),
            TpccRow::NewOrder(no) => write!(f, "NewOrder(o_id={})", no.no_o_id),
            TpccRow::Order(o) => write!(f, "Order(c_id={}, ol_cnt={})", o.o_c_id, o.o_ol_cnt),
            TpccRow::OrderLine(ol) => write!(f, "OrderLine(i_id={})", ol.ol_i_id),
            TpccRow::Item(i) => write!(f, "Item({})", i.i_name),
            TpccRow::Stock(s) => write!(f, "Stock(qty={})", s.s_quantity),
            TpccRow::CustLastOrder(o_id) => write!(f, "CustLastOrder(o_id={o_id})"),
            TpccRow::Supplier(s) => write!(f, "Supplier({})", s.s_name),
            TpccRow::Nation(n) => write!(f, "Nation({})", n.n_name),
            TpccRow::Region(r) => write!(f, "Region({})", r.r_name),
        }
    }
}

// Field-projection helpers so transaction code doesn't need to match on
// TpccRow everywhere.
impl TpccRow {
    pub fn as_warehouse(&self) -> &Warehouse {
        match self { TpccRow::Warehouse(w) => w, _ => panic!("expected Warehouse row") }
    }
    pub fn as_district(&self) -> &District {
        match self { TpccRow::District(d) => d, _ => panic!("expected District row") }
    }
    pub fn as_customer(&self) -> &Customer {
        match self { TpccRow::Customer(c) => c, _ => panic!("expected Customer row") }
    }
    pub fn as_order(&self) -> &Order {
        match self { TpccRow::Order(o) => o, _ => panic!("expected Order row") }
    }
    pub fn as_order_line(&self) -> &OrderLine {
        match self { TpccRow::OrderLine(ol) => ol, _ => panic!("expected OrderLine row") }
    }
    pub fn as_item(&self) -> &Item {
        match self { TpccRow::Item(i) => i, _ => panic!("expected Item row") }
    }
    pub fn as_stock(&self) -> &Stock {
        match self { TpccRow::Stock(s) => s, _ => panic!("expected Stock row") }
    }
    pub fn as_cust_last_order(&self) -> u32 {
        match self { TpccRow::CustLastOrder(o_id) => *o_id, _ => panic!("expected CustLastOrder row") }
    }
    pub fn as_supplier(&self) -> &Supplier {
        match self { TpccRow::Supplier(s) => s, _ => panic!("expected Supplier row") }
    }
    pub fn as_nation(&self) -> &Nation {
        match self { TpccRow::Nation(n) => n, _ => panic!("expected Nation row") }
    }
    pub fn as_region(&self) -> &Region {
        match self { TpccRow::Region(r) => r, _ => panic!("expected Region row") }
    }
}
