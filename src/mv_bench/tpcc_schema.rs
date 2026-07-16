//! TPC-C schema for the cMVBT benchmark harness.
//!
//! Each of the nine TPC-C tables (plus two maintained secondary indexes) gets
//! its own [`MVBTSt`] tree/index — a [`TpccDatabase`] is a thin, domain-named
//! wrapper around a [`crate::mv_db::Database`] holding all 14 as named
//! tables, sharing that database's one transactional core (`TxContext`) *and*
//! its one shared WAL. Sharing the transactional core — not sharing one
//! physical tree — is what lets a [`crate::mv_bench::tpcc_txn::TpccTxn`]
//! spanning several "tables" (e.g. NewOrder touching Warehouse/District/
//! Customer/Order/NewOrder/OrderLine/Stock) commit atomically and stay
//! snapshot-isolated as a whole, matching how the referenced benchmarks
//! (TPC-C + OLAP scans, e.g. Alhomssi & Leis, VLDB'23) treat a business
//! transaction as one unit — while giving each table an independently-sized,
//! independently-scanned index, closer to how a real storage engine
//! physically separates relations. Sharing the WAL means a `TpccTxn`
//! spanning several tables now logs exactly *one* Commit marker for the
//! whole transaction (see `crate::mv_db::DbTransaction::commit`'s doc),
//! instead of one marker per touched table.
//!
//! [`Table`] is a convenience enum over this database's 14 tables — it
//! carries no data of its own; `TpccDatabase` resolves it to a `TableId`
//! (the underlying `Database`'s actual, plain sequential per-table index)
//! once at construction, cached in `TpccDatabase::table_ids` and indexed by
//! `Table as usize` (safe: `Table`'s declaration order matches `Table::ALL`'s).
//!
//! Also carries CH-benCHmark's (Cole et al., "The Mixed Workload CH-benCHmark",
//! DBTest 2011) three TPC-H-derived dimension tables — SUPPLIER, NATION,
//! REGION — as three more tables on the same `TpccDatabase`, feeding the
//! analytical queries in `mv_bench::tpch_queries`.
//!
//! Key layout: since table selection is now "which table" (a [`Table`] value
//! resolving to a `TableId`), not "which key range", every key is just that
//! table's primary-key columns packed MSB-first (so a byte-ordered range scan
//! matches the natural column order, e.g. scanning all districts of a
//! warehouse or all order-lines of an order) — no table tag bits needed.
//!
//! Several row fields (addresses, `i_data`, `s_dist`, ...) are never read by
//! the 5 transaction profiles, same as in the real spec — they exist for
//! realistic row footprint (page density, leaf fan-out) rather than being
//! touched by transaction logic, so `dead_code` is silenced module-wide.
#![allow(dead_code)]

use crate::mv_db::Database;
use crate::mv_query::interval::Interval;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_tree::mvbt::FAN_OUT;
use crate::mv_wal::record::TableId;
use std::fmt::{Display, Formatter};
use triomphe::Arc;

pub type TpccKey = u64;

/// A single table's tree. Reuses the base tree's `FAN_OUT` for consistency
/// with the rest of the codebase, but `NUM_RECORDS` is recomputed
/// separately: `RecordPoint<TpccKey, TpccRow>` is 40B (`TpccRow` is bigger
/// than the base tree's `u64` payload), so it targets the same ~4000B leaf
/// record-array budget `FAN_OUT`'s internal-node arrays and the base tree's
/// `NUM_RECORDS` use, not the base tree's own record count.
pub const TPCC_FAN_OUT: usize       = FAN_OUT;
pub const TPCC_NUM_RECORDS: usize   = 100;

pub type TpccTree = crate::mv_tree::mvbt::MVBTSt<TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>;

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

/// Selects one of `TpccDatabase`'s 14 tables/trees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Table {
    Warehouse,
    District,
    Customer,
    /// A maintained secondary index standing in for the ORDER table's real
    /// secondary index on (o_w_id,o_d_id,o_c_id,o_id), so OrderStatus can
    /// find "the customer's most recent order" in O(1) instead of a
    /// descending scan.
    CustLastOrder,
    /// (w_id, d_id, last_code, first_code, c_id) -> presence marker; the
    /// "by last name" customer lookup Payment/OrderStatus need.
    CustomerNameIdx,
    History,
    NewOrder,
    Orders,
    OrderLine,
    Item,
    Stock,
    /// CH-benCHmark's TPC-H-derived dimension tables (`mv_bench::tpch_queries`
    /// module docs): SUPPLIER links to STOCK via `Stock::s_su_suppkey`,
    /// NATION/REGION are the standard fixed TPC-H reference tables.
    Supplier,
    Nation,
    Region,
}

impl Table {
    pub const ALL: [Table; 14] = [
        Table::Warehouse, Table::District, Table::Customer, Table::CustLastOrder,
        Table::CustomerNameIdx, Table::History, Table::NewOrder, Table::Orders,
        Table::OrderLine, Table::Item, Table::Stock, Table::Supplier, Table::Nation,
        Table::Region,
    ];

    /// Lowercase name — this table's actual identity, used as the
    /// `mv_db::Database::create_table` argument `TpccDatabase` resolves
    /// every `Table` variant to a `TableId` through, and for diagnostics.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Table::Warehouse => "warehouse",
            Table::District => "district",
            Table::Customer => "customer",
            Table::CustLastOrder => "cust_last_order",
            Table::CustomerNameIdx => "customer_name_idx",
            Table::History => "history",
            Table::NewOrder => "new_order",
            Table::Orders => "orders",
            Table::OrderLine => "order_line",
            Table::Item => "item",
            Table::Stock => "stock",
            Table::Supplier => "supplier",
            Table::Nation => "nation",
            Table::Region => "region",
        }
    }
}

/// All 14 TPC-C/CH-benCHmark tables — a thin, domain-named wrapper over a
/// [`crate::mv_db::Database`], see this module's doc.
pub struct TpccDatabase {
    pub(crate) db: Database<TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>,
    /// `Table -> TableId`, resolved once at construction and indexed by
    /// `Table as usize` — see `Table`'s doc.
    pub(crate) table_ids: [TableId; 14],
}

fn inc_key(k: TpccKey) -> TpccKey { k.checked_add(1).unwrap_or(TpccKey::MAX) }
fn dec_key(k: TpccKey) -> TpccKey { k.checked_sub(1).unwrap_or(TpccKey::MIN) }

impl TpccDatabase {
    pub fn new(root_index_type: RootIndexType) -> Self {
        let db = Database::new(root_index_type, inc_key, dec_key, TpccKey::MIN, TpccKey::MAX);
        let table_ids = Self::create_all_tables(&db);
        Self { db, table_ids }
    }

    /// Creates every one of the 14 tables, in `Table::ALL`'s fixed order —
    /// or, for a database recovered from an already-populated catalog,
    /// simply looks each one up (`Database::create_table` is idempotent by
    /// name, see its doc) — and returns the resulting `Table -> TableId`
    /// cache. Shared by `new` (always actually creates) and `open_recovered`
    /// (recreates from the catalog `Database::open_recovered` already read;
    /// this loop is then a no-op lookup for every name already present, or
    /// a real create for a genuinely fresh — no prior WAL — database).
    fn create_all_tables(db: &Database<TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>) -> [TableId; 14] {
        let mut table_ids = [0 as TableId; 14];
        for t in Table::ALL {
            table_ids[t as usize] = db.create_table(t.as_str())
                .table_id()
                .expect("mv_db::Database::create_table always assigns its new table a TableId");
        }
        table_ids
    }

    #[inline(always)]
    pub fn tree_for(&self, table: Table) -> Arc<TpccTree> {
        self.db.table(self.table_ids[table as usize])
            .expect("TpccDatabase creates every Table::ALL entry at construction")
    }

    /// Reads off the shared clock — same value regardless of which table's
    /// tree it's read through, since every table shares this database's one
    /// `ctx`.
    pub fn current_version(&self) -> crate::mv_record_model::version_info::Version {
        self.db.current_version()
    }

    /// Toggles block reclaim uniformly across every table on this database
    /// — see `MVBTSt::enable_gc`'s doc for why partial/per-table toggling
    /// would make pruning the shared commit logs unsound.
    pub fn enable_gc(&self, update_in_place: bool) {
        self.db.enable_gc(update_in_place);
    }

    pub fn disable_gc(&self) {
        self.db.disable_gc();
    }

    pub fn truncate_commit_log(&self, enabled: bool) {
        self.db.allow_historic_query(enabled);
    }

    pub fn root_star_index(&self) -> RootIndexType {
        self.db.root_star_index()
    }
}

impl TpccDatabase {
    /// Builds a fresh database, replays the *single* shared WAL file found
    /// at `wal_path` — via `mv_db::Database::open_recovered`, which reads
    /// its own table catalog to know which tables to recreate, in their
    /// original order, with no per-table file/path bookkeeping needed here
    /// — then attaches a live writer. The `TpccDatabase` counterpart to
    /// `mv_db::Database::open_recovered`. Unlike the old one-file-per-table
    /// design, a `TpccTxn` spanning several tables now logs exactly one
    /// Commit marker for the whole transaction (see
    /// `mv_db::DbTransaction::commit`'s doc), so recovery no longer has the
    /// old "a crash between two tables' markers can leave one table's share
    /// of a transaction replayed and another's not" gap.
    pub fn open_recovered(
        root_index_type: RootIndexType,
        wal_path: &std::path::Path,
        flush_interval: std::time::Duration,
    ) -> std::io::Result<Self> {
        let db = Database::open_recovered(
            root_index_type, inc_key, dec_key, TpccKey::MIN, TpccKey::MAX,
            wal_path, flush_interval,
        )?;
        let table_ids = Self::create_all_tables(&db);
        Ok(Self { db, table_ids })
    }

    /// Attaches one shared live WAL at `wal_path` — for a fresh (not
    /// recovered) database; use `open_recovered` instead when the log might
    /// already contain data from a prior run.
    pub fn enable_wal(&self, wal_path: &std::path::Path, flush_interval: std::time::Duration) -> std::io::Result<()> {
        self.db.enable_wal(wal_path, flush_interval)
    }
}

// ---------------------------------------------------------------------
// Table range helpers
// ---------------------------------------------------------------------
//
// Table selection is now "which tree" (see `Table`/`TpccDatabase::tree_for`),
// not "which key range", so every one of these is just the trivial
// full-range scan of that table's own tree. Kept as thin named wrappers so
// `olap_scan.rs`/`tpch_queries.rs` call sites don't change shape, just their
// target tree.

#[inline(always)]
fn full_range() -> Interval<TpccKey> {
    Interval::new(TpccKey::MIN, TpccKey::MAX)
}

pub fn warehouse_table_range() -> Interval<TpccKey> { full_range() }
pub fn district_table_range() -> Interval<TpccKey> { full_range() }
pub fn order_line_table_range() -> Interval<TpccKey> { full_range() }
pub fn stock_table_range() -> Interval<TpccKey> { full_range() }
pub fn orders_table_range() -> Interval<TpccKey> { full_range() }
pub fn supplier_table_range() -> Interval<TpccKey> { full_range() }
pub fn nation_table_range() -> Interval<TpccKey> { full_range() }
pub fn region_table_range() -> Interval<TpccKey> { full_range() }

// ---------------------------------------------------------------------
// Key builders
// ---------------------------------------------------------------------

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
pub const fn k_warehouse(w_id: u32) -> TpccKey {
    w_id as u64
}

#[inline(always)]
pub const fn k_district(w_id: u32, d_id: u8) -> TpccKey {
    ((w_id as u64) << D_ID_BITS) | d_id as u64
}

#[inline(always)]
pub const fn k_customer(w_id: u32, d_id: u8, c_id: u32) -> TpccKey {
    ((w_id as u64) << (D_ID_BITS + C_ID_BITS))
        | ((d_id as u64) << C_ID_BITS)
        | c_id as u64
}

#[inline(always)]
pub const fn k_customer_name_idx(w_id: u32, d_id: u8, last_code: u16, first_code: u16, c_id: u32) -> TpccKey {
    ((w_id as u64) << (D_ID_BITS + LAST_CODE_BITS + FIRST_CODE_BITS + C_ID_BITS))
        | ((d_id as u64) << (LAST_CODE_BITS + FIRST_CODE_BITS + C_ID_BITS))
        | ((last_code as u64) << (FIRST_CODE_BITS + C_ID_BITS))
        | ((first_code as u64) << C_ID_BITS)
        | c_id as u64
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
    i_id as u64
}

#[inline(always)]
pub const fn k_stock(w_id: u32, i_id: u32) -> TpccKey {
    ((w_id as u64) << I_ID_BITS) | i_id as u64
}

#[inline(always)]
pub const fn k_order(w_id: u32, d_id: u8, o_id: u32) -> TpccKey {
    ((w_id as u64) << (D_ID_BITS + O_ID_BITS))
        | ((d_id as u64) << O_ID_BITS)
        | o_id as u64
}

/// Decodes an ORDERS-table key back into `(w_id, d_id, o_id)` — the inverse
/// of `k_order`, used by `tpch_queries` after a full-table scan to recover
/// each order's identity for its follow-up `order_line` range scan.
#[inline(always)]
pub const fn decode_order_key(key: TpccKey) -> (u32, u8, u32) {
    let o_id = (key & ((1u64 << O_ID_BITS) - 1)) as u32;
    let d_id = ((key >> O_ID_BITS) & ((1u64 << D_ID_BITS) - 1)) as u8;
    let w_id = (key >> (O_ID_BITS + D_ID_BITS)) as u32;
    (w_id, d_id, o_id)
}

#[inline(always)]
pub const fn k_new_order(w_id: u32, d_id: u8, o_id: u32) -> TpccKey {
    ((w_id as u64) << (D_ID_BITS + O_ID_BITS))
        | ((d_id as u64) << O_ID_BITS)
        | o_id as u64
}

/// `[lower, upper]` bounds covering every `o_id` for a fixed `(w_id, d_id)` —
/// the Delivery transaction's "find the oldest queued new-order" scan.
pub const fn k_new_order_district_bounds(w_id: u32, d_id: u8) -> (TpccKey, TpccKey) {
    (k_new_order(w_id, d_id, 0), k_new_order(w_id, d_id, u32::MAX))
}

#[inline(always)]
pub const fn k_order_line(w_id: u32, d_id: u8, o_id: u32, ol_number: u8) -> TpccKey {
    ((w_id as u64) << (D_ID_BITS + O_ID_BITS + OL_NO_BITS))
        | ((d_id as u64) << (O_ID_BITS + OL_NO_BITS))
        | ((o_id as u64) << OL_NO_BITS)
        | ol_number as u64
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
    ((w_id as u64) << (D_ID_BITS + C_ID_BITS))
        | ((d_id as u64) << C_ID_BITS)
        | c_id as u64
}

#[inline(always)]
pub fn k_history(seq: u64) -> TpccKey {
    seq
}

#[inline(always)]
pub const fn k_supplier(su_id: u32) -> TpccKey {
    su_id as u64
}

#[inline(always)]
pub const fn decode_supplier_id(key: TpccKey) -> u32 {
    key as u32
}

#[inline(always)]
pub const fn k_nation(n_id: u8) -> TpccKey {
    n_id as u64
}

#[inline(always)]
pub const fn decode_nation_id(key: TpccKey) -> u8 {
    key as u8
}

#[inline(always)]
pub const fn k_region(r_id: u8) -> TpccKey {
    r_id as u64
}

#[inline(always)]
pub const fn decode_region_id(key: TpccKey) -> u8 {
    key as u8
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

/// Payload shared by every one of `TpccDatabase`'s 14 tables — the larger row
/// kinds (`Customer`, `Stock`) are boxed so the enum itself — and thus every
/// leaf record, including the small ones (`Warehouse`, `NewOrderMarker`, ...)
/// — stays compact; the same pattern the base tree already uses for large
/// generic payloads (see `mv_test::PayloadIndirection`). Keeping one shared
/// enum (rather than a distinct native Rust struct payload per table) is
/// what lets every table be the same monomorphized `TpccTree`, so
/// `TpccDatabase` can be a plain struct of same-typed fields and
/// `mv_bench::tpcc_txn::TpccTxn` a single uniform transaction type reused for
/// every table.
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
    /// (w_id,d_id,c_id) -> most recent o_id, see `Table::CustLastOrder`.
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
