//! Initial TPC-C data-set population (spec §4.3), scaled by [`TpccConfig`].
//!
//! Population uses plain single-op `dispatch_crud` inserts (each already its
//! own tiny auto-committing transaction) rather than one giant `Transaction`
//! per warehouse: a single multi-million-row transaction would hold one
//! OSIC snapshot open for the entire load phase, which is neither necessary
//! (nothing reads the data set until loading is done) nor representative of
//! anything the paper's methodology measures.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use crate::mv_bench::tpcc_random::*;
use crate::mv_bench::tpcc_schema::*;
use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;

#[inline]
fn insert(tree: &TpccTree, key: TpccKey, row: TpccRow) {
    match tree.dispatch_crud(CRUDOperation::Insert(key, row)) {
        CRUDOperationResult::Inserted(_) => {}
        other => panic!("tpcc load: unexpected insert result for key {key}: {other}"),
    }
}

/// Standard (fixed) TPC-H `region`/`nation` reference data, reused as-is by
/// CH-benCHmark (see `mv_bench::tpch_queries` module docs): 5 regions, 25
/// nations, `(name, regionkey)` pairs indexed by their position (`nationkey`).
const REGIONS: [&str; 5] = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];

const NATIONS: [(&str, u8); 25] = [
    ("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("EGYPT", 4),
    ("ETHIOPIA", 0), ("FRANCE", 3), ("GERMANY", 3), ("INDIA", 2), ("INDONESIA", 2),
    ("IRAN", 4), ("IRAQ", 4), ("JAPAN", 2), ("JORDAN", 4), ("KENYA", 0),
    ("MOROCCO", 0), ("MOZAMBIQUE", 0), ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3),
    ("SAUDI ARABIA", 4), ("VIETNAM", 2), ("RUSSIA", 3), ("UNITED KINGDOM", 3), ("UNITED STATES", 1),
];

/// CH-benCHmark's deterministic STOCK -> SUPPLIER assignment (see
/// `mv_bench::tpch_queries` module docs): every `(w_id, i_id)` maps to
/// exactly one of the `num_suppliers` suppliers, spreading suppliers evenly
/// across stock rows without needing a separate mapping table.
#[inline]
pub fn su_suppkey_for(w_id: u32, i_id: u32, num_suppliers: u32) -> u32 {
    ((w_id as u64 * i_id as u64) % num_suppliers.max(1) as u64) as u32
}

/// Loads the fixed TPC-H REGION (5 rows) and NATION (25 rows) tables; call
/// once regardless of scale.
pub fn populate_regions_and_nations(db: &TpccDatabase) {
    for (r_id, name) in REGIONS.iter().enumerate() {
        insert(&db.tree_for(Table::Region), k_region(r_id as u8), TpccRow::Region(Box::new(Region {
            r_name: name.to_string(),
            r_comment: rnd_astring(20, 80),
        })));
    }

    for (n_id, (name, r_id)) in NATIONS.iter().enumerate() {
        insert(&db.tree_for(Table::Nation), k_nation(n_id as u8), TpccRow::Nation(Box::new(Nation {
            n_name: name.to_string(),
            n_regionkey: *r_id,
            n_comment: rnd_astring(20, 80),
        })));
    }
}

/// Loads `cfg.num_suppliers` rows (CH-benCHmark's fixed-size SUPPLIER pool,
/// see `TpccConfig::num_suppliers`), each assigned a uniformly random nation.
pub fn populate_suppliers(db: &TpccDatabase, cfg: &TpccConfig) {
    for su_id in 0..cfg.num_suppliers {
        insert(&db.tree_for(Table::Supplier), k_supplier(su_id), TpccRow::Supplier(Box::new(Supplier {
            s_name: format!("Supplier#{:09}", su_id),
            s_address: rnd_astring(10, 40),
            s_nationkey: with_fast_rng(|rng| rng.u8(0..NATIONS.len() as u8)),
            s_phone: rnd_phone(),
            s_acctbal: with_fast_rng(|rng| rng.i32(-99999..=999999)) as f64 / 100.0,
            s_comment: rnd_astring(20, 100),
        })));
    }
}

/// Warehouse-independent item catalog; call once regardless of `num_warehouses`.
pub fn populate_items(db: &TpccDatabase, cfg: &TpccConfig) {
    for i_id in 1..=cfg.num_items {
        insert(&db.tree_for(Table::Item), k_item(i_id), TpccRow::Item(Box::new(Item {
            i_im_id: with_fast_rng(|rng| rng.u32(1..=10_000)),
            i_name: rnd_astring(14, 24),
            i_price: with_fast_rng(|rng| rng.i32(100..=10_000)) as f64 / 100.0,
            i_data: rnd_original_data(26, 50),
        })));
    }
}

/// Populates one warehouse's rows: itself, its districts, customers (+ name
/// index + one history row each), initial orders/order-lines/new-orders, and
/// its per-item stock. `history_seq` is a process-wide counter shared by
/// every loader thread so History keys never collide across warehouses.
pub fn populate_warehouse(db: &TpccDatabase, cfg: &TpccConfig, w_id: u32, history_seq: &AtomicU64) {
    insert(&db.tree_for(Table::Warehouse), k_warehouse(w_id), TpccRow::Warehouse(Box::new(Warehouse {
        w_name: rnd_astring(6, 10),
        w_street_1: rnd_astring(10, 20),
        w_street_2: rnd_astring(10, 20),
        w_city: rnd_astring(10, 20),
        w_state: rnd_astring(2, 2),
        w_zip: rnd_zip(),
        w_tax: with_fast_rng(|rng| rng.i32(0..=2000)) as f64 / 10000.0,
        w_ytd: 300_000.0,
    })));

    for d_id in 1..=cfg.districts_per_warehouse {
        insert(&db.tree_for(Table::District), k_district(w_id, d_id), TpccRow::District(Box::new(District {
            d_name: rnd_astring(6, 10),
            d_street_1: rnd_astring(10, 20),
            d_street_2: rnd_astring(10, 20),
            d_city: rnd_astring(10, 20),
            d_state: rnd_astring(2, 2),
            d_zip: rnd_zip(),
            d_tax: with_fast_rng(|rng| rng.i32(0..=2000)) as f64 / 10000.0,
            d_ytd: 30_000.0,
            d_next_o_id: cfg.initial_orders_per_district + 1,
        })));

        for c_ord in 0..cfg.customers_per_district {
            let c_id = c_ord + 1;
            let last_code = c_last_code_for_load(c_ord);
            let c_last = gen_last_name(last_code);
            let c_first = rnd_astring(8, 16);
            let first_code_v = first_code(&c_first);
            let c_credit_bad = with_fast_rng(|rng| rng.u32(0..10)) == 0;

            insert(&db.tree_for(Table::Customer), k_customer(w_id, d_id, c_id), TpccRow::Customer(Box::new(Customer {
                c_first,
                c_middle: "OE".to_string(),
                c_last,
                c_street_1: rnd_astring(10, 20),
                c_street_2: rnd_astring(10, 20),
                c_city: rnd_astring(10, 20),
                c_state: rnd_astring(2, 2),
                c_zip: rnd_zip(),
                c_phone: rnd_phone(),
                c_since: now_millis(),
                c_credit_bad,
                c_credit_lim: 50_000.0,
                c_discount: with_fast_rng(|rng| rng.i32(0..=5000)) as f64 / 10000.0,
                c_balance: -10.0,
                c_ytd_payment: 10.0,
                c_payment_cnt: 1,
                c_delivery_cnt: 0,
                c_data: rnd_astring(300, 500),
            })));
            insert(&db.tree_for(Table::CustomerNameIdx), k_customer_name_idx(w_id, d_id, last_code, first_code_v, c_id), TpccRow::CustomerNameIdx);

            let h_key = k_history(history_seq.fetch_add(1, Relaxed));
            insert(&db.tree_for(Table::History), h_key, TpccRow::History(Box::new(History {
                h_c_id: c_id,
                h_c_d_id: d_id,
                h_c_w_id: w_id,
                h_d_id: d_id,
                h_w_id: w_id,
                h_date: now_millis(),
                h_amount: 10.0,
                h_data: rnd_astring(12, 24),
            })));
        }

        // Order ids are assigned as a random permutation of customer ids,
        // per spec §4.3.3.1.
        let mut c_ids: Vec<u32> = (1..=cfg.customers_per_district).collect();
        with_fast_rng(|rng| rng.shuffle(&mut c_ids));

        let new_order_floor = cfg.initial_orders_per_district.saturating_sub(cfg.initial_new_orders);

        for o_ord in 0..cfg.initial_orders_per_district {
            let o_id = o_ord + 1;
            let c_id = c_ids[o_ord as usize];
            let ol_cnt = with_fast_rng(|rng| rng.u8(5..=15));
            let is_new = o_id > new_order_floor;
            let o_carrier_id = if is_new { None } else { Some(with_fast_rng(|rng| rng.u32(1..=10))) };

            insert(&db.tree_for(Table::Orders), k_order(w_id, d_id, o_id), TpccRow::Order(Box::new(Order {
                o_c_id: c_id,
                o_entry_d: now_millis(),
                o_carrier_id,
                o_ol_cnt: ol_cnt,
                o_all_local: true,
            })));
            insert(&db.tree_for(Table::CustLastOrder), k_cust_last_order(w_id, d_id, c_id), TpccRow::CustLastOrder(o_id));

            for ol_number in 1..=ol_cnt {
                let i_id = with_fast_rng(|rng| rng.u32(1..=cfg.num_items));
                let (ol_delivery_d, ol_amount) = if is_new {
                    (None, with_fast_rng(|rng| rng.i32(100..=999_999)) as f64 / 100.0)
                } else {
                    (Some(now_millis()), 0.0)
                };

                insert(&db.tree_for(Table::OrderLine), k_order_line(w_id, d_id, o_id, ol_number), TpccRow::OrderLine(Box::new(OrderLine {
                    ol_i_id: i_id,
                    ol_supply_w_id: w_id,
                    ol_delivery_d,
                    ol_quantity: 5,
                    ol_amount,
                    ol_dist_info: rnd_astring(24, 24),
                })));
            }

            if is_new {
                insert(&db.tree_for(Table::NewOrder), k_new_order(w_id, d_id, o_id), TpccRow::NewOrder(NewOrderMarker { no_o_id: o_id }));
            }
        }
    }

    for i_id in 1..=cfg.num_items {
        insert(&db.tree_for(Table::Stock), k_stock(w_id, i_id), TpccRow::Stock(Box::new(Stock {
            s_quantity: with_fast_rng(|rng| rng.i32(10..=100)),
            s_dist: std::array::from_fn(|_| rnd_astring(24, 24)),
            s_ytd: 0.0,
            s_order_cnt: 0,
            s_remote_cnt: 0,
            s_data: rnd_original_data(26, 50),
            s_su_suppkey: su_suppkey_for(w_id, i_id, cfg.num_suppliers),
        })));
    }
}
