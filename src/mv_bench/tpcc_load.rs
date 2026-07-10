//! Initial TPC-C data-set population (spec §4.3), scaled by [`TpccConfig`].
//!
//! Population uses plain single-op `dispatch_crud` inserts (each already its
//! own tiny auto-committing transaction) rather than one giant `Transaction`
//! per warehouse: a single multi-million-row transaction would hold one
//! OSIC snapshot open for the entire load phase, which is neither necessary
//! (nothing reads the data set until loading is done) nor representative of
//! anything the paper's methodology measures.

use rand::prelude::*;
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

/// Warehouse-independent item catalog; call once regardless of `num_warehouses`.
pub fn populate_items(tree: &TpccTree, cfg: &TpccConfig) {
    for i_id in 1..=cfg.num_items {
        insert(tree, k_item(i_id), TpccRow::Item(Box::new(Item {
            i_im_id: rand::rng().random_range(1..=10_000),
            i_name: rnd_astring(14, 24),
            i_price: rand::rng().random_range(100..=10_000) as f64 / 100.0,
            i_data: rnd_original_data(26, 50),
        })));
    }
}

/// Populates one warehouse's rows: itself, its districts, customers (+ name
/// index + one history row each), initial orders/order-lines/new-orders, and
/// its per-item stock. `history_seq` is a process-wide counter shared by
/// every loader thread so History keys never collide across warehouses.
pub fn populate_warehouse(tree: &TpccTree, cfg: &TpccConfig, w_id: u32, history_seq: &AtomicU64) {
    insert(tree, k_warehouse(w_id), TpccRow::Warehouse(Box::new(Warehouse {
        w_name: rnd_astring(6, 10),
        w_street_1: rnd_astring(10, 20),
        w_street_2: rnd_astring(10, 20),
        w_city: rnd_astring(10, 20),
        w_state: rnd_astring(2, 2),
        w_zip: rnd_zip(),
        w_tax: rand::rng().random_range(0..=2000) as f64 / 10000.0,
        w_ytd: 300_000.0,
    })));

    for d_id in 1..=cfg.districts_per_warehouse {
        insert(tree, k_district(w_id, d_id), TpccRow::District(Box::new(District {
            d_name: rnd_astring(6, 10),
            d_street_1: rnd_astring(10, 20),
            d_street_2: rnd_astring(10, 20),
            d_city: rnd_astring(10, 20),
            d_state: rnd_astring(2, 2),
            d_zip: rnd_zip(),
            d_tax: rand::rng().random_range(0..=2000) as f64 / 10000.0,
            d_ytd: 30_000.0,
            d_next_o_id: cfg.initial_orders_per_district + 1,
        })));

        for c_ord in 0..cfg.customers_per_district {
            let c_id = c_ord + 1;
            let last_code = c_last_code_for_load(c_ord);
            let c_last = gen_last_name(last_code);
            let c_first = rnd_astring(8, 16);
            let first_code_v = first_code(&c_first);
            let c_credit_bad = rand::rng().random_range(0..10) == 0;

            insert(tree, k_customer(w_id, d_id, c_id), TpccRow::Customer(Box::new(Customer {
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
                c_discount: rand::rng().random_range(0..=5000) as f64 / 10000.0,
                c_balance: -10.0,
                c_ytd_payment: 10.0,
                c_payment_cnt: 1,
                c_delivery_cnt: 0,
                c_data: rnd_astring(300, 500),
            })));
            insert(tree, k_customer_name_idx(w_id, d_id, last_code, first_code_v, c_id), TpccRow::CustomerNameIdx);

            let h_key = k_history(history_seq.fetch_add(1, Relaxed));
            insert(tree, h_key, TpccRow::History(Box::new(History {
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
        c_ids.shuffle(&mut rand::rng());

        let new_order_floor = cfg.initial_orders_per_district.saturating_sub(cfg.initial_new_orders);

        for o_ord in 0..cfg.initial_orders_per_district {
            let o_id = o_ord + 1;
            let c_id = c_ids[o_ord as usize];
            let ol_cnt = rand::rng().random_range(5..=15u8);
            let is_new = o_id > new_order_floor;
            let o_carrier_id = if is_new { None } else { Some(rand::rng().random_range(1..=10u32)) };

            insert(tree, k_order(w_id, d_id, o_id), TpccRow::Order(Box::new(Order {
                o_c_id: c_id,
                o_entry_d: now_millis(),
                o_carrier_id,
                o_ol_cnt: ol_cnt,
                o_all_local: true,
            })));
            insert(tree, k_cust_last_order(w_id, d_id, c_id), TpccRow::CustLastOrder(o_id));

            for ol_number in 1..=ol_cnt {
                let i_id = rand::rng().random_range(1..=cfg.num_items);
                let (ol_delivery_d, ol_amount) = if is_new {
                    (None, rand::rng().random_range(100..=999_999) as f64 / 100.0)
                } else {
                    (Some(now_millis()), 0.0)
                };

                insert(tree, k_order_line(w_id, d_id, o_id, ol_number), TpccRow::OrderLine(Box::new(OrderLine {
                    ol_i_id: i_id,
                    ol_supply_w_id: w_id,
                    ol_delivery_d,
                    ol_quantity: 5,
                    ol_amount,
                    ol_dist_info: rnd_astring(24, 24),
                })));
            }

            if is_new {
                insert(tree, k_new_order(w_id, d_id, o_id), TpccRow::NewOrder(NewOrderMarker { no_o_id: o_id }));
            }
        }
    }

    for i_id in 1..=cfg.num_items {
        insert(tree, k_stock(w_id, i_id), TpccRow::Stock(Box::new(Stock {
            s_quantity: rand::rng().random_range(10..=100),
            s_dist: std::array::from_fn(|_| rnd_astring(24, 24)),
            s_ytd: 0.0,
            s_order_cnt: 0,
            s_remote_cnt: 0,
            s_data: rnd_original_data(26, 50),
        })));
    }
}
