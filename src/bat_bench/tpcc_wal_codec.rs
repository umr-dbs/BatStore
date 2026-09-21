//! Real (variable-length) WAL encoding for [`TpccRow`], implementing
//! `bat_wal::record::WalPayload`. `TpccRow`'s large variants box `String`-
//! bearing structs (`Customer`, `Stock`, ...), so the WAL's original
//! fixed-size raw-memcpy encoding (`write_raw`/`read_raw`, still used for
//! `Key`/the base `Payload = u64` case) would write out heap pointers
//! instead of the data they point to — unsound the instant the recovered
//! value is read, cloned, or dropped. Every variant gets a tag byte plus its
//! own explicit field encoding instead.

use crate::bat_bench::tpcc_schema::*;
use crate::bat_wal::record::WalPayload;

const TAG_EMPTY: u8 = 0;
const TAG_WAREHOUSE: u8 = 1;
const TAG_DISTRICT: u8 = 2;
const TAG_CUSTOMER: u8 = 3;
const TAG_CUSTOMER_NAME_IDX: u8 = 4;
const TAG_HISTORY: u8 = 5;
const TAG_NEW_ORDER: u8 = 6;
const TAG_ORDER: u8 = 7;
const TAG_ORDER_LINE: u8 = 8;
const TAG_ITEM: u8 = 9;
const TAG_STOCK: u8 = 10;
const TAG_CUST_LAST_ORDER: u8 = 11;
const TAG_SUPPLIER: u8 = 12;
const TAG_NATION: u8 = 13;
const TAG_REGION: u8 = 14;

struct Writer<'a>(&'a mut Vec<u8>);

impl<'a> Writer<'a> {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn bool(&mut self, v: bool) {
        self.0.push(v as u8);
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn i32(&mut self, v: i32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn f64(&mut self, v: f64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }

    fn str(&mut self, s: &str) {
        self.u32(s.len() as u32);
        self.0.extend_from_slice(s.as_bytes());
    }

    /// Raw fixed-width bytes, no length prefix - for fields whose length is
    /// already known at both encode and decode time (e.g. `Stock::s_dist`'s
    /// `[u8; 24]` entries), unlike `str`'s variable-length framing.
    fn fixed<const N: usize>(&mut self, v: &[u8; N]) {
        self.0.extend_from_slice(v);
    }

    fn opt_u32(&mut self, v: Option<u32>) {
        match v {
            Some(x) => {
                self.bool(true);
                self.u32(x);
            }
            None => self.bool(false),
        }
    }

    fn opt_i64(&mut self, v: Option<i64>) {
        match v {
            Some(x) => {
                self.bool(true);
                self.i64(x);
            }
            None => self.bool(false),
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let s = self.bytes.get(self.pos..self.pos + n)?;
        self.pos += n;
        Some(s)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn bool(&mut self) -> Option<bool> {
        Some(self.u8()? != 0)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn i32(&mut self) -> Option<i32> {
        Some(i32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn i64(&mut self) -> Option<i64> {
        Some(i64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn f64(&mut self) -> Option<f64> {
        Some(f64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn str(&mut self) -> Option<String> {
        let len = self.u32()? as usize;
        std::str::from_utf8(self.take(len)?)
            .ok()
            .map(str::to_string)
    }

    fn fixed<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N)?.try_into().ok()
    }

    fn opt_u32(&mut self) -> Option<Option<u32>> {
        if self.bool()? {
            Some(Some(self.u32()?))
        } else {
            Some(None)
        }
    }

    fn opt_i64(&mut self) -> Option<Option<i64>> {
        if self.bool()? {
            Some(Some(self.i64()?))
        } else {
            Some(None)
        }
    }
}

impl WalPayload for TpccRow {
    fn wal_encode(&self, out: &mut Vec<u8>) {
        let mut w = Writer(out);
        match self {
            TpccRow::Empty => w.u8(TAG_EMPTY),
            TpccRow::Warehouse(x) => {
                w.u8(TAG_WAREHOUSE);
                w.str(&x.w_name);
                w.str(&x.w_street_1);
                w.str(&x.w_street_2);
                w.str(&x.w_city);
                w.str(&x.w_state);
                w.str(&x.w_zip);
                w.f64(x.w_tax);
                w.f64(x.w_ytd);
            }
            TpccRow::District(x) => {
                w.u8(TAG_DISTRICT);
                w.str(&x.d_name);
                w.str(&x.d_street_1);
                w.str(&x.d_street_2);
                w.str(&x.d_city);
                w.str(&x.d_state);
                w.str(&x.d_zip);
                w.f64(x.d_tax);
                w.f64(x.d_ytd);
                w.u32(x.d_next_o_id);
            }
            TpccRow::Customer(x) => {
                w.u8(TAG_CUSTOMER);
                w.str(&x.c_first);
                w.str(&x.c_middle);
                w.str(&x.c_last);
                w.str(&x.c_street_1);
                w.str(&x.c_street_2);
                w.str(&x.c_city);
                w.str(&x.c_state);
                w.str(&x.c_zip);
                w.str(&x.c_phone);
                w.i64(x.c_since);
                w.bool(x.c_credit_bad);
                w.f64(x.c_credit_lim);
                w.f64(x.c_discount);
                w.f64(x.c_balance);
                w.f64(x.c_ytd_payment);
                w.u32(x.c_payment_cnt);
                w.u32(x.c_delivery_cnt);
                w.str(&x.c_data);
            }
            TpccRow::CustomerNameIdx => w.u8(TAG_CUSTOMER_NAME_IDX),
            TpccRow::History(x) => {
                w.u8(TAG_HISTORY);
                w.u32(x.h_c_id);
                w.u8(x.h_c_d_id);
                w.u32(x.h_c_w_id);
                w.u8(x.h_d_id);
                w.u32(x.h_w_id);
                w.i64(x.h_date);
                w.f64(x.h_amount);
                w.str(&x.h_data);
            }
            TpccRow::NewOrder(m) => {
                w.u8(TAG_NEW_ORDER);
                w.u32(m.no_o_id);
            }
            TpccRow::Order(x) => {
                w.u8(TAG_ORDER);
                w.u32(x.o_c_id);
                w.i64(x.o_entry_d);
                w.opt_u32(x.o_carrier_id);
                w.u8(x.o_ol_cnt);
                w.bool(x.o_all_local);
            }
            TpccRow::OrderLine(x) => {
                w.u8(TAG_ORDER_LINE);
                w.u32(x.ol_i_id);
                w.u32(x.ol_supply_w_id);
                w.opt_i64(x.ol_delivery_d);
                w.u8(x.ol_quantity);
                w.f64(x.ol_amount);
                w.fixed(&x.ol_dist_info);
            }
            TpccRow::Item(x) => {
                w.u8(TAG_ITEM);
                w.u32(x.i_im_id);
                w.str(&x.i_name);
                w.f64(x.i_price);
                w.str(&x.i_data);
            }
            TpccRow::Stock(x) => {
                w.u8(TAG_STOCK);
                w.i32(x.s_quantity);
                for d in &x.s_dist {
                    w.fixed(d);
                }
                w.f64(x.s_ytd);
                w.u32(x.s_order_cnt);
                w.u32(x.s_remote_cnt);
                w.str(&x.s_data);
                w.u32(x.s_su_suppkey);
            }
            TpccRow::CustLastOrder(o_id) => {
                w.u8(TAG_CUST_LAST_ORDER);
                w.u32(*o_id);
            }
            TpccRow::Supplier(x) => {
                w.u8(TAG_SUPPLIER);
                w.str(&x.s_name);
                w.str(&x.s_address);
                w.u8(x.s_nationkey);
                w.str(&x.s_phone);
                w.f64(x.s_acctbal);
                w.str(&x.s_comment);
            }
            TpccRow::Nation(x) => {
                w.u8(TAG_NATION);
                w.str(&x.n_name);
                w.u8(x.n_regionkey);
                w.str(&x.n_comment);
            }
            TpccRow::Region(x) => {
                w.u8(TAG_REGION);
                w.str(&x.r_name);
                w.str(&x.r_comment);
            }
        }
    }

    fn wal_decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        Some(match r.u8()? {
            TAG_EMPTY => TpccRow::Empty,
            TAG_WAREHOUSE => TpccRow::Warehouse(Box::new(Warehouse {
                w_name: r.str()?,
                w_street_1: r.str()?,
                w_street_2: r.str()?,
                w_city: r.str()?,
                w_state: r.str()?,
                w_zip: r.str()?,
                w_tax: r.f64()?,
                w_ytd: r.f64()?,
            })),
            TAG_DISTRICT => TpccRow::District(Box::new(District {
                d_name: r.str()?,
                d_street_1: r.str()?,
                d_street_2: r.str()?,
                d_city: r.str()?,
                d_state: r.str()?,
                d_zip: r.str()?,
                d_tax: r.f64()?,
                d_ytd: r.f64()?,
                d_next_o_id: r.u32()?,
            })),
            TAG_CUSTOMER => TpccRow::Customer(Box::new(Customer {
                c_first: r.str()?,
                c_middle: r.str()?,
                c_last: r.str()?,
                c_street_1: r.str()?,
                c_street_2: r.str()?,
                c_city: r.str()?,
                c_state: r.str()?,
                c_zip: r.str()?,
                c_phone: r.str()?,
                c_since: r.i64()?,
                c_credit_bad: r.bool()?,
                c_credit_lim: r.f64()?,
                c_discount: r.f64()?,
                c_balance: r.f64()?,
                c_ytd_payment: r.f64()?,
                c_payment_cnt: r.u32()?,
                c_delivery_cnt: r.u32()?,
                c_data: r.str()?,
            })),
            TAG_CUSTOMER_NAME_IDX => TpccRow::CustomerNameIdx,
            TAG_HISTORY => TpccRow::History(Box::new(History {
                h_c_id: r.u32()?,
                h_c_d_id: r.u8()?,
                h_c_w_id: r.u32()?,
                h_d_id: r.u8()?,
                h_w_id: r.u32()?,
                h_date: r.i64()?,
                h_amount: r.f64()?,
                h_data: r.str()?,
            })),
            TAG_NEW_ORDER => TpccRow::NewOrder(NewOrderMarker { no_o_id: r.u32()? }),
            TAG_ORDER => TpccRow::Order(Box::new(Order {
                o_c_id: r.u32()?,
                o_entry_d: r.i64()?,
                o_carrier_id: r.opt_u32()?,
                o_ol_cnt: r.u8()?,
                o_all_local: r.bool()?,
            })),
            TAG_ORDER_LINE => TpccRow::OrderLine(Box::new(OrderLine {
                ol_i_id: r.u32()?,
                ol_supply_w_id: r.u32()?,
                ol_delivery_d: r.opt_i64()?,
                ol_quantity: r.u8()?,
                ol_amount: r.f64()?,
                ol_dist_info: r.fixed::<24>()?,
            })),
            TAG_ITEM => TpccRow::Item(Box::new(Item {
                i_im_id: r.u32()?,
                i_name: r.str()?,
                i_price: r.f64()?,
                i_data: r.str()?,
            })),
            TAG_STOCK => {
                let s_quantity = r.i32()?;
                let mut s_dist = [[0u8; 24]; 10];
                for slot in s_dist.iter_mut() {
                    *slot = r.fixed::<24>()?;
                }
                TpccRow::Stock(Box::new(Stock {
                    s_quantity,
                    s_dist,
                    s_ytd: r.f64()?,
                    s_order_cnt: r.u32()?,
                    s_remote_cnt: r.u32()?,
                    s_data: r.str()?,
                    s_su_suppkey: r.u32()?,
                }))
            }
            TAG_CUST_LAST_ORDER => TpccRow::CustLastOrder(r.u32()?),
            TAG_SUPPLIER => TpccRow::Supplier(Box::new(Supplier {
                s_name: r.str()?,
                s_address: r.str()?,
                s_nationkey: r.u8()?,
                s_phone: r.str()?,
                s_acctbal: r.f64()?,
                s_comment: r.str()?,
            })),
            TAG_NATION => TpccRow::Nation(Box::new(Nation {
                n_name: r.str()?,
                n_regionkey: r.u8()?,
                n_comment: r.str()?,
            })),
            TAG_REGION => TpccRow::Region(Box::new(Region {
                r_name: r.str()?,
                r_comment: r.str()?,
            })),
            _ => return None,
        })
    }

    fn wal_encode_size_hint(&self) -> usize {
        fn str_len(s: &str) -> usize {
            4 + s.len()
        }
        fn opt_len(present: bool, some_size: usize) -> usize {
            1 + if present { some_size } else { 0 }
        }

        const TAG: usize = 1;
        match self {
            TpccRow::Empty | TpccRow::CustomerNameIdx => TAG,
            TpccRow::Warehouse(x) => {
                TAG + str_len(&x.w_name)
                    + str_len(&x.w_street_1)
                    + str_len(&x.w_street_2)
                    + str_len(&x.w_city)
                    + str_len(&x.w_state)
                    + str_len(&x.w_zip)
                    + 8
                    + 8
            }
            TpccRow::District(x) => {
                TAG + str_len(&x.d_name)
                    + str_len(&x.d_street_1)
                    + str_len(&x.d_street_2)
                    + str_len(&x.d_city)
                    + str_len(&x.d_state)
                    + str_len(&x.d_zip)
                    + 8
                    + 8
                    + 4
            }
            TpccRow::Customer(x) => {
                TAG + str_len(&x.c_first)
                    + str_len(&x.c_middle)
                    + str_len(&x.c_last)
                    + str_len(&x.c_street_1)
                    + str_len(&x.c_street_2)
                    + str_len(&x.c_city)
                    + str_len(&x.c_state)
                    + str_len(&x.c_zip)
                    + str_len(&x.c_phone)
                    + 8
                    + 1
                    + 8
                    + 8
                    + 8
                    + 8
                    + 4
                    + 4
                    + str_len(&x.c_data)
            }
            TpccRow::History(x) => TAG + 4 + 1 + 4 + 1 + 4 + 8 + 8 + str_len(&x.h_data),
            TpccRow::NewOrder(_) => TAG + 4,
            TpccRow::Order(x) => TAG + 4 + 8 + opt_len(x.o_carrier_id.is_some(), 4) + 1 + 1,
            TpccRow::OrderLine(x) => {
                TAG + 4 + 4 + opt_len(x.ol_delivery_d.is_some(), 8) + 1 + 8 + 24
            }
            TpccRow::Item(x) => TAG + 4 + str_len(&x.i_name) + 8 + str_len(&x.i_data),
            TpccRow::Stock(x) => TAG + 4 + x.s_dist.len() * 24 + 8 + 4 + 4 + str_len(&x.s_data) + 4,
            TpccRow::CustLastOrder(_) => TAG + 4,
            TpccRow::Supplier(x) => {
                TAG + str_len(&x.s_name)
                    + str_len(&x.s_address)
                    + 1
                    + str_len(&x.s_phone)
                    + 8
                    + str_len(&x.s_comment)
            }
            TpccRow::Nation(x) => TAG + str_len(&x.n_name) + 1 + str_len(&x.n_comment),
            TpccRow::Region(x) => TAG + str_len(&x.r_name) + str_len(&x.r_comment),
        }
    }
}
