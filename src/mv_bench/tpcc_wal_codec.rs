//! Real (variable-length) WAL encoding for [`TpccRow`], implementing
//! `mv_wal::record::WalPayload`. `TpccRow`'s large variants box `String`-
//! bearing structs (`Customer`, `Stock`, ...), so the WAL's original
//! fixed-size raw-memcpy encoding (`write_raw`/`read_raw`, still used for
//! `Key`/the base `Payload = u64` case) would write out heap pointers
//! instead of the data they point to — unsound the instant the recovered
//! value is read, cloned, or dropped. Every variant gets a tag byte plus its
//! own explicit field encoding instead.

use crate::mv_bench::tpcc_schema::*;
use crate::mv_wal::record::WalPayload;

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
    fn u8(&mut self, v: u8) { self.0.push(v); }
    fn bool(&mut self, v: bool) { self.0.push(v as u8); }
    fn u32(&mut self, v: u32) { self.0.extend_from_slice(&v.to_le_bytes()); }
    fn i32(&mut self, v: i32) { self.0.extend_from_slice(&v.to_le_bytes()); }
    fn i64(&mut self, v: i64) { self.0.extend_from_slice(&v.to_le_bytes()); }
    fn f64(&mut self, v: f64) { self.0.extend_from_slice(&v.to_le_bytes()); }

    fn str(&mut self, s: &str) {
        self.u32(s.len() as u32);
        self.0.extend_from_slice(s.as_bytes());
    }

    fn opt_u32(&mut self, v: Option<u32>) {
        match v {
            Some(x) => { self.bool(true); self.u32(x); }
            None => self.bool(false),
        }
    }

    fn opt_i64(&mut self, v: Option<i64>) {
        match v {
            Some(x) => { self.bool(true); self.i64(x); }
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
        std::str::from_utf8(self.take(len)?).ok().map(str::to_string)
    }

    fn opt_u32(&mut self) -> Option<Option<u32>> {
        if self.bool()? { Some(Some(self.u32()?)) } else { Some(None) }
    }

    fn opt_i64(&mut self) -> Option<Option<i64>> {
        if self.bool()? { Some(Some(self.i64()?)) } else { Some(None) }
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
                w.str(&x.ol_dist_info);
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
                    w.str(d);
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
                ol_dist_info: r.str()?,
            })),
            TAG_ITEM => TpccRow::Item(Box::new(Item {
                i_im_id: r.u32()?,
                i_name: r.str()?,
                i_price: r.f64()?,
                i_data: r.str()?,
            })),
            TAG_STOCK => {
                let s_quantity = r.i32()?;
                let mut s_dist: [String; 10] = Default::default();
                for slot in s_dist.iter_mut() {
                    *slot = r.str()?;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(row: TpccRow) {
        let mut bytes = Vec::new();
        row.wal_encode(&mut bytes);
        let decoded = TpccRow::wal_decode(&bytes).expect("decode should succeed");
        assert_eq!(format!("{row}"), format!("{decoded}"), "Display mismatch after round-trip");
        // Re-encoding the decoded value must reproduce the exact same bytes
        // — the strongest available equality check given TpccRow has no
        // PartialEq (its rows carry no natural key to compare on) and
        // catches any field silently dropped/misordered by wal_decode.
        let mut re_encoded = Vec::new();
        decoded.wal_encode(&mut re_encoded);
        assert_eq!(bytes, re_encoded, "byte mismatch after round-trip");
    }

    #[test]
    fn every_variant_round_trips() {
        round_trip(TpccRow::Empty);
        round_trip(TpccRow::Warehouse(Box::new(Warehouse {
            w_name: "W1".into(), w_street_1: "s1".into(), w_street_2: "s2".into(),
            w_city: "city".into(), w_state: "CA".into(), w_zip: "123451111".into(),
            w_tax: 0.05, w_ytd: 300_000.0,
        })));
        round_trip(TpccRow::District(Box::new(District {
            d_name: "D1".into(), d_street_1: "s1".into(), d_street_2: "s2".into(),
            d_city: "city".into(), d_state: "CA".into(), d_zip: "123451111".into(),
            d_tax: 0.05, d_ytd: 30_000.0, d_next_o_id: 3001,
        })));
        round_trip(TpccRow::Customer(Box::new(Customer {
            c_first: "Amir".into(), c_middle: "OE".into(), c_last: "BARBAR".into(),
            c_street_1: "s1".into(), c_street_2: "s2".into(), c_city: "city".into(),
            c_state: "CA".into(), c_zip: "123451111".into(), c_phone: "1234567890123456".into(),
            c_since: 1234567890, c_credit_bad: true, c_credit_lim: 50_000.0,
            c_discount: 0.15, c_balance: -10.0, c_ytd_payment: 10.0,
            c_payment_cnt: 1, c_delivery_cnt: 0, c_data: "x".repeat(400),
        })));
        round_trip(TpccRow::CustomerNameIdx);
        round_trip(TpccRow::History(Box::new(History {
            h_c_id: 1, h_c_d_id: 2, h_c_w_id: 3, h_d_id: 2, h_w_id: 3,
            h_date: 42, h_amount: 10.0, h_data: "note".into(),
        })));
        round_trip(TpccRow::NewOrder(NewOrderMarker { no_o_id: 3001 }));
        round_trip(TpccRow::Order(Box::new(Order {
            o_c_id: 7, o_entry_d: 42, o_carrier_id: None, o_ol_cnt: 10, o_all_local: true,
        })));
        round_trip(TpccRow::Order(Box::new(Order {
            o_c_id: 7, o_entry_d: 42, o_carrier_id: Some(3), o_ol_cnt: 10, o_all_local: false,
        })));
        round_trip(TpccRow::OrderLine(Box::new(OrderLine {
            ol_i_id: 99, ol_supply_w_id: 1, ol_delivery_d: None,
            ol_quantity: 5, ol_amount: 12.34, ol_dist_info: "d".repeat(24),
        })));
        round_trip(TpccRow::OrderLine(Box::new(OrderLine {
            ol_i_id: 99, ol_supply_w_id: 1, ol_delivery_d: Some(99),
            ol_quantity: 5, ol_amount: 12.34, ol_dist_info: "d".repeat(24),
        })));
        round_trip(TpccRow::Item(Box::new(Item {
            i_im_id: 5, i_name: "widget".into(), i_price: 9.99, i_data: "ORIGINALxyz".into(),
        })));
        round_trip(TpccRow::Stock(Box::new(Stock {
            s_quantity: -5, s_dist: std::array::from_fn(|i| format!("dist{i}")),
            s_ytd: 1.0, s_order_cnt: 2, s_remote_cnt: 3, s_data: "data".into(),
            s_su_suppkey: 4321,
        })));
        round_trip(TpccRow::CustLastOrder(42));
        round_trip(TpccRow::Supplier(Box::new(Supplier {
            s_name: "Supplier#1".into(), s_address: "addr".into(), s_nationkey: 7,
            s_phone: "1234567890123456".into(), s_acctbal: 1234.56, s_comment: "comment".into(),
        })));
        round_trip(TpccRow::Nation(Box::new(Nation {
            n_name: "GERMANY".into(), n_regionkey: 3, n_comment: "comment".into(),
        })));
        round_trip(TpccRow::Region(Box::new(Region {
            r_name: "EUROPE".into(), r_comment: "comment".into(),
        })));
    }

    #[test]
    fn decode_rejects_truncated_bytes() {
        let row = TpccRow::Customer(Box::new(Customer {
            c_first: "Amir".into(), c_middle: "OE".into(), c_last: "BARBAR".into(),
            c_street_1: "s1".into(), c_street_2: "s2".into(), c_city: "city".into(),
            c_state: "CA".into(), c_zip: "123451111".into(), c_phone: "1234567890123456".into(),
            c_since: 1234567890, c_credit_bad: true, c_credit_lim: 50_000.0,
            c_discount: 0.15, c_balance: -10.0, c_ytd_payment: 10.0,
            c_payment_cnt: 1, c_delivery_cnt: 0, c_data: "x".repeat(400),
        }));
        let mut bytes = Vec::new();
        row.wal_encode(&mut bytes);

        for cut in 0..bytes.len() {
            assert!(TpccRow::wal_decode(&bytes[..cut]).is_none(), "truncation at {cut} should fail, not misparse");
        }
    }

    /// The real end-to-end check: before `WalPayload` existed, a `TpccRow`
    /// carrying a `Box`'d struct (e.g. `Customer`) would log its *pointer*
    /// bytes, not its data — reading it back (here, via a genuine crash +
    /// `open_recovered`, not just `wal_decode` in isolation) would reconstruct
    /// a `Box` wrapping a dangling/foreign-process pointer, which is
    /// undefined behavior the instant it's read, cloned, or dropped. This
    /// drops the tree (releasing every `Box` normally) and recovers into a
    /// *new* tree from *only* the WAL bytes, so any such corruption would
    /// manifest as wrong data, a panic, or a crash here.
    #[test]
    fn crash_recovery_round_trip_for_boxed_rows() {
        use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
        use crate::mv_crud_model::crud_operation::CRUDOperation;
        use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
        use crate::mv_root::index_root::RootIndexType;
        use crate::mv_bench::tpcc_schema::TpccTree;

        let path = std::env::temp_dir().join(format!("cmvbt_tpcc_wal_test_{}.log", std::process::id()));
        // Per-worker WAL sharding (see mv_tree::mvbt::wal_shard_path): a
        // single test thread means everything lands in worker 0's shard,
        // the only one that's actually a real file on disk.
        let shard_path = crate::mv_tree::mvbt::wal_shard_path(&path, 0);
        let _ = std::fs::remove_file(&shard_path);

        let warehouse_key = k_warehouse(1);
        let customer_key = k_customer(1, 1, 42);
        let stock_key = k_stock(1, 7);

        {
            let tree = TpccTree::make_standard(RootIndexType::default());
            tree.enable_wal(&path, std::time::Duration::from_millis(2)).unwrap();

            assert!(matches!(
                tree.dispatch_crud(CRUDOperation::Insert(warehouse_key, TpccRow::Warehouse(Box::new(Warehouse {
                    w_name: "Marburg".into(), w_street_1: "Uniplatz".into(), w_street_2: "".into(),
                    w_city: "Marburg".into(), w_state: "HE".into(), w_zip: "350321111".into(),
                    w_tax: 0.07, w_ytd: 300_000.0,
                })))),
                CRUDOperationResult::Inserted(_)
            ));

            assert!(matches!(
                tree.dispatch_crud(CRUDOperation::Insert(customer_key, TpccRow::Customer(Box::new(Customer {
                    c_first: "Amir".into(), c_middle: "OE".into(), c_last: "BARBAR".into(),
                    c_street_1: "s1".into(), c_street_2: "s2".into(), c_city: "city".into(),
                    c_state: "HE".into(), c_zip: "350321111".into(), c_phone: "1234567890123456".into(),
                    c_since: 1234567890, c_credit_bad: true, c_credit_lim: 50_000.0,
                    c_discount: 0.15, c_balance: -10.0, c_ytd_payment: 10.0,
                    c_payment_cnt: 1, c_delivery_cnt: 0, c_data: "x".repeat(450),
                })))),
                CRUDOperationResult::Inserted(_)
            ));

            assert!(matches!(
                tree.dispatch_crud(CRUDOperation::Insert(stock_key, TpccRow::Stock(Box::new(Stock {
                    s_quantity: 42, s_dist: std::array::from_fn(|i| format!("dist{i}")),
                    s_ytd: 1.0, s_order_cnt: 2, s_remote_cnt: 3, s_data: "ORIGINALxyz".into(),
                    s_su_suppkey: 999,
                })))),
                CRUDOperationResult::Inserted(_)
            ));
        } // tree drops here: every Box is deallocated normally, exactly like a real crash would leave nothing behind but the WAL file.

        let recovered = TpccTree::open_recovered(RootIndexType::default(), &path, std::time::Duration::from_millis(2)).unwrap();
        let version = recovered.current_version();

        match recovered.dispatch_crud(CRUDOperation::Point(warehouse_key, version)) {
            CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => {
                let w = r[0].payload.as_warehouse();
                assert_eq!(w.w_name, "Marburg");
                assert_eq!(w.w_tax, 0.07);
            }
            other => panic!("warehouse missing or wrong after recovery: {other}"),
        }

        match recovered.dispatch_crud(CRUDOperation::Point(customer_key, version)) {
            CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => {
                let c = r[0].payload.as_customer();
                assert_eq!(c.c_last, "BARBAR");
                assert_eq!(c.c_data.len(), 450);
                assert!(c.c_credit_bad);
            }
            other => panic!("customer missing or wrong after recovery: {other}"),
        }

        match recovered.dispatch_crud(CRUDOperation::Point(stock_key, version)) {
            CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => {
                let s = r[0].payload.as_stock();
                assert_eq!(s.s_quantity, 42);
                assert_eq!(s.s_dist[3], "dist3");
                assert_eq!(s.s_data, "ORIGINALxyz");
            }
            other => panic!("stock missing or wrong after recovery: {other}"),
        }

        let _ = std::fs::remove_file(&shard_path);
    }

    /// `TpccDatabase` counterpart to `crash_recovery_round_trip_for_boxed_rows`:
    /// each table is now its own tree with its own WAL shard files (see
    /// `tpcc_schema::table_wal_path`), sharing one `TxContext` — this
    /// confirms `TpccDatabase::open_recovered` correctly replays every
    /// table's own shard independently and that a write to one table
    /// (Warehouse) survives recovery alongside a write to a different table
    /// (Customer), even though they're now physically separate trees.
    #[test]
    fn tpcc_database_crash_recovery_round_trip_across_tables() {
        use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
        use crate::mv_crud_model::crud_operation::CRUDOperation;
        use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
        use crate::mv_root::index_root::RootIndexType;
        use crate::mv_bench::tpcc_schema::{table_wal_path, Table, TpccDatabase};

        let base_path = std::env::temp_dir().join(format!("cmvbt_tpcc_db_wal_test_{}.log", std::process::id()));
        for t in Table::ALL {
            let _ = std::fs::remove_file(crate::mv_tree::mvbt::wal_shard_path(&table_wal_path(&base_path, t), 0));
        }

        let warehouse_key = k_warehouse(1);
        let customer_key = k_customer(1, 1, 42);

        {
            let db = TpccDatabase::new(RootIndexType::default());
            db.enable_wal(&base_path, std::time::Duration::from_millis(2)).unwrap();

            assert!(matches!(
                db.warehouse.dispatch_crud(CRUDOperation::Insert(warehouse_key, TpccRow::Warehouse(Box::new(Warehouse {
                    w_name: "Marburg".into(), w_street_1: "Uniplatz".into(), w_street_2: "".into(),
                    w_city: "Marburg".into(), w_state: "HE".into(), w_zip: "350321111".into(),
                    w_tax: 0.07, w_ytd: 300_000.0,
                })))),
                CRUDOperationResult::Inserted(_)
            ));

            assert!(matches!(
                db.customer.dispatch_crud(CRUDOperation::Insert(customer_key, TpccRow::Customer(Box::new(Customer {
                    c_first: "Amir".into(), c_middle: "OE".into(), c_last: "BARBAR".into(),
                    c_street_1: "s1".into(), c_street_2: "s2".into(), c_city: "city".into(),
                    c_state: "HE".into(), c_zip: "350321111".into(), c_phone: "1234567890123456".into(),
                    c_since: 1234567890, c_credit_bad: true, c_credit_lim: 50_000.0,
                    c_discount: 0.15, c_balance: -10.0, c_ytd_payment: 10.0,
                    c_payment_cnt: 1, c_delivery_cnt: 0, c_data: "x".repeat(450),
                })))),
                CRUDOperationResult::Inserted(_)
            ));
        } // db drops here: every table's Box'd rows are deallocated normally.

        let recovered = TpccDatabase::open_recovered(RootIndexType::default(), &base_path, std::time::Duration::from_millis(2)).unwrap();
        let version = recovered.current_version();

        match recovered.warehouse.dispatch_crud(CRUDOperation::Point(warehouse_key, version)) {
            CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => {
                let w = r[0].payload.as_warehouse();
                assert_eq!(w.w_name, "Marburg");
                assert_eq!(w.w_tax, 0.07);
            }
            other => panic!("warehouse missing or wrong after recovery: {other}"),
        }

        match recovered.customer.dispatch_crud(CRUDOperation::Point(customer_key, version)) {
            CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => {
                let c = r[0].payload.as_customer();
                assert_eq!(c.c_last, "BARBAR");
                assert_eq!(c.c_data.len(), 450);
                assert!(c.c_credit_bad);
            }
            other => panic!("customer missing or wrong after recovery: {other}"),
        }

        for t in Table::ALL {
            let _ = std::fs::remove_file(crate::mv_tree::mvbt::wal_shard_path(&table_wal_path(&base_path, t), 0));
        }
    }
}
