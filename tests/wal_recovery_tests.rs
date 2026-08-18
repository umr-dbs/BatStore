use std::fs;
use std::time::Duration;

use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_record_model::tx_stamp::TxStamp;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_sync::clock::GlobalClock;
use crate::bat_tree::mvbt::MVBTSt;
use crate::bat_wal::record;
use crate::bat_wal::recovery::replay_database;
use crate::bat_wal::writer::WalWriter;

type TestTree = MVBTSt<8, 8, u64, u64>;

/// A single shared log carrying writes for two different tables (one
/// transaction touching both, plus a second transaction touching only
/// one) must, after `replay_database`, leave each tree with exactly its
/// own writes — the concrete proof that table-tagged entries actually
/// demultiplex instead of all landing in whichever tree happens to be
/// passed first.
#[test]
fn replay_database_routes_writes_to_correct_table() {
    let path =
        std::env::temp_dir().join(format!("batstore_replay_db_test_{}.log", std::process::id()));
    let _ = fs::remove_file(&path);

    const TABLE_A: record::TableId = 0;
    const TABLE_B: record::TableId = 1;

    {
        let writer: WalWriter<u64, u64> = WalWriter::open(&path, Duration::from_millis(2)).unwrap();
        let clock = GlobalClock::new();

        // tx1 (worker 0): writes both tables under one shared stamp, one commit marker.
        let stamp1 = TxStamp::new(0, clock.next_timestamp());
        writer.log_with_stamp_for_table(TABLE_A, stamp1, |_| CRUDOperation::Insert(10u64, 100u64));
        writer.log_with_stamp_for_table(TABLE_B, stamp1, |_| CRUDOperation::Insert(20u64, 200u64));
        let ts_commit1 = clock.next_timestamp();
        let t1 = writer.log_commit_for_table_with_ticket(stamp1, ts_commit1);

        // tx2 (worker 0, later stamp): writes only TABLE_A.
        let stamp2 = TxStamp::new(0, clock.next_timestamp());
        writer.log_with_stamp_for_table(TABLE_A, stamp2, |_| CRUDOperation::Insert(11u64, 111u64));
        let ts_commit2 = clock.next_timestamp();
        let t2 = writer.log_commit_for_table_with_ticket(stamp2, ts_commit2);

        writer.wait_flushed(t1);
        writer.wait_flushed(t2);
    } // writer drops here, flushing/closing the file.

    let tree_a = TestTree::make_standard(RootIndexType::default());
    let tree_b = TestTree::make_standard(RootIndexType::default());

    // Indexed directly by TableId — table_a at index 0, table_b at index 1.
    let tables: [&TestTree; 2] = [&tree_a, &tree_b];

    replay_database(&tables, &path).unwrap();

    let va = tree_a.current_version();
    let vb = tree_b.current_version();

    match tree_a.dispatch_crud(CRUDOperation::Point(10, va)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 100 => {}
        other => panic!("table A should have key 10, got {other}"),
    }
    match tree_a.dispatch_crud(CRUDOperation::Point(11, va)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 111 => {}
        other => panic!("table A should have key 11, got {other}"),
    }
    match tree_a.dispatch_crud(CRUDOperation::Point(20, va)) {
        CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
        other => panic!("table A must NOT have table B's key 20, got {other}"),
    }

    match tree_b.dispatch_crud(CRUDOperation::Point(20, vb)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 200 => {}
        other => panic!("table B should have key 20, got {other}"),
    }
    match tree_b.dispatch_crud(CRUDOperation::Point(10, vb)) {
        CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
        other => panic!("table B must NOT have table A's key 10, got {other}"),
    }

    let _ = fs::remove_file(&path);
}
