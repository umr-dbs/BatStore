//! Correctness checks for the YCSB benchmark harness (`mv_bench::ycsb_load`/
//! `ycsb_txn`): a tiny table, exercising each of the five Core Workload ops
//! (Read/Update/Insert/Scan/Read-Modify-Write) and checking the actual row
//! content/counts they produce, not just that the call returned `Ok`.

use crate::mv_bench::ycsb_load::populate;
use crate::mv_bench::ycsb_schema::{YcsbConfig, YcsbTree};
use crate::mv_bench::ycsb_txn;
use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_root::index_root::RootIndexType;

/// Small enough that loading + a handful of ops finishes in well under a
/// second.
fn tiny_cfg() -> YcsbConfig {
    YcsbConfig {
        record_count: 50,
        field_count: 3,
        field_length: 8,
    }
}

fn read_bytes(tree: &YcsbTree, key: u64) -> Option<Vec<u8>> {
    match tree.dispatch_crud(CRUDOperation::PointSi(key)) {
        CRUDOperationResult::MatchedRecords(v) => v.first().map(|r| r.payload.as_bytes().to_vec()),
        other => panic!("ycsb test: unexpected point result: {other}"),
    }
}

/// `populate` must load exactly `1..=record_count`, each row shaped exactly
/// `field_count * field_length` bytes (YCSB's fixed-width row), and nothing
/// beyond that range.
#[test]
fn populate_loads_exactly_the_configured_keys_with_correctly_shaped_rows() {
    let cfg = tiny_cfg();
    let tree = YcsbTree::make_standard(RootIndexType::default());
    populate(&tree, &cfg);

    for key in 1..=cfg.record_count {
        let bytes =
            read_bytes(&tree, key).unwrap_or_else(|| panic!("key {key} should have been loaded"));
        assert_eq!(
            bytes.len(),
            cfg.field_count * cfg.field_length,
            "row at key {key} has the wrong byte width"
        );
    }
    assert!(
        read_bytes(&tree, cfg.record_count + 1).is_none(),
        "must not load a row beyond record_count"
    );
}

/// Insert mints a brand-new row; Read must then find it with the right
/// shape; Update must change exactly one field by default while
/// leaving the key itself in place; Update on a never-inserted key must
/// report a miss instead of silently creating one.
#[test]
fn insert_read_and_update_round_trip_correctly() {
    let cfg = tiny_cfg();
    let tree = YcsbTree::make_standard(RootIndexType::default());
    populate(&tree, &cfg);

    let new_key = cfg.record_count + 1;
    assert!(
        !ycsb_txn::read(&tree, new_key),
        "key must not exist before Insert"
    );

    ycsb_txn::insert(&tree, &cfg, new_key);
    assert!(
        ycsb_txn::read(&tree, new_key),
        "key must exist right after Insert"
    );
    let inserted = read_bytes(&tree, new_key).unwrap();
    assert_eq!(inserted.len(), cfg.field_count * cfg.field_length);

    assert!(
        ycsb_txn::update(&tree, &cfg, new_key, false),
        "Update on an existing key must report a hit"
    );
    let updated = read_bytes(&tree, new_key).unwrap();
    assert_eq!(updated.len(), cfg.field_count * cfg.field_length);
    let changed_fields = inserted
        .chunks_exact(cfg.field_length)
        .zip(updated.chunks_exact(cfg.field_length))
        .filter(|(before, after)| before != after)
        .count();
    assert_eq!(
        changed_fields, 1,
        "writeallfields=false must preserve every unselected field"
    );

    assert!(ycsb_txn::update(&tree, &cfg, new_key, true));
    let all_fields = read_bytes(&tree, new_key).unwrap();
    assert_eq!(all_fields.len(), cfg.field_count * cfg.field_length);
    assert_ne!(
        updated, all_fields,
        "writeallfields=true must generate a fresh complete row"
    );

    assert!(
        !ycsb_txn::update(&tree, &cfg, new_key + 1, false),
        "Update on a never-inserted key must report a miss, not create it"
    );
    assert!(!ycsb_txn::read(&tree, new_key + 1));
}

/// Scan must return exactly the count of loaded keys within `[start_key,
/// start_key + len)`, including the truncated count when the requested
/// range runs past the last loaded key.
#[test]
fn scan_returns_the_exact_count_of_keys_in_range() {
    let cfg = tiny_cfg();
    let tree = YcsbTree::make_standard(RootIndexType::default());
    populate(&tree, &cfg);

    assert_eq!(
        ycsb_txn::scan(&tree, 10, 5),
        5,
        "keys 10..=14 are all loaded"
    );
    assert_eq!(
        ycsb_txn::scan(&tree, 1, cfg.record_count),
        cfg.record_count as usize,
        "the entire loaded range"
    );
    assert_eq!(
        ycsb_txn::scan(&tree, cfg.record_count - 2, 10),
        3,
        "only 3 of the requested 10 keys exist before running off the loaded range"
    );
}

/// Read-Modify-Write must replace the row's content (like Update) while
/// reporting whether the target key existed.
#[test]
fn read_modify_write_replaces_content_and_reports_existence() {
    let cfg = tiny_cfg();
    let tree = YcsbTree::make_standard(RootIndexType::default());
    populate(&tree, &cfg);

    let before = read_bytes(&tree, 5).unwrap();
    assert!(ycsb_txn::read_modify_write(&tree, &cfg, 5, false));
    let after = read_bytes(&tree, 5).unwrap();
    assert_eq!(after.len(), cfg.field_count * cfg.field_length);
    assert_ne!(
        before, after,
        "read_modify_write must actually rewrite the row"
    );

    assert!(
        !ycsb_txn::read_modify_write(&tree, &cfg, cfg.record_count + 100, false),
        "must report a miss for a never-inserted key"
    );
}
