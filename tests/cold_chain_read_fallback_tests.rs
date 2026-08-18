//! Stage 2 of the cold-page-chain design (see `ColdLink`'s doc in
//! `bat_page_model::node`, and the SMO livelock investigation this follows
//! from): tests for the read-path fallback (`MVBTSt::scan_leaf_for_key`/
//! `scan_cold_chain_for_key` in `bat_query::query`) against hand-built cold
//! chains. Nothing in `split()`/`merge()` writes a real cold chain yet
//! (that's Stage 3) -- these tests build one directly the same low-level
//! way `leaf_page_abort_tests.rs` builds leaf content, bypassing the whole
//! `Database`/`DbTransaction`/`TxContext` machinery. That's deliberate: it
//! isolates "does the walk-the-chain-and-apply-the-visibility-predicate
//! mechanism work" from "does the real OSIC visibility system work" (the
//! latter is already covered extensively elsewhere) -- so `is_visible`
//! here is a small, fully-controlled test closure, not the real
//! `bat_sync::visibility::is_visible`.

use crate::bat_block::block::Block;
use crate::bat_page_model::leaf_page::LeafPage;
use crate::bat_page_model::node::{ColdLink, Node, PageType};
use crate::bat_record_model::record_point::RecordPoint;
use crate::bat_record_model::tx_stamp::TxStamp;
use crate::bat_record_model::version_info::VersionInfo;
use crate::bat_sync::safe_cell::SafeCell;
use crate::bat_sync::smart_cell::{OptCell, SmartCell};
use crate::bat_tree::mvbt::MVBTSt as Query;

const FAN: usize = 8;
type TestQuery = Query<FAN, FAN, u64, u64>;
type TestLeaf = LeafPage<FAN, u64, u64>;
type TestBlock = Block<FAN, FAN, u64, u64>;
type TestCell = OptCell<TestBlock>;

/// Mirrors `leaf_page_abort_tests.rs`'s own `insert` helper -- builds one
/// record directly via `push_uncommitted`/`commit_delta`, no transaction
/// machinery involved.
fn insert(leaf: &mut TestLeaf, key: u64, stamp: TxStamp, payload: u64) {
    let len = leaf.len();
    leaf.push_uncommitted(RecordPoint::new(key, VersionInfo::new(stamp), payload), len);
    leaf.commit_delta(1, 0);
}

/// Same, but the record is already deleted by `delete_stamp` at
/// construction (`VersionInfo::from`), standing in for what a real
/// cold-offload would migrate: an already-superseded version.
fn insert_deleted(
    leaf: &mut TestLeaf,
    key: u64,
    insert_stamp: TxStamp,
    delete_stamp: TxStamp,
    payload: u64,
) {
    let len = leaf.len();
    leaf.push_uncommitted(
        RecordPoint::new(key, VersionInfo::from(insert_stamp, delete_stamp), payload),
        len,
    );
    leaf.commit_delta(1, 0);
}

/// Builds a leaf-shaped `OptCell<Block<..>>` (real pointee type
/// `BlockRef`/`SmartCell` expects) with `cold_link` set and `build`
/// applied to its (initially empty) `LeafPage`. Used for both a
/// directly-linked cold page and, when `link` isn't `ColdLink::none()`,
/// for a cold page that itself chains further.
fn build_cell(
    link: ColdLink<FAN, FAN, u64, u64>,
    build: impl FnOnce(&mut TestLeaf),
) -> Box<TestCell> {
    let mut node = Node::<FAN, FAN, u64, u64>::new_leaf_with_cold_link(link);
    if let PageType::LeafMut(leaf) = node.as_page_mut() {
        build(leaf);
    }
    Box::new(OptCell::new(TestBlock {
        node_data: SafeCell::new(node),
    }))
}

fn cold_link_to(
    cell: &TestCell,
    count: u32,
    min_ts: u64,
    chain_len: u16,
    chain_total: u32,
) -> ColdLink<FAN, FAN, u64, u64> {
    ColdLink::new(
        SmartCell(cell as *const _),
        count,
        min_ts,
        chain_len,
        chain_total,
    )
}

/// A record inserted at `ts_start` t and (if any) deleted at `ts_start` d is
/// visible to this fixed, hand-controlled predicate iff `is_visible(t) ==
/// true`. Mirrors `VersionInfo::matches`'s real semantics (see that
/// method's doc) -- the tests below choose stamps so each scenario's
/// expected visibility is unambiguous under this rule.
fn visible_iff_ts_start_le(threshold: u64) -> impl FnMut(TxStamp) -> bool {
    move |stamp: TxStamp| stamp.ts_start() <= threshold
}

#[test]
fn scan_leaf_for_key_finds_a_visible_record() {
    let mut leaf = TestLeaf::new();
    insert(&mut leaf, 42, TxStamp::new(1, 10), 100);

    let mut is_visible = visible_iff_ts_start_le(20);
    let found = TestQuery::scan_leaf_for_key(&leaf, 42, &mut is_visible);
    assert_eq!(found.map(|r| *r.payload()), Some(100));
}

#[test]
fn scan_leaf_for_key_misses_a_not_yet_visible_record() {
    let mut leaf = TestLeaf::new();
    insert(&mut leaf, 42, TxStamp::new(1, 10), 100);

    // threshold(5) < insertion ts_start(10) -> not visible to this reader.
    let mut is_visible = visible_iff_ts_start_le(5);
    assert!(TestQuery::scan_leaf_for_key(&leaf, 42, &mut is_visible).is_none());
}

#[test]
fn scan_leaf_for_key_misses_a_deleted_and_visible_delete() {
    let mut leaf = TestLeaf::new();
    // Inserted at 10, deleted at 15 -- both visible to a reader at threshold
    // 20, so this reader should NOT see it (delete already visible).
    insert_deleted(&mut leaf, 42, TxStamp::new(1, 10), TxStamp::new(1, 15), 100);

    let mut is_visible = visible_iff_ts_start_le(20);
    assert!(TestQuery::scan_leaf_for_key(&leaf, 42, &mut is_visible).is_none());
}

#[test]
fn scan_leaf_for_key_finds_a_deleted_but_not_yet_visibly_deleted_record() {
    let mut leaf = TestLeaf::new();
    // Inserted at 10 (visible), deleted at 15 (NOT visible to a reader at
    // threshold 12) -- an older reader should still see the pre-delete
    // value. This is exactly the shape a cold-offloaded record has: dead
    // from a fresh reader's perspective, but still needed by an older one.
    insert_deleted(&mut leaf, 42, TxStamp::new(1, 10), TxStamp::new(1, 15), 100);

    let mut is_visible = visible_iff_ts_start_le(12);
    let found = TestQuery::scan_leaf_for_key(&leaf, 42, &mut is_visible);
    assert_eq!(found.map(|r| *r.payload()), Some(100));
}

#[test]
fn point_read_miss_on_hot_falls_through_to_a_directly_linked_cold_page() {
    // Cold page holds the only visible version of key 42.
    let cold = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 42, TxStamp::new(1, 10), TxStamp::new(1, 15), 100);
    });
    let link = cold_link_to(&cold, 1, 10, 1, 1);

    // The hot leaf itself has nothing for key 42 at all (simulates
    // cold-offload already having moved the only relevant version out).
    let mut is_visible = visible_iff_ts_start_le(12);
    assert!(TestQuery::scan_leaf_for_key(&TestLeaf::new(), 42, &mut is_visible).is_none());

    let found = TestQuery::scan_cold_chain_for_key(link, 42, &mut is_visible);
    assert_eq!(found.map(|r| *r.payload.get()), Some(100));
}

#[test]
fn point_read_miss_on_hot_and_first_cold_page_walks_the_chain() {
    // Oldest link: has the visible record.
    let cold2 = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 7, TxStamp::new(2, 1), TxStamp::new(2, 30), 999);
    });
    let link_to_cold2 = cold_link_to(&cold2, 1, 1, 1, 1);

    // Newest link (the one the hot leaf directly points to): does NOT have
    // key 7 at all, but chains to cold2 via its own cold_link.
    let cold1 = build_cell(link_to_cold2, |leaf| {
        insert(leaf, 99, TxStamp::new(3, 50), 1);
    });
    let link_to_cold1 = cold_link_to(&cold1, 1, 50, 2, 2);

    let mut is_visible = visible_iff_ts_start_le(10);
    let found = TestQuery::scan_cold_chain_for_key(link_to_cold1, 7, &mut is_visible);
    assert_eq!(found.map(|r| *r.payload.get()), Some(999));
}

#[test]
fn scan_cold_chain_for_key_returns_none_when_key_is_visible_nowhere() {
    let cold = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 42, TxStamp::new(1, 10), TxStamp::new(1, 15), 100);
    });
    let link = cold_link_to(&cold, 1, 10, 1, 1);

    // threshold(5) is before the insertion itself -- not visible anywhere
    // in the chain, same as a genuine miss.
    let mut is_visible = visible_iff_ts_start_le(5);
    assert!(TestQuery::scan_cold_chain_for_key(link, 42, &mut is_visible).is_none());
}

#[test]
fn point_read_walks_through_many_cold_pages_to_reach_the_oldest() {
    // A 6-page chain, each of the first 5 holding one unrelated filler key
    // so the walk can't accidentally succeed early - only the deepest
    // (6th, oldest) page actually has key 42. Regression guard for a
    // chain-walk that works for the 2-page case above but silently
    // truncates (an off-by-one loop bound, an accidental early return)
    // once a real multi-page chain (`ColdLink::chain_len` > 2) is involved.
    const DEPTH: u64 = 6;
    // Every page must outlive the walk below (see `build_cell`'s doc: the
    // `SmartCell` pointers `cold_link_to` hands out are raw and don't keep
    // their pointee alive) - kept in one `Vec` for the whole test instead
    // of one binding per page, since `DEPTH` is a loop bound here.
    let mut pages: Vec<Box<TestCell>> = Vec::new();
    let deepest = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 42, TxStamp::new(1, 1), TxStamp::new(1, 5), 4200);
    });
    let mut link = cold_link_to(&deepest, 1, 1, 1, 1);
    pages.push(deepest);

    // Chains newest-to-oldest, same direction `build_private_cold_chain`
    // links pages in: each loop iteration builds a page one level *newer*
    // than the last and points it at the previous (older) page.
    for level in 2..=DEPTH {
        let cell = build_cell(link, |leaf| {
            insert(leaf, 1000 + level, TxStamp::new(1, level), level * 100);
        });
        link = cold_link_to(&cell, 1, level, level as u16, level as u32);
        pages.push(cell);
    }

    let mut is_visible = visible_iff_ts_start_le(3);
    let found = TestQuery::scan_cold_chain_for_key(link, 42, &mut is_visible);
    assert_eq!(
        found.map(|r| *r.payload.get()),
        Some(4200),
        "must walk all the way through 5 unrelated cold pages to reach the 6th"
    );

    // A key that exists nowhere in the whole 6-page chain is still a clean
    // miss, not a false positive from matching the wrong page.
    let mut is_visible = visible_iff_ts_start_le(3);
    assert!(TestQuery::scan_cold_chain_for_key(link, 999_999, &mut is_visible).is_none());
}

#[test]
fn scan_cold_chain_for_key_is_none_for_an_empty_link() {
    // Regression-safety check: a leaf with no cold chain at all must not
    // pay for (or find anything via) a chain walk.
    let mut is_visible = visible_iff_ts_start_le(100);
    assert!(TestQuery::scan_cold_chain_for_key(ColdLink::none(), 42, &mut is_visible).is_none());
}
