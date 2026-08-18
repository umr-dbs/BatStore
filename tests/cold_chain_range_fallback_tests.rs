//! Stage 2 of the cold-page-chain design, range-scan half (see
//! `cold_chain_read_fallback_tests.rs` for the point-read half and the
//! shared rationale for testing against hand-built chains with a
//! fully-controlled `is_visible`, bypassing the real OSIC visibility
//! machinery). Exercises `RangeQueryIter::walk_cold_chain_for_range`
//! directly -- the shared, early-exit-capable walker both `refill` (the
//! buffered `Iterator` path) and `try_for_each_ref` (the zero-copy
//! streaming path) build on.

use crate::mv_block::block::Block;
use crate::mv_page_model::leaf_page::{LeafPage, LeafRecordRef};
use crate::mv_page_model::node::{ColdLink, Node, PageType};
use crate::mv_query::interval::Interval;
use crate::mv_query::iter_query::RangeQueryIter;
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::tx_stamp::TxStamp;
use crate::mv_record_model::version_info::VersionInfo;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::{OptCell, SmartCell};

const FAN: usize = 8;
type TestIter<'a> = RangeQueryIter<'a, FAN, FAN, u64, u64>;
type TestLeaf = LeafPage<FAN, u64, u64>;
type TestBlock = Block<FAN, FAN, u64, u64>;
type TestCell = OptCell<TestBlock>;

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

fn visible_iff_ts_start_le(threshold: u64) -> impl FnMut(TxStamp) -> bool {
    move |stamp: TxStamp| stamp.ts_start() <= threshold
}

#[test]
fn walks_a_single_cold_page_collecting_visible_in_range_matches() {
    let cold = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 5, TxStamp::new(1, 10), TxStamp::new(1, 20), 500);
        insert_deleted(leaf, 6, TxStamp::new(1, 10), TxStamp::new(1, 20), 600);
        // Out of range -- must not be collected even though visible.
        insert_deleted(leaf, 999, TxStamp::new(1, 10), TxStamp::new(1, 20), 1);
    });
    let link = cold_link_to(&cold, 3, 10, 1, 3);

    let mut is_visible = visible_iff_ts_start_le(12);
    let mut found: Vec<(u64, u64)> = Vec::new();
    TestIter::walk_cold_chain_for_range(
        link,
        Interval::new(0, 100),
        &mut is_visible,
        std::collections::HashSet::new(),
        |r: LeafRecordRef<'_, u64, u64>| {
            found.push((r.key(), *r.payload()));
            true
        },
    );
    found.sort();
    assert_eq!(found, vec![(5, 500), (6, 600)]);
}

#[test]
fn walks_a_chain_of_two_cold_pages_collecting_from_both() {
    let cold2 = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 1, TxStamp::new(1, 1), TxStamp::new(1, 30), 111);
    });
    let link_to_cold2 = cold_link_to(&cold2, 1, 1, 1, 1);

    let cold1 = build_cell(link_to_cold2, |leaf| {
        insert_deleted(leaf, 2, TxStamp::new(1, 1), TxStamp::new(1, 30), 222);
    });
    let link_to_cold1 = cold_link_to(&cold1, 1, 1, 2, 2);

    let mut is_visible = visible_iff_ts_start_le(10);
    let mut found: Vec<(u64, u64)> = Vec::new();
    TestIter::walk_cold_chain_for_range(
        link_to_cold1,
        Interval::new(0, 100),
        &mut is_visible,
        std::collections::HashSet::new(),
        |r: LeafRecordRef<'_, u64, u64>| {
            found.push((r.key(), *r.payload()));
            true
        },
    );
    found.sort();
    assert_eq!(found, vec![(1, 111), (2, 222)]);
}

#[test]
fn emits_a_visible_key_only_once_across_cold_pages() {
    let cold2 = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 42, TxStamp::new(1, 10), TxStamp::new(1, 30), 100);
    });
    let link_to_cold2 = cold_link_to(&cold2, 1, 10, 1, 1);
    let cold1 = build_cell(link_to_cold2, |leaf| {
        insert_deleted(leaf, 42, TxStamp::new(1, 10), TxStamp::new(1, 30), 100);
    });
    let link = cold_link_to(&cold1, 1, 10, 2, 2);

    let mut is_visible = visible_iff_ts_start_le(20);
    let mut found = Vec::new();
    TestIter::walk_cold_chain_for_range(
        link,
        Interval::new(0, 100),
        &mut is_visible,
        std::collections::HashSet::new(),
        |r| {
            found.push(r.key());
            true
        },
    );

    assert_eq!(found, vec![42]);
}

#[test]
fn skips_a_cold_key_already_emitted_from_hot() {
    let cold = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 42, TxStamp::new(1, 10), TxStamp::new(1, 30), 100);
    });
    let link = cold_link_to(&cold, 1, 10, 1, 1);
    let mut hot_keys = std::collections::HashSet::new();
    hot_keys.insert(42);

    let mut is_visible = visible_iff_ts_start_le(20);
    let mut visits = 0;
    TestIter::walk_cold_chain_for_range(
        link,
        Interval::new(0, 100),
        &mut is_visible,
        hot_keys,
        |_| {
            visits += 1;
            true
        },
    );

    assert_eq!(visits, 0);
}

#[test]
fn stops_early_when_the_callback_returns_false() {
    let cold2 = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 1, TxStamp::new(1, 1), TxStamp::new(1, 30), 111);
    });
    let link_to_cold2 = cold_link_to(&cold2, 1, 1, 1, 1);

    let cold1 = build_cell(link_to_cold2, |leaf| {
        insert_deleted(leaf, 2, TxStamp::new(1, 1), TxStamp::new(1, 30), 222);
    });
    let link_to_cold1 = cold_link_to(&cold1, 1, 1, 2, 2);

    let mut is_visible = visible_iff_ts_start_le(10);
    let mut visits = 0;
    TestIter::walk_cold_chain_for_range(
        link_to_cold1,
        Interval::new(0, 100),
        &mut is_visible,
        std::collections::HashSet::new(),
        |_r: LeafRecordRef<'_, u64, u64>| {
            visits += 1;
            false // stop after the very first match (mirrors an error in try_for_each_ref)
        },
    );
    assert_eq!(visits, 1, "must stop at cold1, never reach cold2");
}

#[test]
fn walks_a_long_chain_of_many_cold_pages_collecting_from_every_one() {
    // 6 pages, one visible in-range record each - a regression guard that
    // the chain walk doesn't stop early or skip a page once a real
    // multi-page chain (`ColdLink::chain_len` > 2) is involved, unlike the
    // 2-page test above.
    const DEPTH: u64 = 6;
    let mut pages: Vec<Box<TestCell>> = Vec::new();
    let deepest = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 1, TxStamp::new(1, 1), TxStamp::new(1, 30), 100);
    });
    let mut link = cold_link_to(&deepest, 1, 1, 1, 1);
    pages.push(deepest);

    for level in 2..=DEPTH {
        let cell = build_cell(link, |leaf| {
            insert_deleted(
                leaf,
                level,
                TxStamp::new(1, 1),
                TxStamp::new(1, 30),
                (level * 100) as u64,
            );
        });
        link = cold_link_to(&cell, 1, 1, level as u16, level as u32);
        pages.push(cell);
    }

    let mut is_visible = visible_iff_ts_start_le(10);
    let mut found: Vec<(u64, u64)> = Vec::new();
    TestIter::walk_cold_chain_for_range(
        link,
        Interval::new(0, 100),
        &mut is_visible,
        std::collections::HashSet::new(),
        |r: LeafRecordRef<'_, u64, u64>| {
            found.push((r.key(), *r.payload()));
            true
        },
    );
    found.sort();
    assert_eq!(
        found,
        (1..=DEPTH).map(|k| (k, k * 100)).collect::<Vec<_>>(),
        "every one of the 6 chained pages must contribute its record"
    );
}

#[test]
fn stops_partway_through_a_long_chain_when_the_callback_returns_false() {
    // Same 6-page chain shape as above, but the callback stops after the
    // 3rd match - must not visit pages 4-6 at all (mirrors
    // `stops_early_when_the_callback_returns_false`'s 2-page version, at a
    // depth long enough to actually distinguish "stops early" from
    // "happens to finish quickly because there's not much chain left").
    const DEPTH: u64 = 6;
    let mut pages: Vec<Box<TestCell>> = Vec::new();
    let deepest = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 1, TxStamp::new(1, 1), TxStamp::new(1, 30), 100);
    });
    let mut link = cold_link_to(&deepest, 1, 1, 1, 1);
    pages.push(deepest);

    for level in 2..=DEPTH {
        let cell = build_cell(link, |leaf| {
            insert_deleted(
                leaf,
                level,
                TxStamp::new(1, 1),
                TxStamp::new(1, 30),
                (level * 100) as u64,
            );
        });
        link = cold_link_to(&cell, 1, 1, level as u16, level as u32);
        pages.push(cell);
    }

    let mut is_visible = visible_iff_ts_start_le(10);
    let mut visits = 0;
    TestIter::walk_cold_chain_for_range(
        link,
        Interval::new(0, 100),
        &mut is_visible,
        std::collections::HashSet::new(),
        |_r: LeafRecordRef<'_, u64, u64>| {
            visits += 1;
            visits < 3
        },
    );
    assert_eq!(visits, 3, "must stop after the 3rd page, never reach pages 4-6");
}

#[test]
fn nothing_visible_in_range_yields_no_calls() {
    let cold = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 5, TxStamp::new(1, 10), TxStamp::new(1, 20), 500);
    });
    let link = cold_link_to(&cold, 1, 10, 1, 1);

    // threshold(5) predates the insertion -- nothing visible anywhere.
    let mut is_visible = visible_iff_ts_start_le(5);
    let mut visits = 0;
    TestIter::walk_cold_chain_for_range(
        link,
        Interval::new(0, 100),
        &mut is_visible,
        std::collections::HashSet::new(),
        |_| {
            visits += 1;
            true
        },
    );
    assert_eq!(visits, 0);
}

#[test]
fn empty_link_yields_no_calls() {
    let mut is_visible = visible_iff_ts_start_le(100);
    let mut visits = 0;
    TestIter::walk_cold_chain_for_range(
        ColdLink::none(),
        Interval::new(0, 100),
        &mut is_visible,
        std::collections::HashSet::new(),
        |_| {
            visits += 1;
            true
        },
    );
    assert_eq!(visits, 0);
}
