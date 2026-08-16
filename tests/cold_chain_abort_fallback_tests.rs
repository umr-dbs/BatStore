//! Stage 2 of the cold-page-chain design, abort-reversal half (see
//! `cold_chain_read_fallback_tests.rs`/`cold_chain_range_fallback_tests.rs`
//! for the point-read/range-read halves and the shared rationale for
//! testing against hand-built chains). Covers the one part of the
//! cold-chain design those two files don't: `mv_sync::version_handle`'s
//! `abort_write_in_cold_chain`/`undelete_in_cold_chain`/
//! `clone_predecessor_from_cold_chain` - the machinery an aborted
//! transaction's writes fall back to once the record being reverted (or an
//! `Update`-abort's linked predecessor) isn't on the hot leaf itself
//! anymore, only somewhere in its cold chain.
//!
//! Those three functions are plain (module-private, not `pub(crate)`) `fn`s
//! on `MVBTSt`, so they can't be called directly from here - and reaching
//! them indirectly through `MVBTSt::abort_writes` would require a hand-built
//! leaf to be genuinely reachable via real root traversal, which none of
//! this crate's other hand-built-chain tests need either. Each of the three
//! is, however, a thin loop over one already-`pub(crate)` per-page
//! primitive (`LeafPage::abort_write`/`undelete_matching_deletion_stamp`/
//! `clone_undeleted_matching`) plus `ColdLink`'s own chain-walk shape - so
//! this file reimplements that exact walk (`abort_write_across_chain`/
//! `undelete_across_chain`/`clone_predecessor_across_chain` below) using
//! only those accessible primitives, and tests it at chain depths (up to 6
//! pages) deep enough to catch a walk that quietly stops early or
//! mis-locates a match once real multi-page chains are involved - the
//! specific property this codebase had zero prior coverage for.

use crate::mv_block::block::Block;
use crate::mv_page_model::leaf_page::{AbortOutcome, LeafPage};
use crate::mv_page_model::node::{ColdLink, Node, PageType};
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::tx_stamp::TxStamp;
use crate::mv_record_model::version_info::VersionInfo;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::{OptCell, SmartCell};

const FAN: usize = 8;
type TestLeaf = LeafPage<FAN, u64, u64>;
type TestBlock = Block<FAN, FAN, u64, u64>;
type TestCell = OptCell<TestBlock>;
type TestLink = ColdLink<FAN, FAN, u64, u64>;

fn insert(leaf: &mut TestLeaf, key: u64, stamp: TxStamp, payload: u64) {
    let len = leaf.len();
    leaf.push_uncommitted(RecordPoint::new(key, VersionInfo::new(stamp), payload), len);
    leaf.commit_delta(1, 0);
}

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
    leaf.commit_delta(0, 1);
}

fn build_cell(link: TestLink, build: impl FnOnce(&mut TestLeaf)) -> Box<TestCell> {
    let mut node = Node::<FAN, FAN, u64, u64>::new_leaf_with_cold_link(link);
    if let PageType::LeafMut(leaf) = node.as_page_mut() {
        build(leaf);
    }
    Box::new(OptCell::new(TestBlock {
        node_data: SafeCell::new(node),
    }))
}

fn cold_link_to(cell: &TestCell, count: u32, chain_len: u16, chain_total: u32) -> TestLink {
    ColdLink::new(SmartCell(cell as *const _), count, 500, chain_len, chain_total)
}

/// Mirrors `mv_sync::version_handle::MVBTSt::abort_write_in_cold_chain`
/// exactly (see this file's module doc): walk the chain newest-to-oldest,
/// upgrade each page's read guard to a write lock, and try
/// `LeafPage::abort_write` on it - stop at the first page that isn't a
/// clean miss. Also returns how many pages were actually visited, so tests
/// can tell "found on the first page" apart from "found only after walking
/// through several unrelated ones".
fn abort_write_across_chain(
    mut link: TestLink,
    key: u64,
    stamp: TxStamp,
) -> (AbortOutcome, Option<TxStamp>, TestLink, usize) {
    let mut visited = 0;
    while !link.is_none() {
        visited += 1;
        let mut guard = link.cold().borrow_read();
        assert!(
            guard.upgrade_write_lock(),
            "test-only chain, upgrade must never fail"
        );
        let cold = guard.deref_mut();
        let next_link = *cold.cold_link();
        let (outcome, pending) = cold.as_leaf_page().abort_write(key, stamp);
        if outcome != AbortOutcome::NotFound {
            return (outcome, pending, next_link, visited);
        }
        link = next_link;
    }
    (AbortOutcome::NotFound, None, TestLink::none(), visited)
}

/// Mirrors `undelete_in_cold_chain`: whole-page, unbounded search on each
/// page in turn for an entry whose own `deletion_stamp` matches `stamp`.
fn undelete_across_chain(mut link: TestLink, key: u64, stamp: TxStamp) -> (bool, usize) {
    let mut visited = 0;
    while !link.is_none() {
        visited += 1;
        let mut guard = link.cold().borrow_read();
        assert!(guard.upgrade_write_lock());
        let cold = guard.deref_mut();
        let next_link = *cold.cold_link();
        if cold
            .as_leaf_page()
            .undelete_matching_deletion_stamp(key, stamp, None)
        {
            return (true, visited);
        }
        link = next_link;
    }
    (false, visited)
}

/// Mirrors `clone_predecessor_from_cold_chain`: find (without mutating) the
/// entry whose `deletion_stamp` matches `stamp`, returning an already-
/// undeleted clone of it - the hot-leaf-side analogue of
/// `undelete_across_chain` above (see `LeafPage::clone_undeleted_matching`'s
/// doc for why this path leaves the historical page itself untouched).
fn clone_predecessor_across_chain(
    mut link: TestLink,
    key: u64,
    stamp: TxStamp,
) -> Option<RecordPoint<u64, u64>> {
    while !link.is_none() {
        let cold_cell = link.cold();
        let guard = cold_cell.borrow_read();
        let cold = &*guard;
        let next = *cold.cold_link();
        if let Some(record) = cold.as_leaf_page_ref().clone_undeleted_matching(key, stamp) {
            return Some(record);
        }
        link = next;
    }
    None
}

/// Builds a chain of `depth` pages, each holding one distinct filler key
/// (`2000 + level`) live and never deleted, except page 1 (the deepest, the
/// last one visited) which instead holds `special_key` however `install`
/// shapes it. Returns the link to the newest (first-visited) page.
fn build_chain_with_special_deepest_page(
    depth: u64,
    install: impl FnOnce(&mut TestLeaf),
) -> (TestLink, Vec<Box<TestCell>>) {
    let mut pages: Vec<Box<TestCell>> = Vec::new();
    let deepest = build_cell(ColdLink::none(), install);
    let mut link = cold_link_to(&deepest, 1, 1, 1);
    pages.push(deepest);

    for level in 2..=depth {
        let cell = build_cell(link, |leaf| {
            insert(leaf, 2000 + level, TxStamp::new(1, level), level * 100);
        });
        link = cold_link_to(&cell, 1, level as u16, level as u32);
        pages.push(cell);
    }
    (link, pages)
}

#[test]
fn abort_write_finds_a_bare_insert_on_the_first_of_many_cold_pages() {
    // Newest (first-visited) page directly holds the record to invalidate -
    // a bare Insert-abort, so the walk must stop after exactly one page
    // even though 5 more follow it in the chain. `pending` is still
    // `Some(stamp)` here, not `None`: `apply_invalidate`'s local
    // (same-page) predecessor search can't tell "no predecessor at all"
    // apart from "predecessor is somewhere else" (see `LeafPage::
    // apply_invalidate`'s own doc) - a genuinely bare insert just means the
    // *caller's* downstream search for that same stamp will come up empty,
    // which this test also checks below.
    const DEPTH: u64 = 6;
    let stamp = TxStamp::new(1, 99);
    let (link, _pages) = build_chain_with_special_deepest_page(DEPTH, |_| {});
    let newest = build_cell(link, |leaf| {
        insert(leaf, 42, stamp, 4242);
    });
    let link_to_newest = cold_link_to(&newest, 1, (DEPTH + 1) as u16, (DEPTH + 1) as u32);

    let (outcome, pending, next, visited) = abort_write_across_chain(link_to_newest, 42, stamp);
    assert_eq!(outcome, AbortOutcome::Invalidated);
    assert_eq!(pending, Some(stamp));
    assert_eq!(visited, 1, "must not walk past the very first (newest) page");

    let (found, _) = undelete_across_chain(next, 42, stamp);
    assert!(
        !found,
        "a bare insert's pending stamp must resolve to a harmless no-op downstream, not a false match"
    );
}

#[test]
fn abort_write_reaches_a_bare_insert_on_the_deepest_of_many_cold_pages() {
    // Opposite extreme: the target key only exists on the deepest (6th,
    // last-visited) page - every page before it holds an unrelated filler
    // key, so the walk can't succeed without genuinely reaching the bottom.
    const DEPTH: u64 = 6;
    let stamp = TxStamp::new(1, 99);
    let (link, _pages) = build_chain_with_special_deepest_page(DEPTH, |leaf| {
        insert(leaf, 42, stamp, 4242);
    });

    let (outcome, pending, next, visited) = abort_write_across_chain(link, 42, stamp);
    assert_eq!(outcome, AbortOutcome::Invalidated);
    assert_eq!(pending, Some(stamp));
    assert_eq!(
        visited, DEPTH as usize,
        "must walk through every one of the 5 unrelated pages to reach the 6th"
    );
    assert!(next.is_none(), "the deepest page is the end of the chain");
}

#[test]
fn abort_write_across_chain_reports_a_clean_miss_when_the_key_is_nowhere() {
    const DEPTH: u64 = 6;
    let (link, _pages) = build_chain_with_special_deepest_page(DEPTH, |_| {});
    let (outcome, pending, next, visited) =
        abort_write_across_chain(link, 999_999, TxStamp::new(1, 1));
    assert_eq!(outcome, AbortOutcome::NotFound);
    assert_eq!(pending, None);
    assert!(next.is_none());
    assert_eq!(
        visited, DEPTH as usize,
        "a genuine miss must still walk the entire chain, not give up early"
    );
}

/// The full `Update`-abort shape end to end: the invalidated record (page
/// 2 of 6) has no locally-matching predecessor, so the search must continue
/// from page 2's own successor onward (pages 3-6) to find and undelete the
/// exact entry whose `deletion_stamp` matches - several pages further than
/// where the invalidation itself happened, not just "the next one".
#[test]
fn undelete_across_chain_resolves_a_predecessor_several_pages_past_the_invalidated_one() {
    let stamp = TxStamp::new(1, 100);

    // Page 6 (deepest): holds the real predecessor, deleted by `stamp`.
    let deepest = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 7, TxStamp::new(1, 1), stamp, 700);
    });
    let mut link = cold_link_to(&deepest, 1, 1, 1);
    let mut pages: Vec<Box<TestCell>> = vec![deepest];

    // Pages 5, 4, 3 (three filler pages between page 2 and the deepest page
    // 6): built oldest-to-newest so `link` always points at the page just
    // built (one level shallower each time).
    for level in 2..=4u64 {
        let cell = build_cell(link, |leaf| {
            insert(leaf, 3000 + level, TxStamp::new(1, level), level * 100);
        });
        link = cold_link_to(&cell, 1, level as u16, level as u32);
        pages.push(cell);
    }
    let link_after_invalidated_page = link;

    // Page 2: holds key 7's own entry that will be invalidated by this
    // abort - inserted by `stamp`, still live (never itself deleted), and
    // the *only* entry for key 7 on this page, so `apply_invalidate`'s
    // local (same-page) predecessor search must fail and report the
    // pending stamp onward.
    let page2 = build_cell(link_after_invalidated_page, |leaf| {
        insert(leaf, 7, stamp, 777);
    });
    let link_to_page2 = cold_link_to(&page2, 1, 5, 5);
    pages.push(page2);

    // Page 1 (newest / first-visited): unrelated filler.
    let page1 = build_cell(link_to_page2, |leaf| {
        insert(leaf, 3999, TxStamp::new(1, 1), 399_900);
    });
    let link_to_page1 = cold_link_to(&page1, 1, 6, 6);
    pages.push(page1);

    let (outcome, pending, next_after_invalidate, visited) =
        abort_write_across_chain(link_to_page1, 7, stamp);
    assert_eq!(outcome, AbortOutcome::Invalidated);
    assert_eq!(
        pending,
        Some(stamp),
        "page 2 has no local predecessor match, so it must report the pending stamp"
    );
    assert_eq!(visited, 2, "page 1 (filler) then page 2 (the match)");

    let (found, undelete_visited) = undelete_across_chain(next_after_invalidate, 7, stamp);
    assert!(
        found,
        "the predecessor on page 6 must be found by continuing from page 2's successor"
    );
    assert_eq!(
        undelete_visited, 4,
        "pages 3, 4, 5, then 6 - all four pages past the invalidated page 2"
    );

    // Once undeleted, the exact same search must not find it again - proof
    // the mutation actually took effect, not just that the search reported
    // success without changing anything.
    let (found_again, _) = undelete_across_chain(next_after_invalidate, 7, stamp);
    assert!(
        !found_again,
        "the predecessor was already undeleted; searching for the same deletion_stamp again must miss"
    );
}

/// `clone_predecessor_from_cold_chain`'s own behavior: finds the right
/// entry several pages deep, returns an undeleted clone of it, and — unlike
/// `undelete_across_chain` — leaves the historical page itself unchanged,
/// so a second search for the same `deletion_stamp` still finds it.
#[test]
fn clone_predecessor_across_chain_finds_a_deep_match_without_mutating_the_source_page() {
    const DEPTH: u64 = 6;
    let stamp = TxStamp::new(1, 200);

    let deepest = build_cell(ColdLink::none(), |leaf| {
        insert_deleted(leaf, 9, TxStamp::new(1, 1), stamp, 900);
    });
    let mut link = cold_link_to(&deepest, 1, 1, 1);
    let mut pages: Vec<Box<TestCell>> = vec![deepest];

    for level in 2..=DEPTH {
        let cell = build_cell(link, |leaf| {
            insert(leaf, 4000 + level, TxStamp::new(1, level), level * 100);
        });
        link = cold_link_to(&cell, 1, level as u16, level as u32);
        pages.push(cell);
    }

    let cloned = clone_predecessor_across_chain(link, 9, stamp)
        .expect("must find the deepest page's matching predecessor");
    assert_eq!(*cloned.payload(), 900);
    assert!(
        cloned.version().is_live(),
        "the returned clone must already be undeleted, ready to reinstall on the hot leaf"
    );

    // Unchanged on the source page: searching again finds the identical
    // (still-deleted-on-that-page) record.
    let cloned_again = clone_predecessor_across_chain(link, 9, stamp)
        .expect("the historical page must be untouched by the clone above");
    assert_eq!(*cloned_again.payload(), 900);
}
