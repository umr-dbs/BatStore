//! Stage 1 of the cold-page-chain design (see project memory / the SMO
//! livelock investigation this follows from): pure layout and accessor
//! tests for `ColdLink`, added before anything in `split()`/`merge()`/the
//! read path consumes it. Two things must hold before any later stage
//! builds on this: (1) `Node`'s allocated footprint is byte-identical to
//! before this field existed, at both the production and a small (test)
//! config — the whole point was fitting inside already-reserved padding,
//! and this is the regression check that it actually did; (2) the
//! accessors round-trip correctly in isolation, since nothing else
//! exercises them yet.

use crate::mv_page_model::internal_page::InternalPage;
use crate::mv_page_model::leaf_page::LeafPage;
use crate::mv_page_model::node::{COLD_LINK_SIZE, ColdLink, Node, PADDING};
use crate::mv_sync::smart_cell::OptCell;

type ProdKey = u64;
type ProdPayload = u64;
const PROD_FAN: usize = 123;
const PROD_RECORDS: usize = 123;
const TEST_FAN: usize = 16;
const TEST_RECORDS: usize = 16;

#[test]
fn cold_link_size_fits_inside_reserved_padding() {
    // If this ever fails, `PADDING` needs raising before adding more
    // fields to `ColdLink` -- it means the struct has grown past what's
    // currently reserved.
    assert!(
        COLD_LINK_SIZE <= PADDING,
        "ColdLink ({COLD_LINK_SIZE}B) no longer fits in Node's {PADDING}B reserved padding"
    );
}

#[test]
fn node_size_unchanged_at_production_config() {
    // Baseline captured directly via size_of on the pre-ColdLink code
    // during the investigation this stage follows from: LeafPage=3960,
    // InternalPage=3960, Node=4032, OptCell<Node>=4096. This test exists
    // so a future change to ColdLink's fields (or PADDING's split) can't
    // silently grow the block's allocation past its current jemalloc size
    // class without a test failing loudly first.
    assert_eq!(
        size_of::<LeafPage<PROD_RECORDS, ProdKey, ProdPayload>>(),
        3960
    );
    assert_eq!(
        size_of::<InternalPage<PROD_FAN, PROD_RECORDS, ProdKey, ProdPayload>>(),
        3960
    );
    assert_eq!(
        size_of::<Node<PROD_FAN, PROD_RECORDS, ProdKey, ProdPayload>>(),
        4032
    );
    assert_eq!(
        size_of::<OptCell<Node<PROD_FAN, PROD_RECORDS, ProdKey, ProdPayload>>>(),
        4096
    );
}

#[test]
fn node_size_unchanged_at_small_test_config() {
    // Same regression check at the FAN=16 config used by
    // `verify_concurrent_shared_keys.rs` and friends, confirming the
    // "fits in the existing slack regardless of NUM_RECORDS" claim (the
    // padding source is PADDING/alignment, not proportional to record
    // count).
    assert_eq!(
        size_of::<LeafPage<TEST_RECORDS, ProdKey, ProdPayload>>(),
        536
    );
    assert_eq!(
        size_of::<InternalPage<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>>(),
        536
    );
    assert_eq!(
        size_of::<Node<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>>(),
        640
    );
    assert_eq!(
        size_of::<OptCell<Node<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>>>(),
        704
    );
}

#[test]
fn fresh_nodes_have_no_cold_link() {
    let leaf = Node::<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>::new_leaf();
    assert!(leaf.cold_link().is_none());

    let internal = Node::<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>::new_internal();
    assert!(internal.cold_link().is_none());

    let default_node = Node::<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>::default();
    assert!(default_node.cold_link().is_none());
}

#[test]
fn cloning_a_node_never_carries_a_cold_link() {
    // Documented, intentional behavior (see `Clone`'s doc comment on
    // `Node`) -- not yet a real scenario since nothing populates a link,
    // but pin the behavior now so a future change here is a deliberate
    // decision, not an accident.
    let leaf = Node::<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>::new_leaf();
    assert!(leaf.clone().cold_link().is_none());

    let internal = Node::<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>::new_internal();
    assert!(internal.clone().cold_link().is_none());
}

#[test]
fn cold_link_accessors_round_trip() {
    // Exercises ColdLink::new/getters directly (not through Node, since
    // nothing installs a real one on a Node yet) -- a null BlockRef stands
    // in for a real cold-page pointer, which is fine here: this test is
    // only checking that the plain scalar fields round-trip, not that the
    // pointer is dereferenced (it never should be while none()/unpopulated
    // in this stage).
    type Link = ColdLink<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>;

    let none = Link::none();
    assert!(none.is_none());
    assert_eq!(none.cold_count(), 0);
    assert_eq!(none.min_protecting_ts_start(), 0);
    assert_eq!(none.chain_len(), 0);
    assert_eq!(none.chain_total_count(), 0);

    // `ColdLink::new` asserts a non-null pointer and chain_len > 0 -- both
    // invariants of a *populated* link. Stand in a real (if otherwise
    // unused, never dereferenced here) `OptCell<Block<..>>` -- the actual
    // pointee type `BlockRef`/`SmartCell` expects -- so this test's pointer
    // isn't a lie about the type nothing downstream should copy.
    let cell: crate::mv_sync::smart_cell::OptCell<
        crate::mv_block::block::Block<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>,
    > = Default::default();
    let cold = crate::mv_sync::smart_cell::SmartCell(std::ptr::addr_of!(cell));

    let populated = Link::new(cold, 7, 42, 2, 11);
    assert!(!populated.is_none());
    assert_eq!(populated.cold_count(), 7);
    assert_eq!(populated.min_protecting_ts_start(), 42);
    assert_eq!(populated.chain_len(), 2);
    assert_eq!(populated.chain_total_count(), 11);
    assert_eq!(populated.cold().0, cold.0);
}

#[test]
fn on_reuse_clears_a_populated_cold_link() {
    // A block GC hands back via `free_block` gets `on_reuse()`'d before its
    // new owner writes anything -- if that didn't clear a leftover
    // `cold_link` from the block's *previous* life, the new owner would
    // silently inherit a stale (and eventually dangling, once the old
    // owner's cold page is itself retired) pointer it never wrote and has
    // no way to detect.
    let cell: crate::mv_sync::smart_cell::OptCell<
        crate::mv_block::block::Block<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>,
    > = Default::default();
    let cold = crate::mv_sync::smart_cell::SmartCell(std::ptr::addr_of!(cell));
    let link = ColdLink::<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>::new(cold, 1, 1, 1, 1);

    let mut node = Node::<TEST_FAN, TEST_RECORDS, ProdKey, ProdPayload>::new_leaf_with_cold_link(link);
    assert!(!node.cold_link().is_none());

    node.on_reuse();
    assert!(
        node.cold_link().is_none(),
        "on_reuse() must clear a leftover cold_link from the block's previous life"
    );
}
