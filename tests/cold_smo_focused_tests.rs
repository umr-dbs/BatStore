use crate::bat_block::block::Block;
use crate::bat_page_model::node::{ColdLink, Node};
use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_record_model::record_point::RecordPoint;
use crate::bat_record_model::tx_stamp::TxStamp;
use crate::bat_record_model::version_info::VersionInfo;
use crate::bat_sync::safe_cell::SafeCell;
use crate::bat_sync::smart_cell::{OptCell, SmartCell};
use crate::bat_tree::mvbt::MVBTSt;
use crate::bat_tree::smo::BlockSplit;
use std::collections::HashSet;

const FAN: usize = 8;
const N: usize = 8;
type Tree = MVBTSt<FAN, N, u64, u64>;
type TestBlock = Block<FAN, N, u64, u64>;
type TestCell = OptCell<TestBlock>;
type TestIter = RangeQueryIter<'static, FAN, N, u64, u64>;

fn push_live(node: &mut Node<FAN, N, u64, u64>, key: u64, ts: u64, payload: u64) {
    let leaf = node.as_leaf_page();
    let i = leaf.len();
    leaf.push_uncommitted(
        RecordPoint::new(key, VersionInfo::new(TxStamp::new(0, ts)), payload),
        i,
    );
    leaf.commit_delta(1, 0);
}

fn push_protected(
    node: &mut Node<FAN, N, u64, u64>,
    key: u64,
    insert_ts: u64,
    delete_stamp: TxStamp,
    payload: u64,
) {
    let leaf = node.as_leaf_page();
    let i = leaf.len();
    leaf.push_uncommitted(
        RecordPoint::new(
            key,
            VersionInfo::from(TxStamp::new(0, insert_ts), delete_stamp),
            payload,
        ),
        i,
    );
    leaf.commit_delta(0, 1);
}

fn boxed_leaf(node: Node<FAN, N, u64, u64>) -> Box<TestCell> {
    Box::new(OptCell::new(TestBlock {
        node_data: SafeCell::new(node),
    }))
}

fn link_to(cell: &TestCell, count: u32, chain_len: u16, total: u32) -> ColdLink<FAN, N, u64, u64> {
    ColdLink::new(SmartCell(cell as *const _), count, 500, chain_len, total)
}

fn chain_pages(mut link: ColdLink<FAN, N, u64, u64>) -> Vec<SmartCell<TestBlock>> {
    let mut pages = Vec::new();
    while !link.is_none() {
        let page = link.cold();
        let next = *page.unsafe_borrow().cold_link();
        pages.push(page);
        link = next;
    }
    pages
}

fn output_of_version_split(split: BlockSplit<FAN, N, u64, u64>) -> SmartCell<TestBlock> {
    match split {
        BlockSplit::ByVersion(page) => page,
        BlockSplit::ByKey(..) => panic!("expected an indivisible version split"),
    }
}

#[test]
fn ordinary_key_split_explicitly_creates_no_cold_pages() {
    let tree = Tree::default();
    let mut node = Node::new_leaf();
    for key in 0..N as u64 {
        push_live(&mut node, key, key + 1, key * 10);
    }
    let source = boxed_leaf(node);
    let split = tree.split(source.cell.get_mut(), &Interval::new(0, u64::MAX));

    match split {
        BlockSplit::ByKey(_, left, _, right) => {
            assert!(left.unsafe_borrow().cold_link().is_none());
            assert!(right.unsafe_borrow().cold_link().is_none());
        }
        BlockSplit::ByVersion(..) => panic!("distinct keys have a valid ordinary key split"),
    }
}

#[test]
fn indivisible_required_same_key_history_creates_one_cold_page() {
    let tree = Tree::default();
    let delete = TxStamp::new(0, 500);
    tree.ctx.on_tx_start(delete.ts_start());

    let mut cold = Node::new_leaf();
    push_protected(&mut cold, 7, 1, delete, 10);
    let cold = boxed_leaf(cold);

    let mut node = Node::new_leaf();
    for ts in 2..=N as u64 {
        push_protected(&mut node, 7, ts, delete, ts * 10);
    }
    push_live(&mut node, 7, N as u64 + 1, 900);
    node.set_cold_link(link_to(&cold, 1, 1, 1));
    let source = boxed_leaf(node);
    let output =
        output_of_version_split(tree.split(source.cell.get_mut(), &Interval::new(0, u64::MAX)));
    tree.ctx.on_tx_completed(delete.ts_start());

    let hot = output.unsafe_borrow();
    assert_eq!(hot.as_leaf_page_ref().len(), 1);
    assert_eq!(chain_pages(*hot.cold_link()).len(), 1);
    assert_eq!(hot.cold_link().chain_total_count(), N as u32);
}

#[test]
fn under_capacity_protected_history_is_opportunistically_offloaded() {
    let tree = Tree::default();
    let delete = TxStamp::new(0, 500);
    tree.ctx.on_tx_start(delete.ts_start());

    // Six retained records fit comfortably in this eight-slot leaf. Five
    // are nevertheless historical and continuously snapshot-protected: a
    // version split must shed them from the write-facing hot page instead
    // of reproducing the same six-record page and inviting another SMO.
    let mut node = Node::new_leaf();
    for ts in 1..=5 {
        push_protected(&mut node, 7, ts, delete, ts * 10);
    }
    push_live(&mut node, 7, 6, 60);
    let source = boxed_leaf(node);
    let output =
        output_of_version_split(tree.split(source.cell.get_mut(), &Interval::new(0, u64::MAX)));
    tree.ctx.on_tx_completed(delete.ts_start());

    let hot = output.unsafe_borrow();
    assert_eq!(hot.as_leaf_page_ref().len(), 1);
    assert_eq!(hot.cold_link().chain_total_count(), 5);
    assert_eq!(chain_pages(*hot.cold_link()).len(), 1);
}

#[test]
fn hot_key_split_keeps_capacity_exact_side_when_its_cold_slice_is_empty() {
    let tree = Tree::default();
    let delete = TxStamp::new(0, 500);
    tree.ctx.on_tx_start(delete.ts_start());

    // Key 2's snapshot-protected history (9 dead versions -- one page can't
    // hold that alone) already sits in a private two-page chain, exactly
    // like an earlier SMO would have left it. Key 1 supplies N (= 8) live,
    // currently-visible versions directly on the resident page, with no
    // garbage of its own. Combined (17 records), the only key boundary
    // (between key 1 and key 2, at index 8) doesn't fit capacity on its raw
    // right side -- forcing `split()` into the `try_hot_key_split` fallback.
    // There, key 1's write-facing half lands exactly at capacity, but
    // carries none of key 2's garbage: it must come out as a plain,
    // fully-packed `ByKey` leaf, not get needlessly forced into a
    // cold-chained `ByVersion` fallback just because it happens to land
    // exactly at capacity.
    let mut oldest = Node::new_leaf();
    for ts in 1..=5 {
        push_protected(&mut oldest, 2, ts, delete, ts * 10);
    }
    let oldest = boxed_leaf(oldest);

    let mut newer = Node::new_leaf_with_cold_link(link_to(&oldest, 5, 1, 5));
    for ts in 6..=9 {
        push_protected(&mut newer, 2, ts, delete, ts * 10);
    }
    let newer = boxed_leaf(newer);

    let mut node = Node::new_leaf_with_cold_link(link_to(&newer, 4, 2, 9));
    for ts in 1..=N as u64 {
        push_live(&mut node, 1, ts, ts * 10);
    }
    let source = boxed_leaf(node);

    let split = tree.split(source.cell.get_mut(), &Interval::new(0, u64::MAX));
    tree.ctx.on_tx_completed(delete.ts_start());

    match split {
        BlockSplit::ByKey(_, left, _, right) => {
            assert!(left.unsafe_borrow().cold_link().is_none());
            assert_eq!(left.unsafe_borrow().as_leaf_page_ref().len(), N);
            assert!(!right.unsafe_borrow().cold_link().is_none());
        }
        BlockSplit::ByVersion(..) => panic!(
            "key 1's capacity-exact half carries no garbage of its own and \
             should not need cold offload to split"
        ),
    }
}

/// Builds `chain_pages` pages' worth (each holding exactly `N` retained
/// same-key records, oldest first) of snapshot-protected history for key 7,
/// then a final write-facing page with `N - 1` more protected records plus
/// one live record, and runs a real `split()` over the whole thing - the
/// same shape `make_three_page_output` (below) builds by hand, generalized
/// so the "many cold pages" tests don't need their own hand-rolled 6-page
/// version of this construction.
fn make_chain_output(tree: &Tree, chain_pages: usize) -> (SmartCell<TestBlock>, TxStamp) {
    assert!(chain_pages >= 1);
    let delete = TxStamp::new(0, 500);
    tree.ctx.on_tx_start(delete.ts_start());

    // Every constructed page must outlive the `split()` call below (its
    // input chain is only ever read, never retained past that call - see
    // `retained_owned_leaf_history`'s doc - but Rust still needs the
    // pointee alive for the duration of the call itself).
    let mut pages: Vec<Box<TestCell>> = Vec::new();
    let mut link = ColdLink::none();
    for page_idx in 0..(chain_pages - 1) {
        let mut node = Node::new_leaf();
        let base = page_idx as u64 * N as u64;
        for offset in 1..=N as u64 {
            push_protected(&mut node, 7, base + offset, delete, (base + offset) * 10);
        }
        if !link.is_none() {
            node.set_cold_link(link);
        }
        let cell = boxed_leaf(node);
        let total = (page_idx + 1) as u32 * N as u32;
        link = link_to(&cell, N as u32, (page_idx + 1) as u16, total);
        pages.push(cell);
    }

    let hot_base = (chain_pages - 1) as u64 * N as u64;
    let mut hot = if link.is_none() {
        Node::new_leaf()
    } else {
        Node::new_leaf_with_cold_link(link)
    };
    for offset in 1..N as u64 {
        push_protected(&mut hot, 7, hot_base + offset, delete, (hot_base + offset) * 10);
    }
    push_live(&mut hot, 7, hot_base + N as u64, (hot_base + N as u64) * 10);
    let source = boxed_leaf(hot);
    let output =
        output_of_version_split(tree.split(source.cell.get_mut(), &Interval::new(0, u64::MAX)));
    (output, delete)
}

fn make_three_page_output(tree: &Tree) -> (SmartCell<TestBlock>, TxStamp) {
    make_chain_output(tree, 3)
}

#[test]
fn generated_multi_page_chain_serves_point_and_range_reads() {
    let tree = Tree::default();
    let (output, delete) = make_three_page_output(&tree);
    let hot = output.unsafe_borrow();
    let link = *hot.cold_link();
    assert_eq!(chain_pages(link).len(), 3);
    assert_eq!(link.chain_total_count(), (3 * N - 1) as u32);

    let wanted = TxStamp::new(0, 2);
    let mut visible = |stamp: TxStamp| stamp == wanted;
    let point = Tree::scan_cold_chain_for_key(link, 7, &mut visible)
        .expect("point lookup must reach the oldest generated cold page");
    assert_eq!(*point.payload, 20);

    let mut visible = |stamp: TxStamp| stamp == wanted;
    let mut range_payloads = Vec::new();
    TestIter::walk_cold_chain_for_range(
        link,
        Interval::new(7, 7),
        &mut visible,
        std::collections::HashSet::new(),
        |record| {
            range_payloads.push(*record.payload());
            true
        },
    );
    assert_eq!(range_payloads, vec![20]);
    tree.ctx.on_tx_completed(delete.ts_start());
}

#[test]
fn cold_chain_reclamation_respects_gc_off_and_on() {
    // GC off: retirement is not queued, so none of this generation is reused.
    let off = Tree::default();
    let (off_hot, off_delete) = make_three_page_output(&off);
    let mut off_owned: HashSet<usize> = chain_pages(*off_hot.unsafe_borrow().cold_link())
        .into_iter()
        .map(|p| p.0 as usize)
        .collect();
    off_owned.insert(off_hot.0 as usize);
    off.ctx.on_tx_completed(off_delete.ts_start());
    off.block_manager.register_dead(0, 1, off_hot);
    let fresh = off.block_manager.new_empty_leaf(&off.ctx);
    assert!(!off_owned.contains(&(fresh.0 as usize)));

    // GC on: proving the owner hot page reclaimable cascades its private
    // chain directly into the reusable cache under the same death version.
    let on = Tree::default();
    on.enable_gc(false);
    let (on_hot, on_delete) = make_three_page_output(&on);
    let mut expected: HashSet<usize> = chain_pages(*on_hot.unsafe_borrow().cold_link())
        .into_iter()
        .map(|p| p.0 as usize)
        .collect();
    expected.insert(on_hot.0 as usize);
    on.ctx.on_tx_completed(on_delete.ts_start());
    on.block_manager.register_dead(0, 1, on_hot);

    let reused: HashSet<usize> = (0..expected.len())
        .map(|_| on.block_manager.new_empty_leaf(&on.ctx).0 as usize)
        .collect();
    assert_eq!(reused, expected);
}

/// Same shape as `generated_multi_page_chain_serves_point_and_range_reads`,
/// but with a chain long enough (6 pages, not 3) to guard against a bug
/// that only surfaces once a leaf's `cold_link` has several hops - e.g. a
/// walk that happens to work when it terminates after one or two hops but
/// silently drops or duplicates records once it has to keep going further.
#[test]
fn generated_six_page_chain_serves_point_and_range_reads_at_every_depth() {
    const CHAIN_PAGES: usize = 6;
    let tree = Tree::default();
    let (output, delete) = make_chain_output(&tree, CHAIN_PAGES);
    let hot = output.unsafe_borrow();
    let link = *hot.cold_link();
    assert_eq!(chain_pages(link).len(), CHAIN_PAGES);
    assert_eq!(
        link.chain_total_count(),
        (CHAIN_PAGES as u32 * N as u32 - 1)
    );

    // ts=2 sits in the very first (oldest, deepest) page built - reaching
    // it requires walking every one of the other 5 pages first.
    let wanted = TxStamp::new(0, 2);
    let mut visible = |stamp: TxStamp| stamp == wanted;
    let point = Tree::scan_cold_chain_for_key(link, 7, &mut visible)
        .expect("point lookup must reach the deepest of 6 chained cold pages");
    assert_eq!(*point.payload, 20);

    // ts=N*(CHAIN_PAGES-1)+1 sits in the newest cold page (the one the hot
    // leaf's own `cold_link` points to directly) - the opposite extreme,
    // confirming the walk also correctly returns a *shallow* match instead
    // of over-walking past it.
    let shallow_ts = (N as u64) * (CHAIN_PAGES as u64 - 1) + 1;
    let shallow_wanted = TxStamp::new(0, shallow_ts);
    let mut visible = |stamp: TxStamp| stamp == shallow_wanted;
    let point = Tree::scan_cold_chain_for_key(link, 7, &mut visible)
        .expect("point lookup must find a match on the newest (first-visited) cold page");
    assert_eq!(*point.payload, shallow_ts * 10);

    let mut visible = |stamp: TxStamp| stamp == wanted;
    let mut range_payloads = Vec::new();
    TestIter::walk_cold_chain_for_range(
        link,
        Interval::new(7, 7),
        &mut visible,
        std::collections::HashSet::new(),
        |record| {
            range_payloads.push(*record.payload());
            true
        },
    );
    assert_eq!(range_payloads, vec![20]);
    tree.ctx.on_tx_completed(delete.ts_start());
}

/// Long-chain analogue of `cold_chain_reclamation_respects_gc_off_and_on`:
/// confirms GC reclamation cascades through *every* page of a 6-page chain,
/// not just the 1-2 hops the shorter test happens to cover.
#[test]
fn long_chain_reclamation_cascades_every_page_into_reuse() {
    const CHAIN_PAGES: usize = 6;
    let on = Tree::default();
    on.enable_gc(false);
    let (on_hot, on_delete) = make_chain_output(&on, CHAIN_PAGES);
    let owned = chain_pages(*on_hot.unsafe_borrow().cold_link());
    assert_eq!(owned.len(), CHAIN_PAGES);
    let mut expected: HashSet<usize> = owned.into_iter().map(|p| p.0 as usize).collect();
    expected.insert(on_hot.0 as usize);
    assert_eq!(expected.len(), CHAIN_PAGES + 1, "hot page plus all 6 cold pages, no aliasing");

    on.ctx.on_tx_completed(on_delete.ts_start());
    on.block_manager.register_dead(0, 1, on_hot);

    let reused: HashSet<usize> = (0..expected.len())
        .map(|_| on.block_manager.new_empty_leaf(&on.ctx).0 as usize)
        .collect();
    assert_eq!(
        reused, expected,
        "every page of the 6-page chain (plus the hot page) must come back through reuse"
    );
}
