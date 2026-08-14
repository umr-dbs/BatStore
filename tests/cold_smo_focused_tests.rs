use crate::mv_block::block::Block;
use crate::mv_page_model::node::{ColdLink, Node};
use crate::mv_query::interval::Interval;
use crate::mv_query::iter_query::RangeQueryIter;
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::tx_stamp::TxStamp;
use crate::mv_record_model::version_info::VersionInfo;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::{OptCell, SmartCell};
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_tree::smo::BlockSplit;
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

fn make_three_page_output(tree: &Tree) -> (SmartCell<TestBlock>, TxStamp) {
    let delete = TxStamp::new(0, 500);
    tree.ctx.on_tx_start(delete.ts_start());

    let mut oldest = Node::new_leaf();
    for ts in 1..=N as u64 {
        push_protected(&mut oldest, 7, ts, delete, ts * 10);
    }
    let oldest = boxed_leaf(oldest);

    let mut newer = Node::new_leaf_with_cold_link(link_to(&oldest, N as u32, 1, N as u32));
    for ts in (N as u64 + 1)..=(2 * N) as u64 {
        push_protected(&mut newer, 7, ts, delete, ts * 10);
    }
    let newer = boxed_leaf(newer);

    let mut hot = Node::new_leaf_with_cold_link(link_to(&newer, N as u32, 2, (2 * N) as u32));
    for ts in (2 * N as u64 + 1)..(3 * N) as u64 {
        push_protected(&mut hot, 7, ts, delete, ts * 10);
    }
    push_live(&mut hot, 7, (3 * N) as u64, 3000);
    let source = boxed_leaf(hot);
    let output =
        output_of_version_split(tree.split(source.cell.get_mut(), &Interval::new(0, u64::MAX)));
    (output, delete)
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
    TestIter::walk_cold_chain_for_range(link, Interval::new(7, 7), &mut visible, |record| {
        range_payloads.push(*record.payload());
        true
    });
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
