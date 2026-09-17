//! Walks every root* version of an `MVBTSt` and every block reachable from
//! them, and serializes the *structure* (page type, key ranges, active/dead
//! counts, child pointers, and per-record key + `VersionInfo` metadata) to a
//! JSON file - deliberately never the record `Payload` values themselves, so
//! a dump can never leak the data stored in the tree, only its shape.
//!
//! Physical blocks are shared across many versions in this persistent
//! (path-copying) tree - an unchanged subtree under an old root is the exact
//! same block a newer root also points to. Every node is keyed by its raw
//! block-pointer address and only serialized once (`visited`), so the
//! `nodes` map in the output is the de-duplicated block graph, not one full
//! copy of the tree per root.
//!
//! Read-safety caveat: this reads pages via `unsafe_borrow` (a raw, non-OLC
//! -validated borrow - the same primitive `RootIndexGuard::unsafe_degree_root`
//! already uses for read-only structural introspection elsewhere in this
//! crate), not the versioned/validated read path `bat_query` uses for actual
//! transactions. That is fine for a debug/inspection dump taken at a
//! quiescent point (no concurrent writers mutating the tree), but does not
//! tolerate concurrent structural modification (splits/merges) while the
//! dump runs.

use crate::bat_page_model::node::PageType;
use crate::bat_page_model::BlockRef;
use crate::bat_root::index_root::RootIndex;
use crate::bat_root::tree_root::{TREE_ROOT_MAX_KEY, TREE_ROOT_MIN_KEY};
use crate::bat_tree::mvbt::MVBTSt;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::hash::Hash;
use std::io;
use std::path::Path;

#[derive(Serialize)]
pub struct TreeDump {
    /// Next timestamp the GLC will hand out, serialized as text for JS BigInt.
    pub glc_next: String,
    pub glc_last: String,
    pub max_worker_id: usize,
    pub commit_logs: Vec<Vec<String>>,
    pub historical_visibility_complete: bool,
    pub fan_out: usize,
    pub num_records: usize,
    pub root_index_type: String,
    pub roots: Vec<RootDump>,
    pub nodes: BTreeMap<String, NodeDump>,
}

#[derive(Serialize)]
pub struct RootDump {
    pub version: u64,
    pub height: u16,
    pub node_id: String,
}

#[derive(Serialize)]
#[serde(tag = "type")]
pub enum NodeDump {
    #[serde(rename = "internal")]
    Internal {
        active: u32,
        dead: u32,
        min_key: String,
        max_key: String,
        children: Vec<ChildDump>,
    },
    /// Emitted instead of `Internal` once `max_depth` is reached - the
    /// subtree below genuinely exists but was not walked, so the file size
    /// stays bounded for very large trees. Never emitted for a node that was
    /// already fully dumped via a shallower path to the same physical block.
    #[serde(rename = "internal_truncated")]
    InternalTruncated {
        active: u32,
        dead: u32,
        min_key: String,
        max_key: String,
        child_count: usize,
    },
    #[serde(rename = "leaf")]
    Leaf {
        active: u32,
        dead: u32,
        min_key: String,
        max_key: String,
        records: Vec<RecordDump>,
    },
}

#[derive(Serialize)]
pub struct ChildDump {
    pub node_id: String,
    pub key_lower: String,
    pub key_upper: String,
    pub version: u64,
    /// Whether this slot is still an effective routing entry for current
    /// reads (`InternalPage::live_mask`), vs. superseded/obsolete history
    /// kept around in the same physical page only for older-snapshot readers
    /// or pending GC reclaim. A page's slots are append-only and never
    /// compacted in place, so a currently-reachable internal node can (and
    /// commonly does) hold a mix of both.
    pub live: bool,
}

#[derive(Serialize)]
pub struct RecordDump {
    pub key: String,
    pub insert_worker: u16,
    pub insert_ts: u64,
    pub insert_invalid: bool,
    pub deleted: bool,
    pub delete_worker: Option<u16>,
    pub delete_ts: Option<u64>,
    pub delete_invalid: bool,
}

/// Dumps `tree`'s full root* list and the (de-duplicated) block graph they
/// reach to a pretty-printed JSON file at `path`. `max_depth` bounds how many
/// internal-page hops (from whichever root a given block is first reached
/// through) get walked before a subtree is recorded as
/// `NodeDump::InternalTruncated` instead of being recursed into - `None`
/// walks every reachable block regardless of depth.
pub fn dump_tree_to_file<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Default + Clone + Display + Sync + 'static,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    path: impl AsRef<Path>,
    max_depth: Option<usize>,
) -> io::Result<()> {
    let dump = build_tree_dump(tree, max_depth);
    let file = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(file, &dump)?;
    Ok(())
}

pub(crate) fn build_tree_dump<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Default + Clone + Display + Sync + 'static,
>(
    tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    max_depth: Option<usize>,
) -> TreeDump {
    let mut nodes = BTreeMap::new();
    let mut visited = std::collections::HashSet::new();

    let roots = all_roots(&tree.root)
        .into_iter()
        .map(|(version, height, block)| RootDump {
            version,
            height,
            node_id: dump_node(&block, 0, max_depth, &mut visited, &mut nodes),
        })
        .collect();

    let (commit_logs, historical_visibility_complete) = tree.ctx.dump_commit_logs();
    let glc_next = tree.ctx.current_version();
    TreeDump {
        glc_next: glc_next.to_string(),
        glc_last: glc_next.saturating_sub(1).to_string(),
        max_worker_id: tree.ctx.max_worker_id(),
        commit_logs,
        historical_visibility_complete,
        fan_out: FAN_OUT,
        num_records: NUM_RECORDS,
        root_index_type: tree.root_star_index().to_string(),
        roots,
        nodes,
    }
}

/// Enumerates every `(version, height, root block)` this root* index has
/// ever appended - i.e. the *entire* history, not just the current root -
/// regardless of which of the four `RootIndexType` backings is in use.
fn all_roots<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Default + Clone + Display + Sync + 'static,
>(
    root: &RootIndex<FAN_OUT, NUM_RECORDS, Key, Payload>,
) -> Vec<(u64, u16, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)> {
    match root {
        RootIndex::FrugalList(fg) => fg
            .unsafe_borrow()
            .iter()
            .map(|node| (node.insert_version, node.payload.height(), node.payload.block()))
            .collect(),
        RootIndex::LinkedList(ll) => ll
            .unsafe_borrow()
            .iter()
            .map(|r| (r.version(), r.height(), r.block()))
            .collect(),
        RootIndex::SkipList(sk) => sk
            .unsafe_borrow()
            .0
            .iter()
            .map(|e| (*e.key(), e.value().height(), e.value().block()))
            .collect(),
        RootIndex::BTree(bt) => {
            use CCBPlusTree::crud_model::crud_api::CRUDDispatcher;
            use CCBPlusTree::crud_model::crud_operation::CRUDOperation;
            use CCBPlusTree::crud_model::crud_operation_result::CRUDOperationResult;

            let (_, result) = bt.unsafe_borrow().0.dispatch(CRUDOperation::Range(
                (TREE_ROOT_MIN_KEY, TREE_ROOT_MAX_KEY).into(),
            ));

            match result {
                CRUDOperationResult::MatchedRecords(recs) => recs
                    .into_iter()
                    .map(|r| (r.key, r.payload.height(), r.payload.block()))
                    .collect(),
                _ => Vec::new(),
            }
        }
    }
}

/// Depth-first, de-duplicating dump of `block` and everything below it into
/// `nodes`. Returns `block`'s node id (its raw pointer address, hex-encoded)
/// whether or not this call actually performed the dump - a node already
/// present in `visited` is a shared, already-dumped subtree and is not
/// walked or recorded again.
fn dump_node<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Default + Clone + Display + Sync + 'static,
>(
    block: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    depth: usize,
    max_depth: Option<usize>,
    visited: &mut std::collections::HashSet<usize>,
    nodes: &mut BTreeMap<String, NodeDump>,
) -> String {
    let id = block.0 as usize;
    let id_str = format!("{id:x}");

    if !visited.insert(id) {
        return id_str;
    }

    let node_ref = block.unsafe_borrow();

    let dump = match node_ref.as_page_ref() {
        PageType::LeafRef(leaf) => {
            let (active, dead) = leaf.active_dead_count();
            let records: Vec<RecordDump> = leaf
                .as_records()
                .iter()
                .map(|r| {
                    let insert = r.version().insertion_stamp();
                    let delete = r.version().deletion_stamp();
                    RecordDump {
                        key: r.key().to_string(),
                        insert_worker: insert.worker_id(),
                        insert_ts: insert.ts_start(),
                        insert_invalid: insert.is_invalid(),
                        deleted: r.version().is_deleted(),
                        delete_worker: delete.map(|s| s.worker_id()),
                        delete_ts: delete.map(|s| s.ts_start()),
                        delete_invalid: delete.is_some_and(|s| s.is_invalid()),
                    }
                })
                .collect();

            let min_key = records.first().map(|r| r.key.clone()).unwrap_or_default();
            let max_key = records.last().map(|r| r.key.clone()).unwrap_or_default();

            NodeDump::Leaf { active, dead, min_key, max_key, records }
        }
        PageType::IndexRef(internal) => {
            let (active, dead) = internal.active_dead_count();
            let (keys, versions, children) = internal.keys_versions_pointers();

            let min_key = keys.first().map(|k| k.lower().to_string()).unwrap_or_default();
            let max_key = keys.last().map(|k| k.upper().to_string()).unwrap_or_default();

            if max_depth.is_some_and(|max| depth >= max) {
                NodeDump::InternalTruncated { active, dead, min_key, max_key, child_count: children.len() }
            } else {
                let children = keys
                    .iter()
                    .zip(versions.iter())
                    .zip(children.iter())
                    .enumerate()
                    .map(|(index, ((key, version), child))| ChildDump {
                        node_id: dump_node(child, depth + 1, max_depth, visited, nodes),
                        key_lower: key.lower().to_string(),
                        key_upper: key.upper().to_string(),
                        version: *version,
                        live: internal.is_slot_live(index),
                    })
                    .collect();

                NodeDump::Internal { active, dead, min_key, max_key, children }
            }
        }
        _ => unreachable!("BlockRef always resolves to a Leaf or Internal page"),
    };

    nodes.insert(id_str.clone(), dump);
    id_str
}
