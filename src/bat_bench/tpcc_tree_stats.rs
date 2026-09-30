//! Quiescent current-tree audit and output for the opt-in TPC-C experiment.

use crate::bat_bench::tpcc_schema::{BigTreeOp, Table, TpccDatabase, TpccKey, TpccRow, TreeClass};
use crate::bat_page_model::BlockRef;
use crate::bat_page_model::node::PageType;
use crate::bat_tree::mvbt::MVBTSt;
use crate::bat_tree::smo::BlockUnsafeDegree::{ActiveUnderflow, Overflow};
use crate::bat_tree::stats::{SmoKind, SmoSnapshot};
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

#[derive(Clone, Debug, Serialize)]
pub struct NodeFilling {
    checkpoint: String,
    table: String,
    node_id: String,
    parent_id: String,
    depth: usize,
    is_root: bool,
    node_type: &'static str,
    capacity: usize,
    overflow_threshold: usize,
    live: u32,
    dead: u32,
    total: u32,
    logical_fill: f64,
    physical_fill: f64,
    garbage_ratio: f64,
    weak_d: usize,
    strong_min: usize,
    strong_max: usize,
    below_strong_min: bool,
    above_strong_max: bool,
    strict_weak_violation: bool,
    at_weak_boundary: bool,
    repair_due: bool,
    overflow_due: bool,
    root_collapse_due: bool,
    min_key: String,
    max_key: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct TreeSummary {
    checkpoint: String,
    table: String,
    height: u16,
    nodes: usize,
    internal_nodes: usize,
    leaf_nodes: usize,
    live_entries: u64,
    dead_entries: u64,
    logical_fill_mean: f64,
    logical_fill_p05: f64,
    logical_fill_p50: f64,
    logical_fill_p95: f64,
    physical_fill_mean: f64,
    strict_weak_violations: usize,
    weak_boundary_nodes: usize,
    repair_due_nodes: usize,
    overflow_due_nodes: usize,
    root_collapse_due: bool,
}

#[derive(Clone, Debug)]
struct TreeAudit {
    height: u16,
    rows: Vec<NodeFilling>,
}

#[derive(Clone, Debug)]
pub struct CheckpointReport {
    all: TreeSummary,
    per_table: Vec<TreeSummary>,
}

#[derive(Clone, Debug, Default)]
pub struct DatabaseSmoSnapshot {
    by_table: BTreeMap<String, SmoSnapshot>,
}

impl DatabaseSmoSnapshot {
    pub fn phase_since(&self, earlier: &Self) -> Self {
        let mut by_table = BTreeMap::new();
        for (table, current) in &self.by_table {
            by_table.insert(
                table.clone(),
                current.saturating_sub(earlier.by_table.get(table).copied().unwrap_or_default()),
            );
        }
        Self { by_table }
    }
}

fn audit_tree<const F: usize, const N: usize>(
    tree: &MVBTSt<F, N, TpccKey, TpccRow>,
    table: &str,
    checkpoint: &str,
) -> TreeAudit {
    let root = tree.root.current_root();
    let mut rows = Vec::new();
    let mut visited = HashSet::new();
    walk_current(
        tree,
        root.block(),
        table,
        checkpoint,
        "",
        0,
        true,
        &mut visited,
        &mut rows,
    );
    TreeAudit {
        height: root.height(),
        rows,
    }
}

#[allow(clippy::too_many_arguments)]
fn walk_current<const F: usize, const N: usize>(
    tree: &MVBTSt<F, N, TpccKey, TpccRow>,
    block: BlockRef<F, N, TpccKey, TpccRow>,
    table: &str,
    checkpoint: &str,
    parent_id: &str,
    depth: usize,
    is_root: bool,
    visited: &mut HashSet<usize>,
    rows: &mut Vec<NodeFilling>,
) {
    let raw_id = block.0 as usize;
    if !visited.insert(raw_id) {
        return;
    }
    let id = format!("{raw_id:x}");
    let node = block.unsafe_borrow();
    let (live, dead) = node.active_dead_count();
    let total = live + dead;
    let capacity = node.max_units();
    let overflow_threshold = node.overflow_units_count();
    let weak_d = node.filling_20_percent();
    let strong_min = node.filling_40_percent();
    let strong_max = node.filling_80_percent();
    let logical_fill = live as f64 / capacity as f64;
    let physical_fill = total as f64 / capacity as f64;
    let garbage_ratio = if total == 0 {
        0.0
    } else {
        dead as f64 / total as f64
    };
    let degree = if is_root {
        node.unsafe_degree_root()
    } else {
        node.unsafe_degree(&tree.ctx)
    };

    let (node_type, min_key, max_key, children) = match node.as_page_ref() {
        PageType::LeafRef(leaf) => {
            let mut keys = leaf
                .as_records()
                .iter()
                .filter(|record| record.version().is_live())
                .map(|record| record.key());
            let first = keys.next();
            let last = keys.last().or(first);
            (
                "leaf",
                first.map(|k| k.to_string()).unwrap_or_default(),
                last.map(|k| k.to_string()).unwrap_or_default(),
                Vec::new(),
            )
        }
        PageType::IndexRef(internal) => {
            let (keys, _, pointers) = internal.keys_versions_pointers();
            let live_slots: Vec<_> = (0..keys.len())
                .filter(|&index| internal.is_slot_live(index))
                .collect();
            let min = live_slots
                .first()
                .map(|&index| keys[index].lower().to_string())
                .unwrap_or_default();
            let max = live_slots
                .last()
                .map(|&index| keys[index].upper().to_string())
                .unwrap_or_default();
            let children = live_slots
                .into_iter()
                .map(|index| pointers[index])
                .collect();
            ("internal", min, max, children)
        }
        _ => unreachable!("current tree contains only leaf/internal blocks"),
    };

    rows.push(NodeFilling {
        checkpoint: checkpoint.to_string(),
        table: table.to_string(),
        node_id: id.clone(),
        parent_id: parent_id.to_string(),
        depth,
        is_root,
        node_type,
        capacity,
        overflow_threshold,
        live,
        dead,
        total,
        logical_fill,
        physical_fill,
        garbage_ratio,
        weak_d,
        strong_min,
        strong_max,
        below_strong_min: (live as usize) < strong_min,
        above_strong_max: (live as usize) > strong_max,
        strict_weak_violation: !is_root && live > 0 && (live as usize) < weak_d,
        at_weak_boundary: !is_root && live as usize == weak_d,
        repair_due: !is_root && matches!(degree, ActiveUnderflow),
        overflow_due: matches!(degree, Overflow),
        root_collapse_due: is_root && node_type == "internal" && matches!(degree, ActiveUnderflow),
        min_key,
        max_key,
    });

    for child in children {
        walk_current(
            tree,
            child,
            table,
            checkpoint,
            &id,
            depth + 1,
            false,
            visited,
            rows,
        );
    }
}

fn percentile(mut values: Vec<f64>, p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    let index = ((values.len() - 1) as f64 * p).round() as usize;
    values[index]
}

fn summarize(checkpoint: &str, table: &str, height: u16, rows: &[NodeFilling]) -> TreeSummary {
    let logical: Vec<_> = rows.iter().map(|r| r.logical_fill).collect();
    let physical: Vec<_> = rows.iter().map(|r| r.physical_fill).collect();
    TreeSummary {
        checkpoint: checkpoint.to_string(),
        table: table.to_string(),
        height,
        nodes: rows.len(),
        internal_nodes: rows.iter().filter(|r| r.node_type == "internal").count(),
        leaf_nodes: rows.iter().filter(|r| r.node_type == "leaf").count(),
        live_entries: rows.iter().map(|r| r.live as u64).sum(),
        dead_entries: rows.iter().map(|r| r.dead as u64).sum(),
        logical_fill_mean: logical.iter().sum::<f64>() / logical.len().max(1) as f64,
        logical_fill_p05: percentile(logical.clone(), 0.05),
        logical_fill_p50: percentile(logical.clone(), 0.50),
        logical_fill_p95: percentile(logical, 0.95),
        physical_fill_mean: physical.iter().sum::<f64>() / physical.len().max(1) as f64,
        strict_weak_violations: rows.iter().filter(|r| r.strict_weak_violation).count(),
        weak_boundary_nodes: rows.iter().filter(|r| r.at_weak_boundary).count(),
        repair_due_nodes: rows.iter().filter(|r| r.repair_due).count(),
        overflow_due_nodes: rows.iter().filter(|r| r.overflow_due).count(),
        root_collapse_due: rows.iter().any(|r| r.root_collapse_due),
    }
}

struct AuditOp<'a> {
    table: &'a str,
    checkpoint: &'a str,
}

impl BigTreeOp for AuditOp<'_> {
    type Output = TreeAudit;
    fn run<const F: usize, const N: usize>(
        self,
        tree: &MVBTSt<F, N, TpccKey, TpccRow>,
    ) -> Self::Output {
        audit_tree(tree, self.table, self.checkpoint)
    }
}

struct SnapshotOp;
impl BigTreeOp for SnapshotOp {
    type Output = SmoSnapshot;
    fn run<const F: usize, const N: usize>(
        self,
        tree: &MVBTSt<F, N, TpccKey, TpccRow>,
    ) -> Self::Output {
        tree.smo_stats.snapshot()
    }
}

pub fn snapshot_smos(db: &TpccDatabase) -> DatabaseSmoSnapshot {
    let mut by_table = BTreeMap::new();
    for table in Table::ALL {
        let snapshot = if table.class() == TreeClass::Big {
            db.dispatch_big(table, SnapshotOp)
        } else {
            db.tree_for(table).smo_stats.snapshot()
        };
        by_table.insert(table.as_str().to_string(), snapshot);
    }
    DatabaseSmoSnapshot { by_table }
}

fn csv_writer(path: &Path, header: &str) -> io::Result<fs::File> {
    let needs_header = !path.exists() || path.metadata()?.len() == 0;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if needs_header {
        writeln!(file, "{header}")?;
    }
    Ok(file)
}

pub fn write_checkpoint(
    db: &TpccDatabase,
    output: &Path,
    checkpoint: &str,
) -> io::Result<CheckpointReport> {
    fs::create_dir_all(output)?;
    let mut node_file = csv_writer(
        &output.join("node_filling.csv"),
        "checkpoint,table,node_id,parent_id,depth,is_root,node_type,capacity,overflow_threshold,live,dead,total,logical_fill,physical_fill,garbage_ratio,weak_d,strong_min,strong_max,below_strong_min,above_strong_max,strict_weak_violation,at_weak_boundary,repair_due,overflow_due,root_collapse_due,min_key,max_key",
    )?;
    let mut summary_file = csv_writer(
        &output.join("tree_summary.csv"),
        "checkpoint,table,height,nodes,internal_nodes,leaf_nodes,live_entries,dead_entries,logical_fill_mean,logical_fill_p05,logical_fill_p50,logical_fill_p95,physical_fill_mean,strict_weak_violations,weak_boundary_nodes,repair_due_nodes,overflow_due_nodes,root_collapse_due",
    )?;

    let mut all_rows = Vec::new();
    let mut per_table = Vec::new();
    let mut max_height = 0;
    for table in Table::ALL {
        let name = table.as_str();
        let audit = if table.class() == TreeClass::Big {
            db.dispatch_big(
                table,
                AuditOp {
                    table: name,
                    checkpoint,
                },
            )
        } else {
            audit_tree(&db.tree_for(table), name, checkpoint)
        };
        max_height = max_height.max(audit.height);
        for row in &audit.rows {
            writeln!(
                node_file,
                "{},{},{},{},{},{},{},{},{},{},{},{},{:.9},{:.9},{:.9},{},{},{},{},{},{},{},{},{},{},{},{}",
                row.checkpoint,
                row.table,
                row.node_id,
                row.parent_id,
                row.depth,
                row.is_root,
                row.node_type,
                row.capacity,
                row.overflow_threshold,
                row.live,
                row.dead,
                row.total,
                row.logical_fill,
                row.physical_fill,
                row.garbage_ratio,
                row.weak_d,
                row.strong_min,
                row.strong_max,
                row.below_strong_min,
                row.above_strong_max,
                row.strict_weak_violation,
                row.at_weak_boundary,
                row.repair_due,
                row.overflow_due,
                row.root_collapse_due,
                row.min_key,
                row.max_key
            )?;
        }
        let summary = summarize(checkpoint, name, audit.height, &audit.rows);
        write_summary(&mut summary_file, &summary)?;
        per_table.push(summary);
        all_rows.extend(audit.rows);
    }
    let all = summarize(checkpoint, "__all__", max_height, &all_rows);
    write_summary(&mut summary_file, &all)?;
    Ok(CheckpointReport { all, per_table })
}

fn write_summary(file: &mut fs::File, s: &TreeSummary) -> io::Result<()> {
    writeln!(
        file,
        "{},{},{},{},{},{},{},{},{:.9},{:.9},{:.9},{:.9},{:.9},{},{},{},{},{}",
        s.checkpoint,
        s.table,
        s.height,
        s.nodes,
        s.internal_nodes,
        s.leaf_nodes,
        s.live_entries,
        s.dead_entries,
        s.logical_fill_mean,
        s.logical_fill_p05,
        s.logical_fill_p50,
        s.logical_fill_p95,
        s.physical_fill_mean,
        s.strict_weak_violations,
        s.weak_boundary_nodes,
        s.repair_due_nodes,
        s.overflow_due_nodes,
        s.root_collapse_due
    )
}

pub fn write_smo_phase(
    output: &Path,
    phase: &str,
    snapshot: &DatabaseSmoSnapshot,
    committed_txns: u64,
) -> io::Result<()> {
    let mut file = csv_writer(
        &output.join("smo_counts.csv"),
        "phase,table,smo_type,completed,attempted,failed_or_retried,committed_txns,smo_per_million_commits",
    )?;
    let mut aggregate = SmoSnapshot::default();
    for (table, table_snapshot) in &snapshot.by_table {
        write_smo_rows(&mut file, phase, table, *table_snapshot, committed_txns)?;
        for i in 0..SmoKind::COUNT {
            aggregate.values[i] += table_snapshot.values[i];
        }
    }
    write_smo_rows(&mut file, phase, "__all__", aggregate, committed_txns)
}

pub fn write_human_summary(
    output: &Path,
    final_audit: &CheckpointReport,
    measured_smos: &DatabaseSmoSnapshot,
    committed_txns: u64,
) -> io::Result<()> {
    let completed: u64 = measured_smos
        .by_table
        .values()
        .map(|snapshot| {
            SmoKind::ALL
                .iter()
                .filter(|kind| kind.is_completed())
                .map(|kind| snapshot.get(*kind))
                .sum::<u64>()
        })
        .sum();
    let non_root_nodes = final_audit.all.nodes.saturating_sub(Table::ALL.len());
    let mut file = fs::File::create(output.join("experiment_summary.txt"))?;
    writeln!(file, "TPC-C node filling and SMO experiment")?;
    writeln!(file, "checkpoint: after_run")?;
    writeln!(file, "current nodes: {}", final_audit.all.nodes)?;
    writeln!(file, "current non-root nodes: {non_root_nodes}")?;
    writeln!(
        file,
        "strict weak-condition violations: {} ({:.4}% of non-root nodes)",
        final_audit.all.strict_weak_violations,
        if non_root_nodes == 0 {
            0.0
        } else {
            final_audit.all.strict_weak_violations as f64 * 100.0 / non_root_nodes as f64
        }
    )?;
    writeln!(
        file,
        "nodes exactly at weak boundary: {}",
        final_audit.all.weak_boundary_nodes
    )?;
    writeln!(
        file,
        "implementation repair-due nodes: {}",
        final_audit.all.repair_due_nodes
    )?;
    writeln!(
        file,
        "overflow-due nodes: {}",
        final_audit.all.overflow_due_nodes
    )?;
    writeln!(file, "completed measured-phase SMOs: {completed}")?;
    writeln!(file, "committed transaction units: {committed_txns}")?;
    writeln!(
        file,
        "SMOs per million committed transaction units: {:.3}",
        if committed_txns == 0 {
            0.0
        } else {
            completed as f64 * 1_000_000.0 / committed_txns as f64
        }
    )?;
    writeln!(file, "\nPer-table nonzero weak/maintenance counts:")?;
    for summary in &final_audit.per_table {
        if summary.strict_weak_violations > 0
            || summary.weak_boundary_nodes > 0
            || summary.repair_due_nodes > 0
            || summary.overflow_due_nodes > 0
        {
            writeln!(
                file,
                "{}: weak_violations={} boundary={} repair_due={} overflow_due={}",
                summary.table,
                summary.strict_weak_violations,
                summary.weak_boundary_nodes,
                summary.repair_due_nodes,
                summary.overflow_due_nodes
            )?;
        }
    }
    Ok(())
}

fn write_smo_rows(
    file: &mut fs::File,
    phase: &str,
    table: &str,
    snapshot: SmoSnapshot,
    committed_txns: u64,
) -> io::Result<()> {
    for kind in SmoKind::ALL {
        let count = snapshot.get(kind);
        let (completed, attempted, failed) = if kind.is_completed() {
            (count, 0, 0)
        } else if kind.as_str().ends_with("attempt") {
            (0, count, 0)
        } else {
            (0, 0, count)
        };
        let per_million = if kind.is_completed() && committed_txns > 0 {
            count as f64 * 1_000_000.0 / committed_txns as f64
        } else {
            0.0
        };
        writeln!(
            file,
            "{phase},{table},{},{completed},{attempted},{failed},{committed_txns},{per_million:.6}",
            kind.as_str()
        )?;
    }
    Ok(())
}

#[derive(Serialize)]
pub struct RunMetadata<'a> {
    pub schema_version: u32,
    pub command: &'a str,
    pub git_commit: String,
    pub git_dirty: Option<bool>,
    pub build_profile: &'a str,
    pub allocator: &'a str,
    pub logical_cpus: usize,
    pub completed_unix_seconds: u64,
    pub warehouses: u32,
    pub terminals: usize,
    pub warmup_seconds: u64,
    pub measured_seconds: u64,
    pub gc: bool,
    pub update_in_place: bool,
    pub idle_compaction: bool,
    pub root_star: String,
    pub big_tree_size: String,
    pub num_items: u32,
    pub customers_per_district: u32,
    pub initial_orders_per_district: u32,
    pub final_version: u64,
}

pub fn git_state() -> (String, Option<bool>) {
    let root = env!("CARGO_MANIFEST_DIR");
    let commit = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let dirty = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(root)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| !output.stdout.is_empty());
    (commit, dirty)
}

pub fn write_metadata(output: &Path, metadata: &RunMetadata<'_>) -> io::Result<()> {
    let file = fs::File::create(output.join("run_metadata.json"))?;
    serde_json::to_writer_pretty(file, metadata).map_err(io::Error::other)
}
