"""Regression tests for TPC-C tree-stat plot discovery and rendering."""

from pathlib import Path
import tempfile
import unittest

import pandas as pd

import plot_tree_stats


class TreeStatsPlotTests(unittest.TestCase):
    def _write_run(self, run_dir: Path) -> None:
        run_dir.mkdir(parents=True)
        rows = []
        for checkpoint, dead in (("after_load", 1), ("after_run", 20)):
            for table in ("customer", "__all__"):
                rows.append({
                    "checkpoint": checkpoint, "table": table, "height": 2,
                    "nodes": 11, "internal_nodes": 1, "leaf_nodes": 10,
                    "live_entries": 500, "dead_entries": dead,
                    "logical_fill_mean": .5, "logical_fill_p05": .4,
                    "logical_fill_p50": .5, "logical_fill_p95": .7,
                    "physical_fill_mean": .5 + dead / 1000,
                    "strict_weak_violations": 0, "weak_boundary_nodes": 1,
                    "repair_due_nodes": 1, "overflow_due_nodes": 2,
                    "root_collapse_due": False,
                })
        pd.DataFrame(rows).to_csv(run_dir / "tree_summary.csv", index=False)
        pd.DataFrame({
            "elapsed_sec": [0, 1], "rss_kb": [1024, 2048],
            "jemalloc_resident_bytes": [1024, 2048],
        }).to_csv(run_dir / "mem_stats.csv", index=False)
        pd.DataFrame({
            "checkpoint": ["after_load", "after_run", "after_run"],
            "table": ["customer", "customer", "customer"],
            "logical_fill": [.4, .5, .7],
            "physical_fill": [.5, .8, 1.0],
        }).to_csv(run_dir / "node_filling.csv", index=False)

    def test_finds_nested_run_and_writes_figures(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "experiments" / "run_1"
            self._write_run(run_dir)
            self.assertEqual(plot_tree_stats.find_runs(root), [run_dir])
            self.assertTrue(plot_tree_stats.is_stats_input(root))
            plot_tree_stats.plot_all(root)
            self.assertEqual(
                {path.name for path in (run_dir / "plots").glob("*.png")},
                {
                    "tree_stats_checkpoints.png", "tree_stats_by_table.png",
                    "tree_stats_fill_distribution.png", "tree_stats_memory.png",
                },
            )


if __name__ == "__main__":
    unittest.main()
