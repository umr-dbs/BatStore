"""Regression checks for exact TPC-C concurrency in comparison sweeps."""
import dataclasses
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import compare_engines
from engines import batstore, common


class TpccComparisonTests(unittest.TestCase):
    def test_fixed_population_supports_largest_affinity_point(self):
        scale = compare_engines.configure_tpcc_scale(
            common.Scale(), [2, 8, 128], ["batstore", "postgres"], ["tpcc"],
            ["on", "off"], None,
        )
        self.assertEqual(scale.tpcc_warehouses, 128)
        for threads in (2, 8, 128):
            point = dataclasses.replace(scale, tpcc_terminals=threads)
            self.assertEqual(point.tpcc_warehouses, 128)
            self.assertGreaterEqual(point.tpcc_warehouses, point.tpcc_terminals)

    def test_explicit_insufficient_population_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "must be >= 128"):
            compare_engines.configure_tpcc_scale(
                common.Scale(), [8, 128], ["batstore"], ["tpcc"], ["on"], 8,
            )

    def test_cross_warehouse_and_ycsb_do_not_require_exclusive_warehouses(self):
        for engines, workloads, affinity in (
            (["batstore"], ["tpcc"], ["off"]),
            (["postgres"], ["tpcc"], ["on", "off"]),
            (["batstore"], ["ycsb_a"], ["on", "off"]),
        ):
            scale = compare_engines.configure_tpcc_scale(
                common.Scale(), [2, 128], engines, workloads, affinity, 8,
            )
            self.assertEqual(scale.tpcc_warehouses, 8)

    def test_wrapper_rejects_old_binary_clamping_and_accepts_exact_count(self):
        for actual in (8, 124, 128, None):
            with self.subTest(actual=actual), tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp)
                def run(args, **kwargs):
                    self.assertEqual(args[2:6], ["128", "128", "60", "true"])
                    (out / "stdout.log").write_text(
                        f"- terminals (OLTP) = {actual}\n" if actual else "old output\n"
                    )
                    (out / "tpcc_oltp_timeseries.csv").write_text("new_order_committed\n600\n")
                    (out / "mem_stats.csv").write_text("rss_kb\n1024\n")
                    return 0, 0
                with patch.object(common, "fresh_scratch_dir", return_value=out), \
                     patch.object(common, "run_and_track_rss", side_effect=run), \
                     patch.object(common, "NO_DURABILITY", True):
                    result = batstore.run(
                        "tpcc", common.Scale(tpcc_warehouses=128, tpcc_terminals=128),
                        out, affinity="on",
                    )
                if actual == 128:
                    self.assertEqual(result.notes, "")
                    self.assertEqual(result.primary_metric_value, 10)
                else:
                    self.assertTrue(result.notes.startswith("FAILED: requested 128 terminals"))
                    self.assertEqual(result.primary_metric_value, 0)


if __name__ == "__main__":
    unittest.main()
