"""Regression checks for the one-command paper experiment orchestrator."""
import datetime as dt
import sys
import unittest
from pathlib import Path

import run_paper_experiments as runner


class PaperExperimentRunnerTests(unittest.TestCase):
    def test_full_command_uses_stable_per_hypothesis_output_root(self):
        collection = Path("/tmp/paper/run_20261007_120000")
        command = runner.experiment_command("h4", collection, quick=False, skip_build=True)
        self.assertEqual(command[:2], [sys.executable, str(runner.SCRIPTS_DIR / "h4.py")])
        self.assertIn(str(collection / "h4_results"), command)
        self.assertIn("--compact", command)
        self.assertIn("--skip-build", command)
        self.assertNotIn("--duration", command)

    def test_quick_command_applies_only_that_experiments_profile(self):
        collection = Path("/tmp/paper/run")
        command = runner.experiment_command("h3", collection, quick=True, skip_build=False)
        self.assertEqual(command[-len(runner.QUICK_ARGUMENTS["h3"]):], runner.QUICK_ARGUMENTS["h3"])
        self.assertNotIn("--skip-build", command)

    def test_collection_name_is_deterministic_for_a_given_time(self):
        now = dt.datetime(2026, 10, 7, 12, 34, 56)
        self.assertEqual(
            runner.default_collection_dir(Path("paper_results"), now),
            Path("paper_results/run_20261007_123456"),
        )

    def test_experiment_selection_rejects_unknown_names(self):
        self.assertEqual(runner.parse_experiments("h2,h2,H6"), ["h2", "h6"])
        with self.assertRaisesRegex(Exception, "unknown: h7"):
            runner.parse_experiments("h1,h7")

    def test_plotting_runs_after_the_collection(self):
        collection = Path("/tmp/paper/run")
        self.assertEqual(
            runner.plot_command(collection),
            [sys.executable, str(runner.SCRIPTS_DIR / "plot.py"), str(collection),
             "--kind", "hypotheses", "--compact"],
        )


if __name__ == "__main__":
    unittest.main()
