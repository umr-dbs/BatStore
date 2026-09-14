"""Fast regression tests for PostgreSQL setup/wrapper control paths."""
from __future__ import annotations

import io
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import setup_environment
from engines import common, hyrise, postgres_benchbase, umbra_benchbase


class PostgresWrapperTests(unittest.TestCase):
    def test_supported_benchbase_templates_are_valid_and_unlimited(self):
        values = {"database": "benchbase", "username": "admin", "password": "password"}
        configs = [
            postgres_benchbase.TPCC_CONFIG_TEMPLATE.format(
                warehouses=2, terminals=4, duration=10, **values,
            ),
            postgres_benchbase.CHBENCHMARK_CONFIG_TEMPLATE.format(
                warehouses=2, terminals=4, olap_threads=1, duration=10,
                ch_weights=postgres_benchbase.CHBENCHMARK_WEIGHTS["htap_q1"], **values,
            ),
            postgres_benchbase.YCSB_CONFIG_TEMPLATE.format(
                scalefactor=10, theta=0.99, field_size=100, terminals=4, duration=10,
                weights=postgres_benchbase.YCSB_WEIGHTS["a"], **values,
            ),
        ]
        for config in configs:
            with self.subTest(benchmark=ET.fromstring(config).findtext("url")):
                root = ET.fromstring(config)
                rates = [node.text.strip() for node in root.findall(".//rate")]
                self.assertTrue(rates)
                self.assertEqual(set(rates), {"unlimited"})

    def test_find_postmaster_uses_backend_parent_not_oldest_process(self):
        # stat fields after the closing ')' begin with: state, ppid, ...
        proc = MagicMock()
        proc.stdin = io.StringIO()
        proc.stdout = io.StringIO("321\n")
        with patch.object(subprocess, "Popen", return_value=proc), \
             patch.object(Path, "read_text", return_value="321 (postgres: admin db) S 123 0 0\n"):
            self.assertEqual(postgres_benchbase._find_postmaster_pid(), 123)
        self.assertEqual(proc.stdin.getvalue(), "SELECT pg_backend_pid();\n")
        proc.terminate.assert_called_once()

    def test_s_htap_is_explicitly_skipped_before_touching_postgres(self):
        scale = common.Scale(ycsb_threads=8, s_htap_duration=17)
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(postgres_benchbase, "_verify_postmaster_numa_binding") as verify:
            result = postgres_benchbase.run("s_htap", scale, Path(tmp))
        verify.assert_not_called()
        self.assertEqual(result.duration_secs, 17)
        self.assertEqual(result.threads, 8)
        self.assertIn("no s_htap plugin", result.notes)

    def test_umbra_s_htap_is_skipped_before_starting_container(self):
        scale = common.Scale(ycsb_threads=4)
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(umbra_benchbase, "_start_container") as start:
            result = umbra_benchbase.run("s_htap", scale, Path(tmp))
        start.assert_not_called()
        self.assertIn("no s_htap plugin", result.notes)

    def test_hyrise_s_htap_is_skipped_before_starting_server(self):
        scale = common.Scale(ycsb_threads=4)
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(hyrise, "_start_server") as start:
            result = hyrise.run("s_htap", scale, Path(tmp))
        start.assert_not_called()
        self.assertIn("no s_htap plugin", result.notes)

    def test_connection_values_escape_xml_and_url_components(self):
        with patch.object(common, "PG_DATABASE", "db/name"), \
             patch.object(common, "PG_ROLE", "a&b"), \
             patch.object(common, "PG_PASSWORD", "x<y"):
            values = postgres_benchbase._template_connection_values()
        self.assertEqual(values["database"], "db%2Fname")
        self.assertEqual(values["username"], "a&amp;b")
        self.assertEqual(values["password"], "x&lt;y")


class PostgresSetupTests(unittest.TestCase):
    def test_pg_cluster_selects_port_5432_not_first_line(self):
        listing = "15 old 5433 online postgres /data/old /log/old\n16 main 5432 down postgres /data/main /log/main\n"
        completed = subprocess.CompletedProcess(["pg_lsclusters"], 0, listing, "")
        with patch.object(subprocess, "run", return_value=completed):
            self.assertEqual(setup_environment._pg_cluster()[:3], ["16", "main", "5432"])
            self.assertEqual(setup_environment._pg_data_directory(), Path("/data/main"))

    def test_sql_quoting_handles_custom_role_and_password(self):
        self.assertEqual(setup_environment._postgres_identifier('a"b'), '"a""b"')
        self.assertEqual(setup_environment._postgres_literal("p'ass"), "'p''ass'")

    def test_postgres_owned_file_check_runs_as_postgres(self):
        path = Path("/restricted/postgresql_data/PG_VERSION")
        completed = subprocess.CompletedProcess([], 0)
        with patch.object(subprocess, "run", return_value=completed) as run:
            self.assertTrue(setup_environment._postgres_file_exists(path))
        run.assert_called_once_with(
            ["sudo", "-u", "postgres", "test", "-f", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def test_postgres_owned_file_check_reports_missing(self):
        completed = subprocess.CompletedProcess([], 1)
        with patch.object(subprocess, "run", return_value=completed):
            self.assertFalse(
                setup_environment._postgres_file_exists(Path("/restricted/PG_VERSION"))
            )


class SetupModeTests(unittest.TestCase):
    STEP_FUNCTIONS = (
        "step_apt_packages", "step_wiredtiger", "step_leanstore", "step_hyrise",
        "step_vweaver_hugepages", "step_vweaver_ermia", "step_vweaver_ermia_frugal",
        "step_postgres", "step_benchbase", "step_umbra", "step_batstore",
        "step_python_venv", "step_postgres_tmpfs", "step_fresh_checkouts",
    )

    def run_setup(self, full: bool):
        argv = ["setup_environment.py", "--skip-postgres-tmpfs"]
        if full:
            argv.append("--full")
        with ExitStack() as stack:
            calls = {
                name: stack.enter_context(patch.object(setup_environment, name))
                for name in self.STEP_FUNCTIONS
            }
            stack.enter_context(patch.object(sys, "argv", argv))
            stack.enter_context(patch("builtins.print"))
            setup_environment.main()
        return calls

    def test_default_setup_excludes_optional_engines(self):
        calls = self.run_setup(full=False)
        for name in ("step_wiredtiger", "step_leanstore", "step_postgres",
                     "step_benchbase", "step_batstore"):
            calls[name].assert_called_once()
        for name in ("step_hyrise", "step_vweaver_hugepages", "step_vweaver_ermia",
                     "step_vweaver_ermia_frugal", "step_umbra"):
            calls[name].assert_not_called()
        calls["step_apt_packages"].assert_called_once_with(False)
        calls["step_fresh_checkouts"].assert_called_once_with(False)

    def test_full_setup_includes_optional_engines(self):
        calls = self.run_setup(full=True)
        for name in ("step_hyrise", "step_vweaver_hugepages", "step_vweaver_ermia",
                     "step_vweaver_ermia_frugal", "step_umbra"):
            calls[name].assert_called_once()
        calls["step_apt_packages"].assert_called_once_with(True)
        calls["step_fresh_checkouts"].assert_called_once_with(True)


if __name__ == "__main__":
    unittest.main()
