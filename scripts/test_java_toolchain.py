"""Regression tests for the JDK environment used to compile BenchBase."""
import os
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from engines import common


class JavaToolchainTests(unittest.TestCase):
    def test_jdk_21_env_replaces_an_old_java_home(self):
        old_home = Path("/opt/jdk-17")
        new_home = Path("/usr/lib/jvm/java-21-openjdk-test")

        def javac_version(cmd, **_kwargs):
            major = "17.0.1" if str(cmd[0]).startswith(str(old_home)) else "21.0.2"
            return subprocess.CompletedProcess(cmd, 0, "", f"javac {major}\n")

        with patch.object(common, "_jdk_candidates", return_value=[old_home, new_home]), \
             patch.object(subprocess, "run", side_effect=javac_version), \
             patch.dict(os.environ, {"JAVA_HOME": str(old_home), "PATH": "/usr/bin"}):
            env = common.jdk_21_env()

        self.assertEqual(env["JAVA_HOME"], str(new_home))
        self.assertEqual(env["PATH"].split(os.pathsep)[0], str(new_home / "bin"))

    def test_jdk_21_env_reports_checked_old_jdk(self):
        old_home = Path("/opt/jdk-17")
        completed = subprocess.CompletedProcess([], 0, "", "javac 17.0.1\n")
        with patch.object(common, "_jdk_candidates", return_value=[old_home]), \
             patch.object(subprocess, "run", return_value=completed):
            with self.assertRaisesRegex(RuntimeError, "openjdk-21-jdk-headless"):
                common.jdk_21_env()


if __name__ == "__main__":
    unittest.main()
