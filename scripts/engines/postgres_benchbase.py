"""PostgreSQL engine wrapper - drives BenchBase (github.com/cmu-db/benchbase),
the same JDBC-based tool the paper itself used for its PostgreSQL comparison
(LeanStore's own repo has no working Postgres integration - see the plan doc).

Requires: local PostgreSQL server, an `admin`/`password` superuser role, and a
`benchbase` database (see the plan's setup notes) - `--create=true` recreates
the benchmark's own tables against that database on every run.

Note: peak_rss_mb is the legacy manifest column name. For PostgreSQL it reports the
SERVER cgroup's total charged memory during the measured execution phase (anonymous
memory, shared memory, filesystem cache, and tmpfs), excluding the JDBC client. On hosts
without cgroup-v2 accounting it falls back to summed RSS for the postmaster process tree.
The source and peak memory.stat breakdown are preserved in memory_stats.json.

NUMA-pinned: setup_environment.py constrains the actual PostgreSQL cluster service's
cgroup to node 0's CPUs and memory nodes. This wrapper verifies the live postmaster's
effective masks before every run; the JDBC client is independently pinned by
common.run_and_track_rss.
"""
from __future__ import annotations

import atexit
import json
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
from urllib.parse import quote
from xml.sax.saxutils import escape

from . import common

BENCHBASE_HOME = common.BENCHBASE_HOME
BENCHBASE_JAR = BENCHBASE_HOME / "benchbase.jar"

def ensure_built() -> None:
    repo = BENCHBASE_HOME.parent.parent
    if not repo.is_dir():
        raise SystemExit(
            f"BenchBase repository not found at {repo.resolve(strict=False)}. "
            f"Set WORKSPACE_ROOT to the directory containing the benchbase checkout "
            f"(current WORKSPACE_ROOT={common.WORKSPACE_ROOT.resolve(strict=False)}), or pass "
            f"--workspace-root to scripts/run_s_ycsb_sweep.py."
        )
    subprocess.run(["mvn", "package", "-P", "postgres", "-DskipTests",
                    "-Dmaven.compiler.release=21"], cwd=repo, check=True)
    tgz = repo / "target" / "benchbase-postgres.tgz"
    subprocess.run(["tar", "xzf", str(tgz), "-C", str(repo / "target")], check=True)

# Postgres has no literal "GC" flag; autovacuum (which cleans up dead/old row versions) is
# the closest real, standard analog, toggled without a restart via ALTER SYSTEM + reload.
SUPPORTS_GC_TOGGLE = True
_AUTOVACUUM_DISABLED_BY_HARNESS = False

# BenchBase's own transaction-type order for YCSB (config/postgres/sample_ycsb_config.xml):
# ReadRecord, InsertRecord, ScanRecord, UpdateRecord, DeleteRecord, ReadModifyWriteRecord.
# D approximates YCSB's "latest" distribution with BenchBase's zipfian skewFactor (no separate
# recency-biased generator available) - the same documented approximation as the LeanStore side.
YCSB_WEIGHTS = {
    "a": "50,0,0,50,0,0",
    "b": "95,0,0,5,0,0",
    "c": "100,0,0,0,0,0",
    "d": "95,5,0,0,0,0",
    "e": "0,5,95,0,0,0",
    "f": "50,0,0,0,0,50",
}

TPCC_CONFIG_TEMPLATE = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:5432/{database}?sslmode=disable&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true</url>
    <username>{username}</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_REPEATABLE_READ</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <!-- Measure saturation throughput; a numeric rate is a global BenchBase
                 client-side request throttle and would flatten every thread sweep. -->
            <rate>unlimited</rate>
            <weights>45,43,4,4,4</weights>
        </work>
    </works>
    <transactiontypes>
        <transactiontype><name>NewOrder</name></transactiontype>
        <transactiontype><name>Payment</name></transactiontype>
        <transactiontype><name>OrderStatus</name></transactiontype>
        <transactiontype><name>Delivery</name></transactiontype>
        <transactiontype><name>StockLevel</name></transactiontype>
    </transactiontypes>
</parameters>
"""

# BenchBase's chbenchmark plugin mixes a TPC-C work phase with the standard CH-benCHmark
# Q1-Q22 analytical queries (real SQL - see src/main/java/.../chbenchmark/queries/Q*.java);
# here only Q1/Q6 ever get nonzero weight, matching HTAP_WORKLOADS (common.py) and the
# LeanStore/WiredTiger/BatStore side of this same restriction.
CHBENCHMARK_WEIGHTS = {
    "htap_q1": "100," + ",".join(["0"] * 21),
    "htap_q6": ",".join(["0"] * 5) + ",100," + ",".join(["0"] * 16),
    # PostgreSQL already used BenchBase's canonical SQL. Its retained variant
    # aliases therefore intentionally execute the same Q1/Q6.
    "htap_q1_variant": "100," + ",".join(["0"] * 21),
    "htap_q6_variant": ",".join(["0"] * 5) + ",100," + ",".join(["0"] * 16),
}

CHBENCHMARK_CONFIG_TEMPLATE = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:5432/{database}?sslmode=disable&amp;ApplicationName=chbenchmark&amp;reWriteBatchedInserts=true</url>
    <username>{username}</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_SERIALIZABLE</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <!-- Default (no @bench) applies to tpcc; chbenchmark gets its own dedicated analytics
         terminal pool (default 1), matching the "N OLTP threads + M OLAP threads"
         convention used for BatStore/LeanStore/WiredTiger's htap_q1/htap_q6 (see their
         engines/*.py) - sized from scale.htap_olap_threads. -->
    <terminals>{terminals}</terminals>
    <terminals bench="chbenchmark">{olap_threads}</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <!-- Neither the OLTP nor analytical client should impose a throughput cap. -->
            <rate>unlimited</rate>
            <rate bench="chbenchmark">unlimited</rate>
            <weights bench="tpcc">45,43,4,4,4</weights>
            <weights bench="chbenchmark">{ch_weights}</weights>
        </work>
    </works>
    <transactiontypes bench="chbenchmark">
        <transactiontype><name>Q1</name></transactiontype>
        <transactiontype><name>Q2</name></transactiontype>
        <transactiontype><name>Q3</name></transactiontype>
        <transactiontype><name>Q4</name></transactiontype>
        <transactiontype><name>Q5</name></transactiontype>
        <transactiontype><name>Q6</name></transactiontype>
        <transactiontype><name>Q7</name></transactiontype>
        <transactiontype><name>Q8</name></transactiontype>
        <transactiontype><name>Q9</name></transactiontype>
        <transactiontype><name>Q10</name></transactiontype>
        <transactiontype><name>Q11</name></transactiontype>
        <transactiontype><name>Q12</name></transactiontype>
        <transactiontype><name>Q13</name></transactiontype>
        <transactiontype><name>Q14</name></transactiontype>
        <transactiontype><name>Q15</name></transactiontype>
        <transactiontype><name>Q16</name></transactiontype>
        <transactiontype><name>Q17</name></transactiontype>
        <transactiontype><name>Q18</name></transactiontype>
        <transactiontype><name>Q19</name></transactiontype>
        <transactiontype><name>Q20</name></transactiontype>
        <transactiontype><name>Q21</name></transactiontype>
        <transactiontype><name>Q22</name></transactiontype>
    </transactiontypes>
    <transactiontypes bench="tpcc">
        <transactiontype><name>NewOrder</name></transactiontype>
        <transactiontype><name>Payment</name></transactiontype>
        <transactiontype><name>OrderStatus</name></transactiontype>
        <transactiontype><name>Delivery</name></transactiontype>
        <transactiontype><name>StockLevel</name></transactiontype>
    </transactiontypes>
</parameters>
"""

YCSB_CONFIG_TEMPLATE = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:5432/{database}?sslmode=disable&amp;ApplicationName=ycsb&amp;reWriteBatchedInserts=true</url>
    <username>{username}</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_REPEATABLE_READ</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{scalefactor}</scalefactor>
    <skewFactor>{theta}</skewFactor>
    <fieldSize>{field_size}</fieldSize>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <!-- Measure saturation throughput rather than BenchBase's sample-config
                 default ceiling of 10,000 requests/second. -->
            <rate>unlimited</rate>
            <weights>{weights}</weights>
        </work>
    </works>
    <transactiontypes>
        <transactiontype><name>ReadRecord</name></transactiontype>
        <transactiontype><name>InsertRecord</name></transactiontype>
        <transactiontype><name>ScanRecord</name></transactiontype>
        <transactiontype><name>UpdateRecord</name></transactiontype>
        <transactiontype><name>DeleteRecord</name></transactiontype>
        <transactiontype><name>ReadModifyWriteRecord</name></transactiontype>
    </transactiontypes>
</parameters>
"""


SHTAP_CONFIG_TEMPLATE = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:5432/{database}?sslmode=disable&amp;ApplicationName=s_htap&amp;reWriteBatchedInserts=true</url>
    <username>{username}</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_REPEATABLE_READ</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{scalefactor}</scalefactor>
    <hotWindow>{hot_window}</hotWindow>
    <hotTheta>{hot_theta}</hotTheta>
    <arrivalRatio>{arrival_ratio}</arrivalRatio>
    <maxLateness>{max_lateness}</maxLateness>
    <olapThreads>{olap_threads}</olapThreads>
    <olapLag>{olap_lag}</olapLag>
    <olapSpan>{olap_span}</olapSpan>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <!-- Measure saturation throughput; a numeric rate is a global BenchBase
                 client-side request throttle and would flatten every thread sweep. -->
            <rate>unlimited</rate>
            <!-- These weights only matter for BenchBase's own bookkeeping (its per-
                 transaction-type latency CSVs bucket by whichever type its global weighted
                 dispatch assigns) - which SQL actually runs is decided by SHTAPWorker itself
                 from its fixed write/OLAP terminal-id role (see that class's doc), not by
                 this distribution. Proportioned to the write/OLAP terminal split (and,
                 within the write share, to arrivalRatio) purely to keep that bookkeeping
                 close to reality. -->
            <weights>{weights}</weights>
        </work>
    </works>
    <transactiontypes>
        <transactiontype><name>ArrivalUpsert</name></transactiontype>
        <transactiontype><name>HotTailUpdate</name></transactiontype>
        <transactiontype><name>OlapScan</name></transactiontype>
    </transactiontypes>
</parameters>
"""


def _find_postmaster_pid() -> Optional[int]:
    """Return the postmaster serving the configured BenchBase database.

    A host may run several PostgreSQL clusters.  Searching the process table and taking
    the oldest postmaster can therefore select a different cluster from the one reached
    by the JDBC URL.  A normal client backend is a direct postmaster child, so ask the
    target server for that backend PID and read its PPID from procfs instead.
    """
    env = os.environ.copy()
    env["PGPASSWORD"] = common.PG_PASSWORD
    proc = subprocess.Popen(
        ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", common.PG_ROLE,
         "-h", "localhost", "-d", common.PG_DATABASE],
        env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdin is not None and proc.stdout is not None
        # Keep psql waiting for its next command after it prints the PID.  That keeps the
        # corresponding backend alive while its procfs parent is inspected.
        proc.stdin.write("SELECT pg_backend_pid();\n")
        proc.stdin.flush()
        backend_pid = int(proc.stdout.readline().strip())
        stat = Path(f"/proc/{backend_pid}/stat").read_text()
        # /proc/<pid>/stat's comm field is parenthesized and may contain spaces.
        return int(stat.rsplit(")", 1)[1].split()[1])
    except (BrokenPipeError, OSError, ValueError, IndexError):
        return None
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def _verify_postmaster_numa_binding() -> int:
    """Return the postmaster PID, refusing a comparison unless its live effective CPU
    and memory-node masks are exactly the node used for every embedded engine."""
    pid = _find_postmaster_pid()
    if pid is None:
        sys.exit("cannot locate the PostgreSQL postmaster; is the cluster running?")
    status: dict[str, str] = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            status[key] = value.strip()
    expected_cpus = common.expand_cpu_list(common.numa_node_cpu_list())
    actual_cpus = common.expand_cpu_list(status.get("Cpus_allowed_list", ""))
    actual_nodes = common.expand_cpu_list(status.get("Mems_allowed_list", ""))
    if actual_cpus != expected_cpus or actual_nodes != {common.NUMA_NODE}:
        sys.exit(
            f"PostgreSQL postmaster PID {pid} is not pinned to NUMA node {common.NUMA_NODE}: "
            f"Cpus_allowed_list={status.get('Cpus_allowed_list')!r}, "
            f"Mems_allowed_list={status.get('Mems_allowed_list')!r}; expected CPUs "
            f"{common.numa_node_cpu_list()!r}, memory node {common.NUMA_NODE}. Run "
            f"`python3 scripts/setup_environment.py --reuse-checkouts` to configure the "
            f"cluster service cgroup."
        )
    return pid


def _template_connection_values() -> dict[str, str]:
    """BenchBase XML values matching common's environment-overridable connection.

    The database is a JDBC URL path component; credentials are XML text. Keeping these
    transformations here avoids both malformed generated configs and silently connecting
    as the old hardcoded admin/password user when PG_* overrides are set.
    """
    return {
        "database": quote(common.PG_DATABASE, safe=""),
        "username": escape(common.PG_ROLE),
        "password": escape(common.PG_PASSWORD),
    }


def _assert_unlimited_config(config_path: Path) -> None:
    """Refuse to execute a stale/generated BenchBase config with a numeric rate cap."""
    rates = [((node.text or "").strip(), node.attrib.get("bench", "default"))
             for node in ET.parse(config_path).getroot().findall(".//rate")]
    capped = [(rate, bench) for rate, bench in rates if rate != "unlimited"]
    if not rates or capped:
        raise RuntimeError(
            f"generated BenchBase config is not unlimited-rate: {capped or rates}; "
            f"config={config_path}. Ensure the server is running this updated "
            f"{Path(__file__).resolve()} rather than an older checkout."
        )


def _set_autovacuum(enabled: bool) -> None:
    """ALTER SYSTEM + reload takes effect immediately, no server restart needed. Runs
    directly (not through common.run_and_track_rss) - this is a tiny admin statement, not
    part of the measured workload, and shouldn't be numactl-wrapped or RSS-sampled."""
    global _AUTOVACUUM_DISABLED_BY_HARNESS
    env = os.environ.copy()
    env["PGPASSWORD"] = common.PG_PASSWORD
    value = "on" if enabled else "off"
    subprocess.run(
        ["psql", "-U", common.PG_ROLE, "-h", "localhost", "-d", common.PG_DATABASE,
         "-v", "ON_ERROR_STOP=1",
         "-c", f"ALTER SYSTEM SET autovacuum = {value};",
         "-c", "SELECT pg_reload_conf();",
         *([] if enabled else [
             "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                   "WHERE backend_type = 'autovacuum worker';",
         ])],
        env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    _wait_for_setting("autovacuum", value)
    _AUTOVACUUM_DISABLED_BY_HARNESS = not enabled
    if not enabled:
        deadline = time.monotonic() + 5.0
        while int(_psql_scalar(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE backend_type = 'autovacuum worker';"
        )):
            if time.monotonic() >= deadline:
                raise RuntimeError("autovacuum is off but an existing worker did not stop")
            time.sleep(0.05)


def _restore_autovacuum_at_exit() -> None:
    """Do not leave the benchmark cluster with persistent autovacuum=off."""
    if not _AUTOVACUUM_DISABLED_BY_HARNESS:
        return
    try:
        _set_autovacuum(True)
    except Exception as exc:  # pragma: no cover - best-effort interpreter shutdown path
        print(f"WARNING: failed to restore PostgreSQL autovacuum=on: {exc}", file=sys.stderr)


atexit.register(_restore_autovacuum_at_exit)


def _psql_scalar(sql: str) -> str:
    env = os.environ.copy()
    env["PGPASSWORD"] = common.PG_PASSWORD
    result = subprocess.run(
        ["psql", "-U", common.PG_ROLE, "-h", "localhost", "-d", common.PG_DATABASE, "-tAc", sql],
        env=env, capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def _wait_for_setting(name: str, expected: str, timeout: float = 5.0) -> None:
    """Wait for a SIGHUP-reloaded PostgreSQL setting to become visible."""
    deadline = time.monotonic() + timeout
    while True:
        actual = _psql_scalar(f"SHOW {name};")
        if actual == expected:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"failed to set {name}={expected}; live value is {actual!r}"
            )
        time.sleep(0.05)


def _verify_benchmark_configuration(
    postmaster_pid: int, autovacuum_enabled: bool,
) -> dict:
    """Fail before a long sweep if setup's PostgreSQL profile is not active."""
    budget_bytes = int(common.POSTGRES_MEMORY_BUDGET_GIB * 1024 ** 3)
    expected_shared_bytes = max(
        1, round(common.POSTGRES_MEMORY_BUDGET_GIB * 0.25),
    ) * 1024 ** 3
    expected_cache_bytes = max(
        1, round(common.POSTGRES_MEMORY_BUDGET_GIB * 0.75),
    ) * 1024 ** 3
    snapshot = {
        "memory_budget_gib": common.POSTGRES_MEMORY_BUDGET_GIB,
        "shared_buffers_bytes": int(_psql_scalar(
            "SELECT pg_size_bytes(current_setting('shared_buffers'));",
        )),
        "effective_cache_size_bytes": int(_psql_scalar(
            "SELECT pg_size_bytes(current_setting('effective_cache_size'));",
        )),
        "work_mem_bytes": int(_psql_scalar(
            "SELECT pg_size_bytes(current_setting('work_mem'));",
        )),
        "max_connections": int(_psql_scalar("SHOW max_connections;")),
        "jit": _psql_scalar("SHOW jit;"),
        "autovacuum": _psql_scalar("SHOW autovacuum;"),
        "fsync": _psql_scalar("SHOW fsync;"),
        "synchronous_commit": _psql_scalar("SHOW synchronous_commit;"),
        "full_page_writes": _psql_scalar("SHOW full_page_writes;"),
        "random_page_cost": float(_psql_scalar("SHOW random_page_cost;")),
        "checkpoint_timeout_seconds": int(_psql_scalar(
            "SELECT setting FROM pg_settings WHERE name='checkpoint_timeout';",
        )),
        "max_wal_size_bytes": int(_psql_scalar(
            "SELECT pg_size_bytes(current_setting('max_wal_size'));",
        )),
    }
    errors = []
    if snapshot["shared_buffers_bytes"] != expected_shared_bytes:
        errors.append(
            f"shared_buffers={snapshot['shared_buffers_bytes']} bytes "
            f"(expected {expected_shared_bytes})"
        )
    if snapshot["effective_cache_size_bytes"] != expected_cache_bytes:
        errors.append(
            f"effective_cache_size={snapshot['effective_cache_size_bytes']} bytes "
            f"(expected {expected_cache_bytes})"
        )
    if snapshot["work_mem_bytes"] != 4 * 1024 ** 2:
        errors.append(f"work_mem={snapshot['work_mem_bytes']} bytes (expected 4MiB)")
    if snapshot["max_connections"] < 160:
        errors.append(f"max_connections={snapshot['max_connections']} (expected at least 160)")
    if snapshot["jit"] != "off":
        errors.append(f"jit={snapshot['jit']} (expected off)")
    expected_autovacuum = "on" if autovacuum_enabled else "off"
    if snapshot["autovacuum"] != expected_autovacuum:
        errors.append(
            f"autovacuum={snapshot['autovacuum']} (expected {expected_autovacuum})"
        )
    for setting in ("fsync", "synchronous_commit", "full_page_writes"):
        if snapshot[setting] != "off":
            errors.append(f"{setting}={snapshot[setting]} (expected off)")
    if abs(snapshot["random_page_cost"] - 1.1) > 1e-9:
        errors.append(
            f"random_page_cost={snapshot['random_page_cost']} (expected 1.1)"
        )
    if snapshot["checkpoint_timeout_seconds"] != 30 * 60:
        errors.append(
            f"checkpoint_timeout={snapshot['checkpoint_timeout_seconds']}s (expected 1800s)"
        )
    if snapshot["max_wal_size_bytes"] != 16 * 1024 ** 3:
        errors.append(
            f"max_wal_size={snapshot['max_wal_size_bytes']} bytes (expected 16GiB)"
        )

    cgroup_dir = common._cgroup_v2_dir_for_pid(postmaster_pid)
    if cgroup_dir is not None:
        snapshot["cgroup"] = str(cgroup_dir)
        for filename, expected in (("memory.max", budget_bytes), ("memory.swap.max", 0)):
            try:
                raw_value = (cgroup_dir / filename).read_text().strip()
                actual = None if raw_value == "max" else int(raw_value)
            except (OSError, ValueError):
                actual = None
            snapshot[filename] = actual
            if actual != expected:
                errors.append(f"{filename}={actual!r} (expected {expected})")

    if errors:
        sys.exit(
            "PostgreSQL benchmark tuning is not active:\n  - "
            + "\n  - ".join(errors)
            + "\nRe-run `python3 scripts/setup_environment.py --postgres-only` "
              "before benchmarking."
        )
    return snapshot


def _verify_tmpfs_datadir() -> None:
    """Every other engine's on-disk data lives under common.fresh_scratch_dir, which fails
    loudly if SCRATCH_ROOT isn't tmpfs. PostgreSQL is a pre-existing system service this
    harness doesn't spawn, so it can't go through fresh_scratch_dir - its in-memory-only
    guarantee instead depends on setup_environment.py's default PostgreSQL tmpfs step
    having been run (and tmpfs surviving since, which a reboot would undo). Without an
    equivalent check here, a stale/never-run tmpfs setup would silently benchmark against
    real disk while every other engine's numbers are RAM-only - so ask the live server for
    its actual data_directory and check its mount, same "fail loudly, not silently" rule as
    common.fresh_scratch_dir.
    """
    data_dir = Path(_psql_scalar("SHOW data_directory;"))
    fstype = common._mount_fstype(data_dir)
    if fstype != "tmpfs":
        sys.exit(
            f"PostgreSQL's data_directory ({data_dir}) is not tmpfs-backed (fstype={fstype!r}) - "
            f"refusing to run, since every other engine in this harness is guaranteed "
            f"in-memory-only (see common.fresh_scratch_dir). Run "
            f"`python3 scripts/setup_environment.py --postgres-only` first (tmpfs doesn't "
            f"survive a reboot, so this can go stale)."
        )


def _set_unsafe_durability() -> None:
    """Matches every other engine's benchmark-only durability posture (see the libmdbx
    UtterlyNoSync/WriteMap fix and LeanStore's wal_pwrite=false/wal_fsync=false): none of
    these settings should be reintroduced accidentally by whatever the OS package's default
    postgresql.conf happens to ship. `synchronous_commit=off` stops commits blocking on WAL
    flush, `fsync=off` stops WAL/data file writes from calling fsync at all, and
    `full_page_writes=off` skips the extra page image written on first modification after a
    checkpoint - all three are PGC_SIGHUP (take effect on pg_reload_conf(), no restart
    needed), same mechanism as the existing autovacuum toggle below. Safe here because
    PGDATA is tmpfs-backed (_verify_tmpfs_datadir already refused to run otherwise) - or,
    under common.NO_DURABILITY, because these settings are themselves what makes real
    disk backing safe to use instead - and compare_engines recreates the benchmark
    tables for every point - never do this against a real database.
    """
    env = os.environ.copy()
    env["PGPASSWORD"] = common.PG_PASSWORD
    subprocess.run(
        ["psql", "-U", common.PG_ROLE, "-h", "localhost", "-d", common.PG_DATABASE,
         "-v", "ON_ERROR_STOP=1",
         "-c", "ALTER SYSTEM SET synchronous_commit = off;",
         "-c", "ALTER SYSTEM SET fsync = off;",
         "-c", "ALTER SYSTEM SET full_page_writes = off;",
         "-c", "SELECT pg_reload_conf();"],
        env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    for setting in ("fsync", "synchronous_commit", "full_page_writes"):
        _wait_for_setting(setting, "off")


def _prepare_measured_execution() -> None:
    """Finish load-time maintenance before throughput/memory measurement starts."""
    env = os.environ.copy()
    env["PGPASSWORD"] = common.PG_PASSWORD
    subprocess.run(
        ["psql", "-U", common.PG_ROLE, "-h", "localhost", "-d", common.PG_DATABASE,
         "-v", "ON_ERROR_STOP=1", "-c", "ANALYZE;", "-c", "CHECKPOINT;"],
        env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )


def _start_server_memory_sampler(postmaster_pid: int):
    sampler = common.start_cgroup_memory_sampler(postmaster_pid)
    if sampler is not None:
        return sampler
    sampler = common.start_process_tree_sampler(postmaster_pid)
    sampler[2]["source"] = "summed_process_tree_rss"
    return sampler


def _finish_server_memory_sampler(sampler, output_dir: Path) -> tuple[float, str]:
    stop, thread, peak_box = sampler
    stop.set()
    thread.join()
    metadata = {
        "source": peak_box.get("source", "unknown"),
        "peak_memory_mb": peak_box["mb"],
        "current_memory_mb_at_last_sample": peak_box.get("current_mb"),
        "cgroup": peak_box.get("cgroup"),
        "memory_stat_bytes_at_peak": peak_box.get("stat", {}),
    }
    (output_dir / "memory_stats.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return peak_box["mb"], metadata["source"]


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "on", reload: bool = True,
        ycsb_payload: str = "standard", read_payload: bool = True) -> common.NormalizedResult:
    """`reload=False` can skip BenchBase's expensive --create/--load steps for direct
    callers. compare_engines.py deliberately passes reload=True for every PostgreSQL point
    so every measurement starts from freshly created and loaded benchmark tables.
    """
    # The pinned BenchBase distribution has no S-YCSB plugin.  The native engines have
    # dedicated implementations, but sending `-b s_htap` to BenchBase only produces
    # "Plugin s_htap is undefined" after touching server state.  Make that limitation an
    # explicit skipped point until a real BenchBase plugin is added and built.
    if workload == "s_htap":
        return common.NormalizedResult(
            "postgres", workload, scale.label, scale.s_htap_duration,
            "write_ops_per_sec", 0.0, 0.0, threads=scale.ycsb_threads,
            gc_enabled=gc,
            notes="SKIPPED: the pinned BenchBase build has no s_htap plugin",
            memory_source="not_measured",
        )

    # patches/ycsb_skew_factor_benchbase.patch (applied by setup_environment.py's
    # step_benchbase) makes BenchBase's YCSBBenchmark/YCSBWorker treat skewFactor<=0 as a
    # genuine "uniform" sentinel - YCSBWorker builds a real UniformGenerator for read-key
    # selection in that case, not an approximation - and relaxes the upstream `>=1`
    # rejection so theta>1 (e.g. 1.4) works too. Only skewFactor==1.0 is still rejected:
    # that's ZipfianGenerator's actual singularity (alpha = 1/(1-theta)), with no
    # reasonable substitute, so it's skipped outright rather than attempted.
    ycsb_theta = scale.ycsb_theta
    if workload in common.YCSB_WORKLOADS and ycsb_theta == 1.0:
        return common.NormalizedResult(
            "postgres", workload, scale.label, scale.ycsb_duration, "ops_per_sec", 0.0, 0.0,
            threads=scale.ycsb_threads, gc_enabled=gc,
            notes="SKIPPED: BenchBase's YCSB module rejects skewFactor==1 (the "
                  "ZipfianGenerator singularity - no reasonable substitute)",
        )
    postmaster_pid = _verify_postmaster_numa_binding()
    # Under common.NO_DURABILITY (see compare_engines_new.py), the tmpfs guarantee below
    # is redundant: _set_unsafe_durability()'s fsync=off already means PostgreSQL never
    # blocks on a real disk write either way, so skip requiring tmpfs specifically for it.
    if not common.NO_DURABILITY:
        _verify_tmpfs_datadir()
    _set_unsafe_durability()
    _set_autovacuum(gc != "off")

    output_dir.mkdir(parents=True, exist_ok=True)
    server_config = _verify_benchmark_configuration(postmaster_pid, gc != "off")
    server_config["gc_enabled"] = gc
    (output_dir / "server_config.json").write_text(
        json.dumps(server_config, indent=2) + "\n"
    )
    results_dir = output_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / "config.xml"

    if workload == "tpcc":
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        config_path.write_text(TPCC_CONFIG_TEMPLATE.format(
            warehouses=scale.tpcc_warehouses, terminals=threads, duration=duration,
            **_template_connection_values(),
        ))
        bench_type = "tpcc"
        metric_name = "new_order_per_sec"
    elif workload in common.HTAP_WORKLOADS:
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        config_path.write_text(CHBENCHMARK_CONFIG_TEMPLATE.format(
            warehouses=scale.tpcc_warehouses, terminals=threads, duration=duration,
            ch_weights=CHBENCHMARK_WEIGHTS[workload], olap_threads=scale.htap_olap_threads,
            **_template_connection_values(),
        ))
        bench_type = "tpcc,chbenchmark"
        metric_name = "new_order_per_sec"
    elif workload == "s_htap":
        duration = scale.s_htap_duration
        # scale.ycsb_threads holds the swept --threads value for every workload (see
        # compare_engines.py's scale_variant construction) - split it into a fixed OLAP-scanner
        # pool plus the remainder as write threads, matching batstore.py's "s_htap" branch exactly
        # (SHTAPBenchmark.makeWorkersImpl on the Java side re-derives the same split from
        # olapThreads/terminals independently, so this is only needed here for the <weights>
        # bookkeeping below, not to tell the JVM anything it couldn't figure out itself).
        threads = scale.ycsb_threads
        olap_threads = min(scale.s_htap_olap_threads, max(1, threads - 1))
        write_threads = max(1, threads - olap_threads)
        write_share = write_threads / threads * 100.0
        olap_share = olap_threads / threads * 100.0
        weights = (
            f"{write_share * scale.s_htap_arrival_ratio:.3f},"
            f"{write_share * (1.0 - scale.s_htap_arrival_ratio):.3f},"
            f"{olap_share:.3f}"
        )
        config_path.write_text(SHTAP_CONFIG_TEMPLATE.format(
            scalefactor=scale.s_htap_record_count / 1000.0,
            terminals=threads, duration=duration, weights=weights,
            hot_window=scale.s_htap_hot_window, hot_theta=scale.s_htap_theta,
            arrival_ratio=scale.s_htap_arrival_ratio, max_lateness=scale.s_htap_max_lateness,
            olap_threads=scale.s_htap_olap_threads, olap_lag=scale.s_htap_olap_lag,
            olap_span=scale.s_htap_olap_span,
            **_template_connection_values(),
        ))
        bench_type = "s_htap"
        metric_name = "write_ops_per_sec"
    else:
        letter = workload.split("_", 1)[1]
        duration = scale.ycsb_duration
        threads = scale.ycsb_threads
        config_path.write_text(YCSB_CONFIG_TEMPLATE.format(
            scalefactor=scale.ycsb_records / 1000.0, theta=ycsb_theta,
            terminals=threads, duration=duration, weights=YCSB_WEIGHTS[letter],
            field_size=8 if ycsb_payload == "u64" else 100,
            **_template_connection_values(),
        ))
        bench_type = "ycsb"
        metric_name = "ops_per_sec"

    _assert_unlimited_config(config_path)
    client_numa_node = common.external_client_numa_node()
    print(
        f"PostgreSQL BenchBase config: {config_path} "
        f"(rate=unlimited, terminals={threads}, client_numa_node={client_numa_node})"
    )
    base_args = [
        "java", "-Duser.language=en", "-Duser.country=US", "-jar", str(BENCHBASE_JAR),
        "-b", bench_type, "-c", str(config_path),
    ]
    timeout = common.default_subprocess_timeout(duration)
    bench_env = os.environ.copy()
    bench_env["YCSB_READ_PAYLOAD"] = "true" if read_payload else "false"
    bench_env["YCSB_U64_PAYLOAD"] = "true" if ycsb_payload == "u64" else "false"

    if reload:
        load_results_dir = output_dir / "load_results"
        load_results_dir.mkdir(parents=True, exist_ok=True)
        load_args = [
            *base_args, "--create=true", "--load=true", "--execute=false",
            "-d", str(load_results_dir),
        ]
        load_returncode, _load_client_rss_unused = common.run_and_track_rss(
            load_args, cwd=BENCHBASE_HOME, env=bench_env,
            stdout_path=output_dir / "load_stdout.log", timeout=timeout,
            numa_node=client_numa_node,
        )
        if load_returncode != 0:
            notes = (
                f"TIMEOUT during create/load after {timeout:.0f}s, see load_stdout.log"
                if load_returncode is None
                else f"FAILED create/load exit={load_returncode}, see load_stdout.log"
            )
            return common.NormalizedResult(
                "postgres", workload, scale.label, duration, metric_name, 0.0, 0.0,
                threads=threads, gc_enabled=gc, notes=notes, memory_source="not_measured",
            )

    _prepare_measured_execution()
    memory_sampler = _start_server_memory_sampler(postmaster_pid)
    execute_args = [
        *base_args, "--create=false", "--load=false", "--execute=true",
        "-d", str(results_dir),
    ]
    try:
        returncode, _client_rss_unused = common.run_and_track_rss(
            execute_args, cwd=BENCHBASE_HOME, env=bench_env,
            stdout_path=output_dir / "stdout.log", timeout=timeout,
            numa_node=client_numa_node,
        )
    finally:
        server_peak_rss_mb, server_memory_source = _finish_server_memory_sampler(
            memory_sampler, output_dir,
        )

    if returncode != 0:
        notes = f"TIMEOUT after {timeout:.0f}s, see stdout.log" if returncode is None else \
            f"FAILED exit={returncode}, see stdout.log"
        return common.NormalizedResult(
            "postgres", workload, scale.label, duration, metric_name, 0.0, server_peak_rss_mb,
            threads=threads, gc_enabled=gc,
            notes=notes, memory_source=server_memory_source,
        )

    if workload in (["tpcc"] + common.HTAP_WORKLOADS):
        # New-Order-only, for tpmC parity with the other 3 engines (see leanstore.py/wiredtiger.py) -
        # also the OLTP-side metric for htap_q1/htap_q6, directly comparable to plain "tpcc"
        # at the same threads/gc for an interference% computation (see plot_compare.py).
        results_csv = next(results_dir.glob("*.results.NewOrder.csv"), None)
    else:
        # The aggregate file (all transaction types) - "*.results.csv" doesn't match the
        # per-type "*.results.ReadRecord.csv" etc. siblings BenchBase also writes.
        results_csv = next(results_dir.glob("*.results.csv"), None)

    value = common.avg_csv_column(results_csv, "Throughput (requests/second)") if results_csv else 0.0

    latency = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "count": 0}
    if workload == "ycsb_e":
        latency = common.latency_from_results(results_dir, "ScanRecord")
    elif workload in common.HTAP_WORKLOADS:
        latency = common.latency_from_results(results_dir, "Q1" if "q1" in workload else "Q6")
    elif workload == "s_htap":
        latency = common.latency_from_results(results_dir, "OlapScan")

    return common.NormalizedResult(
        "postgres", workload, scale.label, duration, metric_name, value, server_peak_rss_mb,
        threads=threads, gc_enabled=gc,
        scan_p50_us=latency["p50"], scan_p95_us=latency["p95"], scan_p99_us=latency["p99"],
        scan_avg_us=latency["avg"], scan_count=latency["count"],
        notes="" if postmaster_pid else "peak_rss unavailable (couldn't locate the postmaster PID)",
        memory_source=server_memory_source,
    )
