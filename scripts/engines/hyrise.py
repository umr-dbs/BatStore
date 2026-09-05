"""Hyrise engine wrapper - drives the SAME BenchBase (github.com/cmu-db/benchbase) JDBC
harness as engines/postgres_benchbase.py / engines/umbra_benchbase.py, against a
natively-built `hyriseServer` (https://github.com/hyrise/hyrise - HPI's in-memory,
column-oriented research OLTP+OLAP engine) instead of a real PostgreSQL server or Umbra's
Docker image.

Hyrise ships no Docker image with a working server target for this harness to use (its
own Dockerfile exists but isn't a published image this script could just pull) - it's
built from source instead (setup_environment.py::step_hyrise: CMake + ~20 git
submodules) into one `hyriseServer` binary (src/bin/server.cpp), which implements the
PostgreSQL wire protocol on a configurable --port (upstream defaults to 5432; this
wrapper always overrides it, see common.HYRISE_PORT) well enough, per its own source and
docs, that BenchBase's existing <type>POSTGRES</type> JDBC target should work against it -
the same integration umbra_benchbase.py already relies on for a different third-party
engine.

CONFIRMED live against a real hyriseServer (raw wire-protocol probe, not just psql):
CREATE TABLE/INSERT/SELECT work correctly (CREATE TABLE's own CommandComplete tag is
wrong - "SELECT 0" instead of "CREATE TABLE" - but the DDL itself takes effect and
BenchBase's JDBC path doesn't parse that particular tag's content). `SELECT ... FOR
UPDATE` inside a transaction also works, unlike Umbra - so TPC-C's NewOrder isn't broken
by that. DECIMAL columns silently lose precision (round-tripped 99.99 as 99.9899979 -
Hyrise stores them as float internally; matches its own native TPC-C benchmark's source
comment "As decimals are not supported, we use floats instead").

BUT: UPDATE and DELETE send a malformed CommandComplete tag - confirmed at the raw byte
level, independent of any client library's own parsing: the server literally sends
b"UPDATE -1\x00" / b"DELETE -1\x00" (a negative "rows affected" count is not valid
PostgreSQL wire-protocol syntax; the real value is a per-statement row count Hyrise's
server evidently never tracks for these two statement kinds). psql's own libpq refuses to
interpret it ("could not interpret result from server: UPDATE -1"), and pgjdbc parses the
identical wire message the identical way - so ANY workload that issues an UPDATE or
DELETE will fail as soon as it does. Checked against every workload's actual op mix
(YCSB_WEIGHTS below, TPCC_CONFIG_TEMPLATE, CHBENCHMARK_CONFIG_TEMPLATE, TPC-C's
mandatory NewOrder stock UPDATE / Delivery new_order DELETE): tpcc, ycsb_a
(UpdateRecord), ycsb_b (UpdateRecord), ycsb_f (ReadModifyWriteRecord, itself a
read-then-update), every htap_q1/htap_q6 (+ variants, TPC-C's OLTP side), and s_htap
(HotTailUpdate) ALL issue at least one UPDATE or DELETE and are therefore BROKEN_WORKLOADS
below - SKIPped before a server is even started, the same treatment
umbra_benchbase.py's run() gives Umbra+TPC-C, just for a confirmed reason specific to
each of these six rather than one. ycsb_c (100% ReadRecord), ycsb_d (ReadRecord +
InsertRecord), and ycsb_e (InsertRecord + ScanRecord) never issue either statement and
are unaffected - these three are the only workloads this wrapper actually attempts.

Since this build of Hyrise is in-memory-only with no on-disk persistence at all (no WAL,
no checkpoint/log file - confirmed against src/bin/server.cpp and CMakeLists.txt: the
only disk-adjacent server flag is --benchmark_data, which this wrapper never sets, since
BenchBase creates and loads its own schema instead), there is no scratch directory to
wipe (see common.fresh_scratch_dir - unused here), no NO_DURABILITY branch (unlike Umbra,
which needs one because ITS fsync can't be turned off - Hyrise never pays that cost in
the first place, in every compare_engines*.py mode alike), and no documented
autovacuum-equivalent GC toggle - SUPPORTS_GC_TOGGLE = False, matching
leanstore.py/wiredtiger.py/umbra_benchbase.py.

Spawned fresh per run() as this harness's own OS process (numactl-pinned directly, like
every native engine here - common.numactl_prefix), not a long-running service or
container: started, waited for its port to accept connections, handed to BenchBase, then
killed - the same "fresh every run" lifecycle umbra_benchbase.py gives its container,
just via a plain subprocess instead of Docker. Because numactl execs directly into
hyriseServer (no wrapper layer remains, unlike `docker run` vs. the container's own PID),
the Popen's own pid IS the server's pid - no separate "inspect the container" step is
needed to find it for NUMA-binding verification or RSS sampling.
"""
from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

from . import common, postgres_benchbase, umbra_benchbase

HYRISE_REPO = common.HYRISE_REPO
HYRISE_BUILD_DIR = common.HYRISE_BUILD_DIR
HYRISE_SERVER_BIN = HYRISE_BUILD_DIR / "hyriseServer"

BENCHBASE_HOME = common.BENCHBASE_HOME
BENCHBASE_JAR = BENCHBASE_HOME / "benchbase.jar"


def ensure_built() -> None:
    # Shared BenchBase jar, same as postgres_benchbase.py/umbra_benchbase.py - no
    # Hyrise-specific Maven profile needed, it's driven through the plain POSTGRES target.
    postgres_benchbase.ensure_built()

    if not (HYRISE_REPO / "CMakeLists.txt").exists():
        raise SystemExit(
            f"Hyrise checkout not found at {HYRISE_REPO.resolve(strict=False)} (or its "
            f"submodules aren't fetched) - run `python3 scripts/setup_environment.py` "
            f"first (its step_hyrise clones the repo AND the ~20 git submodules Hyrise's "
            f"build needs; a plain `git clone` alone won't fetch those)."
        )
    common.check_release_build(HYRISE_BUILD_DIR, "Hyrise")
    HYRISE_BUILD_DIR.mkdir(parents=True, exist_ok=True)
    subprocess.run([
        "cmake", "-S", str(HYRISE_REPO), "-B", str(HYRISE_BUILD_DIR),
        "-DCMAKE_BUILD_TYPE=Release",
        # See step_hyrise's own doc: this harness does not install upstream's exact pinned
        # clang-19/gcc-15/LLVM toolchain, so this flag (Hyrise's own documented escape
        # hatch for "non-standard environments") is required to build with whatever
        # compiler is actually on this machine.
        "-DHYRISE_RELAXED_BUILD=On",
        # See setup_environment.py::step_hyrise's own comment: Hyrise defaults to LTO for
        # non-Debug builds and upstream itself warns GCC+LTO link times can be very long -
        # confirmed live (a single link step ran ~16GB RSS for 45+ minutes on a 32GB
        # machine). -DNO_LTO=On is Hyrise's own documented flag to skip it.
        "-DNO_LTO=On",
    ], check=True)
    subprocess.run(
        ["cmake", "--build", str(HYRISE_BUILD_DIR), "--target", "hyriseServer",
         "--parallel", str(os.cpu_count() or 4)],
        check=True,
    )


# No documented autovacuum-equivalent GC toggle - see module doc.
SUPPORTS_GC_TOGGLE = False

# Every workload whose op mix issues at least one UPDATE or DELETE - see module doc for
# the confirmed, raw-wire-protocol-level bug ("UPDATE -1"/"DELETE -1" CommandComplete
# tags) that breaks BenchBase/pgjdbc on both. SKIPped in run() before a server is even
# started, rather than attempted and left to fail mid-load/mid-execute.
BROKEN_WORKLOADS = frozenset(
    ["tpcc", "ycsb_a", "ycsb_b", "ycsb_f", "s_htap"] + common.HTAP_WORKLOADS
)

# Reuse postgres_benchbase.py's weight tables and umbra_benchbase.py's already-parameterized
# ({host}/{port}/{database}/{username}/{password}) BenchBase config templates verbatim - the
# only thing that differs between Umbra and Hyrise here is which connection values get
# substituted in (see _template_connection_values below) and one extra JDBC URL parameter
# (see _add_gss_enc_mode_disable below), not the XML shape itself.
YCSB_WEIGHTS = postgres_benchbase.YCSB_WEIGHTS
CHBENCHMARK_WEIGHTS = postgres_benchbase.CHBENCHMARK_WEIGHTS


def _add_gss_enc_mode_disable(template: str) -> str:
    """pgjdbc defaults to `gssEncMode=allow`, which sends a GSSENCRequest negotiation
    packet before the real login - confirmed live that hyriseServer does not implement
    that (or SSLRequest-style) pre-startup negotiation at all: it replies with a garbage
    byte, and pgjdbc's own confusion at that reply causes it to abort the connection,
    which then crashes hyriseServer outright (uncaught `ClientDisconnectException` ->
    std::terminate - not a per-connection failure, the WHOLE server process dies).
    `gssEncMode=disable` skips that negotiation entirely so pgjdbc goes straight to a
    plain startup packet, exactly like the psql/raw-socket connections that never
    triggered this - confirmed live this avoids the crash.
    """
    marker = "preferQueryMode=simple</url>"
    assert marker in template, "expected Umbra's JDBC URL template shape to be unchanged"
    return template.replace(marker, "preferQueryMode=simple&amp;gssEncMode=disable</url>")


TPCC_CONFIG_TEMPLATE = _add_gss_enc_mode_disable(umbra_benchbase.TPCC_CONFIG_TEMPLATE)
CHBENCHMARK_CONFIG_TEMPLATE = _add_gss_enc_mode_disable(umbra_benchbase.CHBENCHMARK_CONFIG_TEMPLATE)
YCSB_CONFIG_TEMPLATE = _add_gss_enc_mode_disable(umbra_benchbase.YCSB_CONFIG_TEMPLATE)
SHTAP_CONFIG_TEMPLATE = _add_gss_enc_mode_disable(umbra_benchbase.SHTAP_CONFIG_TEMPLATE)


def _template_connection_values() -> dict:
    return {
        "host": "127.0.0.1", "port": common.HYRISE_PORT,
        "database": common.HYRISE_DATABASE, "username": common.HYRISE_ROLE,
        "password": common.HYRISE_PASSWORD,
    }


def _start_server(stdout_path: Path) -> subprocess.Popen:
    cmd = common.numactl_prefix() + [
        str(HYRISE_SERVER_BIN), "--port", str(common.HYRISE_PORT), "--address", "127.0.0.1",
    ]
    stdout_file = open(stdout_path, "wb")
    # start_new_session=True: same reasoning as common.run_and_track_rss - puts the
    # process (numactl execs in place, so this is genuinely hyriseServer itself) in its
    # own process group so _stop_server can reliably signal it.
    return subprocess.Popen(cmd, stdout=stdout_file, stderr=subprocess.STDOUT, start_new_session=True)


def _wait_for_server(proc: subprocess.Popen, timeout: float = 30.0) -> bool:
    """Polls a raw TCP connect to the published port, not a SQL query: unlike Umbra's
    Docker port-forward (which accepts the TCP handshake well before umbra-server itself
    is ready - see umbra_benchbase.py's own comment on this), hyriseServer is a directly
    spawned process with no intermediary port-forwarding layer, so the OS refusing the
    connection until the process is actually listen()ing is a genuine readiness signal
    here. Also bails out early if the process has already exited (a startup crash),
    rather than polling a dead process for the full timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", common.HYRISE_PORT), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def _verify_server_numa_binding(pid: int) -> None:
    """Trust but verify - same reasoning as postgres_benchbase.py's
    _verify_postmaster_numa_binding / umbra_benchbase.py's container equivalent: confirm
    numactl's --cpubind/--membind actually took effect on the live process rather than
    silently benchmarking an unpinned server."""
    status: dict = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            status[key] = value.strip()
    expected_cpus = common.expand_cpu_list(common.numa_node_cpu_list())
    actual_cpus = common.expand_cpu_list(status.get("Cpus_allowed_list", ""))
    actual_nodes = common.expand_cpu_list(status.get("Mems_allowed_list", ""))
    if actual_cpus != expected_cpus or actual_nodes != {common.NUMA_NODE}:
        sys.exit(
            f"hyriseServer (PID {pid}) is not pinned to NUMA node {common.NUMA_NODE}: "
            f"Cpus_allowed_list={status.get('Cpus_allowed_list')!r}, "
            f"Mems_allowed_list={status.get('Mems_allowed_list')!r}; expected CPUs "
            f"{common.numa_node_cpu_list()!r}, memory node {common.NUMA_NODE}. Is `numactl` "
            f"installed and working?"
        )


def _stop_server(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "on", reload: bool = True,
        ycsb_payload: str = "standard", read_payload: bool = True) -> common.NormalizedResult:
    """`gc` accepted for interface parity with the other engine wrappers but unused - see
    SUPPORTS_GC_TOGGLE above. `reload` accepted for parity too; every run() already starts
    from a freshly spawned, empty server (no persisted state - see module doc), so there
    is no cheaper "skip --create/--load" path to offer here, same as umbra_benchbase.py.
    """
    del reload
    if workload in BROKEN_WORKLOADS:
        # See module doc / BROKEN_WORKLOADS's own comment: confirmed, raw-wire-protocol
        # bug in hyriseServer's UPDATE/DELETE CommandComplete tags breaks every workload
        # that issues either statement. SKIPped here, before a server is even started,
        # rather than paying for a load phase that can only ever fail at its first write.
        if workload == "tpcc" or workload in common.HTAP_WORKLOADS:
            duration, threads, metric_name = scale.tpcc_duration, scale.tpcc_terminals, "new_order_per_sec"
        elif workload == "s_htap":
            duration, threads, metric_name = scale.s_htap_duration, scale.ycsb_threads, "write_ops_per_sec"
        else:
            duration, threads, metric_name = scale.ycsb_duration, scale.ycsb_threads, "ops_per_sec"
        return common.NormalizedResult(
            "hyrise", workload, scale.label, duration, metric_name, 0.0, 0.0,
            threads=threads, gc_enabled=gc,
            notes="SKIPPED: this workload issues UPDATE and/or DELETE, which hyriseServer "
                  "sends a malformed CommandComplete tag for ('UPDATE -1'/'DELETE -1', "
                  "confirmed at the raw wire-protocol level - not a valid row count) - "
                  "pgjdbc cannot parse this and fails on the first UPDATE/DELETE. See "
                  "this module's own doc for the full confirmation.",
        )

    if workload in common.YCSB_WORKLOADS and scale.ycsb_theta == 1.0:
        # Same BenchBase jar/patch as postgres_benchbase.py/umbra_benchbase.py - see
        # postgres_benchbase.py's own comment on patches/ycsb_skew_factor_benchbase.patch.
        # A BenchBase-level limitation (the ZipfianGenerator singularity), not specific to
        # which backend it's driving - applies here exactly as it does for every other
        # BenchBase-driven engine.
        return common.NormalizedResult(
            "hyrise", workload, scale.label, scale.ycsb_duration, "ops_per_sec", 0.0, 0.0,
            threads=scale.ycsb_threads, gc_enabled=gc,
            notes="SKIPPED: BenchBase's YCSB module rejects skewFactor==1 (the "
                  "ZipfianGenerator singularity - no reasonable substitute)",
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    results_dir = output_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / "config.xml"

    proc = _start_server(output_dir / "server.log")
    try:
        if not _wait_for_server(proc):
            return common.NormalizedResult(
                "hyrise", workload, scale.label, 0.0, "ops_per_sec", 0.0, 0.0,
                threads=scale.ycsb_threads, gc_enabled=gc,
                notes="FAILED: hyriseServer never started listening within 30s (or exited "
                      "early) - see server.log",
            )
        pid = proc.pid
        _verify_server_numa_binding(pid)

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
                scalefactor=scale.ycsb_records / 1000.0, theta=scale.ycsb_theta,
                terminals=threads, duration=duration, weights=YCSB_WEIGHTS[letter],
                field_size=8 if ycsb_payload == "u64" else 100,
                **_template_connection_values(),
            ))
            bench_type = "ycsb"
            metric_name = "ops_per_sec"

        postgres_benchbase._assert_unlimited_config(config_path)
        client_numa_node = common.external_client_numa_node()
        print(f"Hyrise BenchBase config: {config_path} (rate=unlimited, terminals={threads}, "
              f"port={common.HYRISE_PORT}, client_numa_node={client_numa_node})")

        tree_sampler = common.start_process_tree_sampler(pid)

        args = [
            "java", "-Duser.language=en", "-Duser.country=US", "-jar", str(BENCHBASE_JAR),
            "-b", bench_type, "-c", str(config_path),
            "--create=true", "--load=true", "--execute=true",
            "-d", str(results_dir),
        ]
        timeout = common.default_subprocess_timeout(duration)
        bench_env = os.environ.copy()
        bench_env["YCSB_READ_PAYLOAD"] = "true" if read_payload else "false"
        bench_env["YCSB_U64_PAYLOAD"] = "true" if ycsb_payload == "u64" else "false"
        returncode, _client_rss_unused = common.run_and_track_rss(
            args, cwd=BENCHBASE_HOME, env=bench_env, stdout_path=output_dir / "stdout.log", timeout=timeout,
            numa_node=client_numa_node,
        )

        server_peak_rss_mb = 0.0
        if tree_sampler:
            stop, thread, peak_box = tree_sampler
            stop.set()
            thread.join()
            server_peak_rss_mb = peak_box["mb"]

        if returncode != 0:
            notes = f"TIMEOUT after {timeout:.0f}s, see stdout.log" if returncode is None else \
                f"FAILED exit={returncode}, see stdout.log (Hyrise's BenchBase support is "\
                f"unverified - see this module's own doc)"
            return common.NormalizedResult(
                "hyrise", workload, scale.label, duration, metric_name, 0.0, server_peak_rss_mb,
                threads=threads, gc_enabled=gc,
                notes=notes,
            )

        if workload in (["tpcc"] + common.HTAP_WORKLOADS):
            results_csv = next(results_dir.glob("*.results.NewOrder.csv"), None)
        else:
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
            "hyrise", workload, scale.label, duration, metric_name, value, server_peak_rss_mb,
            threads=threads, gc_enabled=gc,
            scan_p50_us=latency["p50"], scan_p95_us=latency["p95"], scan_p99_us=latency["p99"],
            scan_avg_us=latency["avg"], scan_count=latency["count"],
        )
    finally:
        _stop_server(proc)
