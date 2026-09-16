#!/usr/bin/env bash
# H5: fixed analytical query size while OLTP terminal count increases.
set -euo pipefail
cd "$(dirname "$0")"
if [[ ${1:-} == --help || ${1:-} == -h ]]; then
  python3 scripts/compare_engines.py --help
  exit 0
fi

# A fixed 32-warehouse database gives every point the same analytical scan
# volume. Raising warehouses with terminals changes both scan volume and
# concurrency, so latency differences could not be attributed to concurrency.
warehouses=${H5_WAREHOUSES:-32}
terminals=${H5_TERMINALS:-1,2,4,8,16,32,48,64,80,96,112,120}
duration=${H5_DURATION:-60}
mdbx_timeout=${H5_MDBX_TIMEOUT:-3600}
output_root="${H5_OUTPUT_ROOT:-comparison_h5}/session_$(date +%Y%m%d_%H%M%S)_$$"

python3 scripts/compare_engines.py \
  --engines batstore,wiredtiger,postgres,libmdbx \
  --workloads htap_q1,htap_q6 \
  --threads "$terminals" \
  --warehouses "$warehouses" \
  --htap-olap-threads 2 \
  --scan-pool-workers 120 \
  --tpcc-duration "$duration" \
  --libmdbx-timeout "$mdbx_timeout" \
  --gc on \
  --affinity off \
  --output-root "$output_root" "$@"

run_dir=$(find "$output_root" -mindepth 1 -maxdepth 1 -type d -name 'run_*' -print -quit)
python3 scripts/plot_compare.py --run-dir "$run_dir"
python3 - "$run_dir/manifest.csv" "$run_dir/run_config.json" <<'PY'
import csv
import itertools
import json
import sys

with open(sys.argv[1], newline="") as f:
    rows = list(csv.DictReader(f))
with open(sys.argv[2]) as f:
    config = json.load(f)
expected = set(itertools.product(
    ("batstore", "wiredtiger", "postgres", "libmdbx"),
    ("htap_q1", "htap_q6"),
    (str(t) for t in config["threads"]),
))
actual = {(r["engine"], r["workload"], r["threads"]) for r in rows}
missing = expected - actual
bad = [r for r in rows if r["notes"] or int(r["scan_count"]) == 0]
print(f"H5: {len(rows) - len(bad)}/{len(rows)} points have completed analytical samples")
for engine, workload, terminals in sorted(missing):
    print(f"  {engine} {workload} terminals={terminals}: missing result")
if bad:
    for r in bad:
        print(f"  {r['engine']} {r['workload']} terminals={r['threads']}: {r['notes'] or 'no scans'}")
if bad or missing:
    sys.exit(1)
PY
