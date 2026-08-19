#!/usr/bin/env bash
# Runs the YCSB skew sweep, waits 5s, then runs the HTAP analytical-thread sweep.
#
# Pass arguments for each script separated by "--": everything before "--" is forwarded to
# run_skew_sweep.py, everything after is forwarded to run_htap_analytical_sweep.py.
#
# Usage:
#   scripts/run_sweeps.sh
#   scripts/run_sweeps.sh --engines batstore,libmdbx -- --engines batstore,libmdbx
#   scripts/run_sweeps.sh --tiny --skews uniform,0.99 -- --olap-threads 1,2,4
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

skew_args=()
htap_args=()
in_htap=false
for arg in "$@"; do
    if [[ "$arg" == "--" ]]; then
        in_htap=true
        continue
    fi
    if $in_htap; then
        htap_args+=("$arg")
    else
        skew_args+=("$arg")
    fi
done

echo "===== running YCSB skew sweep ====="
python3 "$SCRIPT_DIR/run_skew_sweep.py" "${skew_args[@]}"

echo "===== skew sweep done, sleeping 5s before HTAP analytical sweep ====="
sleep 5

echo "===== running HTAP analytical-thread sweep ====="
python3 "$SCRIPT_DIR/run_htap_analytical_sweep.py" "${htap_args[@]}"

echo "===== both sweeps complete ====="
