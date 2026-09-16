#!/usr/bin/env bash
# H3: one aging snapshot per engine, repeatedly scanned during OLTP.
set -euo pipefail
cd "$(dirname "$0")"

python3 scripts/h3.py \
  --engines batstore,postgres,wiredtiger,libmdbx \
  --warehouses "${H3_WAREHOUSES:-8}" \
  --terminals "${H3_TERMINALS:-2}" \
  --duration "${H3_DURATION:-600}" \
  --buckets "${H3_BUCKETS:-12}" \
  --libmdbx-timeout "${H3_MDBX_TIMEOUT:-3600}" \
  --output-root "${H3_OUTPUT_ROOT:-comparison_h3}" "$@"
