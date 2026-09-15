  for threads in 1 2 4 8 16 32 48 64 80 96 112 120; do
    warehouses=$(( threads > 16 ? 2*threads : 16 ))

    python3 scripts/compare_engines.py \
      --engines batstore,wiredtiger,postgres,libmdbx \
      --workloads htap_q1,htap_q6 \
      --threads "$threads" \
      --warehouses "$warehouses" \
      --htap-olap-threads 2 \
      --scan-pool-workers 126 \
      --tpcc-duration 60 \
      --gc on \
      --affinity off \
      --output-root comparison_h5
  done
