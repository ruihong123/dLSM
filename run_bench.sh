#!/bin/bash
declare -i THREAD_NUM=16
declare -i TOTAL_NUM=300000000
declare -i NUM_PER_THREAD
NUM_PER_THREAD=TOTAL_NUM/THREAD_NUM
OUTPUTFILE="benchout.txt"

numactl --cpunodebind=1 --localalloc \
    ./db_bench --benchmarks=fillrandom \
    --threads=${THREAD_NUM} --value_size=400 \
    --num=${NUM_PER_THREAD} --bloom_bits=10 \
    --readwritepercent=5 --compute_node_id=0 \
    --fixed_compute_shards_num=0 \
    > ${OUTPUTFILE}

