#!/bin/bash
ROCKSDB_ROOT=$(pwd)/..
SCRIPT_DIR=$(pwd)
DBBENCH_FILE=db_bench

ZNS_DEV=nvme0n1
BENCHMARKS=stats,levelstats

THREADS=8

# ZenFS related parameters
OPTIONS='--fs_uri=zenfs://dev:'$ZNS_DEV

# Background jobs
OPTIONS+=' --max_background_compactions=1'
OPTIONS+=' --max_background_flushes=1'

# key/value size & compression
OPTIONS+=' --key_size=8'
OPTIONS+=' --prefix_size=8'
OPTIONS+=' --value_size=1024'
OPTIONS+=' --compression_type=none'
#OPTIONS+=' --compression_type=zstd'
#OPTIONS+=' --disable_wal=0'

# histogram parameters
OPTIONS+=' --statistics=1'
OPTIONS+=' --histogram=true'

# direct i/o parameters
OPTIONS+=' --use_direct_reads'
OPTIONS+=' --use_direct_io_for_flush_and_compaction'

# write rate limiter parameters
#OPTIONS+=' --benchmark_write_rate_limit='$((80*1024*1024))

# Perf level : measure counter (2)
#OPTIONS+=' --perf_level=2'

# Memtable
OPTIONS+=' --memtablerep=skip_list'

# Bloom filter
OPTIONS+=' --bloom_bits=10'
OPTIONS+=' --bloom_locality=1'

OPTIONS+=' --stats_per_interval=0'
OPTIONS+=' --stats_interval_seconds=20'
OPTIONS+=' --benchmarks='$BENCHMARKS
OPTIONS+=' --num=1'
OPTIONS+=' --threads='$THREADS
OPTIONS+=' --open_files=500000'
OPTIONS+=' --use_existing_db=1'
#OPTIONS+=' --use_existing_db=0'

# 0 - kByCompensatedSize // larger files by size compensated by deletes
# 1 - kOldestLargestSeqFirst // old files to keep small hot range in higher level
# 2 - kOldestSmallestSeqFirst // for uniform random
# 3 - kMinOveralappingRatio (default) // minimize write amplification

#COMPACTION_PRI=3
#OPTIONS+=' --compaction_pri='$COMPACTION_PRI

#$ROCKSDB_ROOT/$DBBENCH_FILE $OPTIONS > ${OUTFILE} 2> ${ERRFILE}
$ROCKSDB_ROOT/$DBBENCH_FILE $OPTIONS
#cgdb --args $ROCKSDB_ROOT/$DBBENCH_FILE $OPTIONS
#valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --verbose --log-file=valgrind.log $ROCKSDB_ROOT/$DBBENCH_FILE $OPTIONS


#mv latency.log latency_${COMPACTION_PRI}.log
