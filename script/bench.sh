#!/bin/bash
ROCKSDB_ROOT=$(pwd)/..
SCRIPT_DIR=$(pwd)
DBBENCH_FILE=db_bench

PCIE_ADDR=`cat ../identify_zns | grep PCIeAddress | awk '{print $2}'`
ZENFS_DEV=`cat ../identify_zns | grep DeviceName | awk '{print $2}'`

TESTMODE=$1
TESTTYPE=$2
VALUESIZE=$3
KEYRANGE=$4
KEYTEST=$5

if [ $TESTTYPE -gt 5 ]
then
TEST=mixgraph
else
TEST=microbench
fi

#BENCHMARKS=prefill,levelstats,resetstats,latencybreakdown,$TEST,levelstats,stats
BENCHMARKS=prefill,levelstats,resetstats,$TEST,levelstats,stats

OUTFILE=stdout_test${TESTMODE}_test${TESTTYPE}.log
ERRFILE=stderr_test${TESTMODE}_test${TESTTYPE}.log

# testmode 0 - WALTZ mode, need 'enablewaltzmode' benchmark at the beginning
# testmode 1 - baseline
if [ $TESTMODE -eq 0 ]
then
BENCHMARKS=enablewaltzmode,$BENCHMARKS
fi

THREADS=4

OPTIONS+=' --waltz_test_type='$TESTTYPE
OPTIONS+=' --waltz_zipf_dist=0.99'
OPTIONS+=' --waltz_scan_max=100'
OPTIONS+=' --waltz_key_range='$KEYRANGE
OPTIONS+=' --waltz_pcie_addr='$PCIE_ADDR

# ZenFS related parameters
OPTIONS+=' --fs_uri=zenfs://dev:'$ZENFS_DEV

# Background jobs
OPTIONS+=' --max_background_compactions=4'
OPTIONS+=' --max_background_flushes=4'
OPTIONS+=' --max_background_jobs=8'

# key/value size & compression
OPTIONS+=' --key_size=8'
OPTIONS+=' --prefix_size=8'
OPTIONS+=' --value_size='$VALUESIZE
OPTIONS+=' --compression_type=none'
OPTIONS+=' --sync=true'

OPTIONS+=' --enable_pipelined_write=false'

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
if [ $TESTTYPE -lt 6 ]
then
# num option is differently used in MixGraph workload, skip setting here
OPTIONS+=' --num='$((KEYTEST/THREADS))
fi
OPTIONS+=' --threads='$THREADS
OPTIONS+=' --open_files=500000'
OPTIONS+=' --use_existing_db=0'

if [ $TESTTYPE -gt 5 ]
then
# MixGraph related options

# Random or Dist, AllDist (7) or PreDist (9)
if [ $TESTTYPE -eq 7 ] || [ $TESTTYPE -eq 9 ]
then
OPTIONS+=' --key_dist_a=0.002312'
OPTIONS+=' --key_dist_b=0.3467'
fi

# All or Prefix
if [ $TESTTYPE -eq 6 ] || [ $TESTTYPE -eq 7 ]
then
# All - AllRand (6), AllDist (7)
OPTIONS+=' --keyrange_num=1'
else
# Prefix - PreRand (8), PreDist (9)
OPTIONS+=' --keyrange_dist_a=14.18'
OPTIONS+=' --keyrange_dist_b=-2.917'
OPTIONS+=' --keyrange_dist_c=0.0164'
OPTIONS+=' --keyrange_dist_d=-0.08082'
OPTIONS+=' --keyrange_num=30'
fi

OPTIONS+=' --value_theta='$VALUESIZE
OPTIONS+=' --value_sigma=0'
OPTIONS+=' --mix_max_value_size='$VALUESIZE

OPTIONS+=' --iter_k=2.517'
OPTIONS+=' --iter_sigma=14.236'
OPTIONS+=' --mix_get_ratio=0.83'
OPTIONS+=' --mix_put_ratio=0.14'
OPTIONS+=' --mix_seek_ratio=0.03'
OPTIONS+=' --sine_mix_rate_interval_milliseconds=5000'

OPTIONS+=' --num='$KEYRANGE
OPTIONS+=' --reads='$((KEYTEST/THREADS))

# disable rate limiter related options
OPTIONS+=' --sine_a=0'
OPTIONS+=' --sine_b=0'
OPTIONS+=' --sine_d=0'
#OPTIONS+=' --sine_a=1000'
#OPTIONS+=' --sine_b=0.000073'
#OPTIONS+=' --sine_d=4500'
fi

$ROCKSDB_ROOT/$DBBENCH_FILE $OPTIONS > ${OUTFILE} 2> ${ERRFILE}
#$ROCKSDB_ROOT/$DBBENCH_FILE $OPTIONS
#gdb --args $ROCKSDB_ROOT/$DBBENCH_FILE $OPTIONS

echo $OPTIONS >> ${ERRFILE}
