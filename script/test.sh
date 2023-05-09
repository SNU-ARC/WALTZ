#!/bin/bash

RUN_SCRIPT=./bench.sh

ulimit -n 100000

TEST_WALTZ=1
TEST_ZENFS=1

# 8GB db setting (4K value, 2M req)
VALUE_SIZE=4000
#KEYRANGE=$((20*1000))
#KEYTEST=$((20*1000))
#KEYRANGE=$((20*1000*1000))
#KEYTEST=$((20*1000*1000))

# 128GB DB, 10M test
#KEYRANGE=$((32*1000*1000))
#KEYTEST=$((10*1000*1000))

# 256GB DB, 20M test
KEYRANGE=$((64*1000*1000))
KEYTEST=$((20*1000*1000))

# 512GB DB, 40M test
#KEYRANGE=$((128*1000*1000))
#KEYTEST=$((40*1000*1000))

TEST_WRONLY_UNIFORM=1
TEST_MIXED_73_UNIFORM=1
TEST_MIXED_37_UNIFORM=1
TEST_WRONLY_ZIPFIAN=1
TEST_MIXED_73_ZIPFIAN=1
TEST_MIXED_37_ZIPFIAN=1
TEST_MIXGRAPH=1

if [ $TEST_WRONLY_UNIFORM -eq 1 ]
then
TESTS=$TESTS" 0"
fi

if [ $TEST_MIXED_73_UNIFORM -eq 1 ]
then
TESTS=$TESTS" 1"
fi

if [ $TEST_MIXED_37_UNIFORM -eq 1 ]
then
TESTS=$TESTS" 2"
fi

if [ $TEST_WRONLY_ZIPFIAN -eq 1 ]
then
TESTS=$TESTS" 3"
fi

if [ $TEST_MIXED_73_ZIPFIAN -eq 1 ]
then
TESTS=$TESTS" 4"
fi

if [ $TEST_MIXED_37_ZIPFIAN -eq 1 ]
then
TESTS=$TESTS" 5"
fi

if [ $TEST_MIXGRAPH -eq 1 ]
then
TESTS=$TESTS" 6 7 8 9"
fi

function print_duration() {
    SECS=$1
    HRS=$((SECS/3600))
    SECS=$((SECS%3600))
    MINS=$((SECS/60))
    SECS=$((SECS%60))
    echo "$HRS"h "$MINS"m "$SECS" s
}
function drop_cache(){
    echo "Drop cache & storage clear"
    sync
    echo 3 > /proc/sys/vm/drop_caches
}
function format_zns(){
    echo "Format ZNS"
    cd ..
    ./format_mkfs.sh
    cd script
    echo "Format Done"
}

swapoff -a

for CUR_TEST in $TESTS
do

echo "KeyRange $(($KEYRANGE/1000000))M, Test $(($KEYTEST/1000000))M, TEST ${CUR_TEST} "`date`

if [ $TEST_WALTZ -eq 1 ]
then
  echo "Format ZNS start at "`date`
  SECONDS=0
  format_zns
  echo "Format ZNS for tc0(WALTZ) $(print_duration $SECONDS)" | tee -a time.log

  drop_cache
  echo "WALTZ test$CUR_TEST start at "`date`
  SECONDS=0
  $RUN_SCRIPT 0 $CUR_TEST $VALUE_SIZE $KEYRANGE $KEYTEST
  echo "Run test$CUR_TEST for tc0(WALTZ) $(print_duration $SECONDS)" | tee -a time.log
  cat stdout_waltz_test${CUR_TEST}.log | grep microbench
  cat stdout_waltz_test${CUR_TEST}.log | grep mixgraph
fi

if [ $TEST_ZENFS -eq 1 ]
then
  echo "Format ZNS start at "`date`
  SECONDS=0
  format_zns
  echo "Format ZNS for tc1(ZenFS) $(print_duration $SECONDS)" | tee -a time.log

  drop_cache
  echo "ZenFS test$CUR_TEST start at "`date`
  SECONDS=0
  $RUN_SCRIPT 1 $CUR_TEST $VALUE_SIZE $KEYRANGE $KEYTEST
  echo "Run test$CUR_TEST for tc1(ZenFS) $(print_duration $SECONDS)" | tee -a time.log
  cat stdout_zenfs_test${CUR_TEST}.log | grep microbench
  cat stdout_zenfs_test${CUR_TEST}.log | grep mixgraph
fi

done

echo "Done "`date`
