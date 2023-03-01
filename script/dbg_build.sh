cd ..
DEBUG_LEVEL=2 ROCKSDB_PLUGINS=zenfs make -j16 db_bench
DEBUG_LEVEL=2 ROCKSDB_PLUGINS=zenfs make -j16 install
cd script
