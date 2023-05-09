# WALTZ

WALTZ: Leveraging Zone Append to Tighten the Tail Latency of LSM Tree on ZNS SSD

# List of changes
db/db\_impl/db\_impl\_write.cc
plugin/zenfs/fs/fs\_zenfs.cc
plugin/zenfs/fs/io\_zenfs.cc
plugin/zenfs/fs/zbd\_zenfs.cc

We changed the db\_impl\_write.cc to skip the batch-group write for WALTZ.
For WALTZ, all the write first encounters the PreprocessWrite, then it directly delivered to the ZNSAppendToWAL instead WriteToWAL of baseline.

Also for SPDK porting, we changed the ZenFS-related source codes, which are located in the plugin/zenfs/fs
The functionality of ZoneManager, ReplacementChecker and other optimization features such as lazy metadata update and zone reservation, is implemented here.

# Instruction

1. Install SPDK v22.01.2 at the plugin directory

```shell
$ cd plugin
$ git clone https://github.com/spdk/spdk
$ cd spdk
$ git checkout v22.01.2
$ git submodule update --init
$ sudo ./scripts/pkgdep.sh
$ ./configure --with-shared --without-isal
$ make -j4
$ cd ../..
```

2. Install prerequisite library

```shell
$ sudo apt install libgflags-dev
```

3. Build WALTZ

``` shell
$ cd script
$ ./rel_build.sh
$ cd ..
```

4. Specify the model name of ZNS SSD
WALTZ needs to specify the model code of the ZNS SSD device in the first line of format\_mkfs.sh to configure the PCIe BDF address (e.g., 01:00.0).
WALTZ uses lspci and grep to extract the PCIe address and pick the first one if there are several devices with same model code.
It is set to a825 as a default, which is the model code of Samsung PM1731a ZNS SSD.

5. Run the benchmark scripts
``` shell
$ cd script
$ ./test.sh
$ cd ..
```
