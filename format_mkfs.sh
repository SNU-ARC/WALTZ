ZNS_SSD_MODELNAME=a825

if [ -d ./plugin/spdk ]
then
  SPDK_DIR=./plugin/spdk/scripts
else
  echo "please install SPDK v22.01.2"
  exit 1
fi

if [ ! -e identify_zns ]
then
$SPDK_DIR/setup.sh reset > /dev/null 2> /dev/null

sleep 3

# Figure out nvmeXnY from pcie address
PCIE_ADDR=`lspci | grep $ZNS_SSD_MODELNAME | head -n1 | awk '{print $1}'`
ZENFS_DEV=`ls -al /sys/block/nvme* | grep $PCIE_ADDR | awk -F/ '{print $NF}'`

echo "PCIeAddress: $PCIE_ADDR" > identify_zns
echo "DeviceName: $ZENFS_DEV" >> identify_zns
else
PCIE_ADDR=`cat identify_zns | grep PCIeAddress | awk '{print $2}'`
ZENFS_DEV=`cat identify_zns | grep DeviceName | awk '{print $2}'`
fi

$SPDK_DIR/setup.sh > /dev/null 2> /dev/null

if [ ! -e ./plugin/zenfs/util/zenfs ]
then
cd plugin/zenfs/util
make
cd ../../..
fi

./plugin/zenfs/util/zenfs format --zbd=$ZENFS_DEV --aux_path=/tmp/aux_zenfs --zns_pci=$PCIE_ADDR

rm -rf /tmp/aux_zenfs
./plugin/zenfs/util/zenfs mkfs --zbd=$ZENFS_DEV --aux_path=/tmp/aux_zenfs --zns_pci=$PCIE_ADDR
