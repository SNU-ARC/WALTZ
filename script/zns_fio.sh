DEV=nvme0n1

#fio --zonemode zbd --zonesize 96M --zonecapacity 96M --offset 0 --offset_increment 1% --numjobs 100 --threads 1 --readwrite write --filename /dev/nvme0n1 --blocksize 128k --name test --iodepth 1 --direct 1 --max_open_zones 256 --ioengine libaio --group_report --size 1%

WK=$1
BS=$2

nvme zns reset-zone /dev/$DEV -a
sleep 1
fio --zonemode zbd --zonesize 96M --zonecapacity 96M --offset 0 --offset_increment 1% --numjobs $WK --threads 1 --readwrite write --filename /dev/$DEV --blocksize $BS --name test --iodepth 1 --direct 1 --max_open_zones 256 --ioengine libaio --group_report --size 1%
