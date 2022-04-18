#!/bin/bash
echo "> Installing required tools for CDP TRIAL"
if  [ -n "$(command -v yum)" ]; then
    echo ">> Detected yum-based Linux"
    sudo yum install -y util-linux
    sudo yum install -y lvm2
    sudo yum install -y e2fsprogs
    sudo yum install -y git
fi
if [ -n "$(command -v apt-get)" ]; then
    echo ">> Detected apt-based Linux"
    sudo apt-get update -y
    sudo apt-get install -y fdisk
    sudo apt-get install -y lvm2
    sudo apt-get install -y e2fsprogs
    sudo apt-get install -y git
fi
ROOT_DISK_DEVICE="/dev/sda"
echo "> Creating new partition for CDP"
sudo fdisk $ROOT_DISK_DEVICE <<EOF
d
n
p
1


w
EOF
sudo partprobe /dev/sda
sudo kpartx -u /dev/sda1
sudo e2fsck -f /dev/sda1
#sudo pvcreate /dev/sda1
sudo xfs_growfs /
cd /
sudo mkdir data

echo "Downloading CDP DC Trial Pre Req Install"

cd ~
git clone https://github.com/carrossoni/CDPDCTrial.git
cd CDPDCTrial
chmod 777 centosvmCDP.sh
sudo ./centosvmCDP.sh

sudo reboot

exit 0