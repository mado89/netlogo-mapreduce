#!/bin/sh

cd /media/media/VMs

# VBoxManage clonehd debian.vmdk master.vmdk

VBoxManage createvm --name "Master" --ostype "Debian" --register

VBoxManage modifyvm "Master" --memory 512
VBoxManage modifyvm "Master" --vram 12
VBoxManage modifyvm "Master" --cpus 1
VBoxManage modifyvm "Master" --boot1 disk

# TODO: fix me: hostonly
VBoxManage modifyvm "Master" --nic1 hostonly
VBoxManage modifyvm "Master" --nictype1 82545EM
VBoxManage modifyvm "Master" --hostonlyadapter1 vboxnet0
#VBoxManage modifyvm "Master" --nicproperty1 name=wlan0

VBoxManage storagectl "Master" --name "Master-sata" --add sata --controller IntelAhci --sataportcount 1
VBoxManage storageattach "Master" --storagectl "Master-sata" --port 1 --type hdd --medium master.vmdk
VBoxManage storageattach "Master" --storagectl "Master-sata" --port 2 --device 0 --type hdd --medium hadfs.vmdk

echo "Starting VM"
VBoxManage startvm "Master" > /dev/null
echo "Waiting for startup";
sleep 2
while [ `VBoxManage showvminfo Master --details --machinereadable | grep GuestAdditionsRunLevel | grep -o "[0-9]*"` -lt "2" ]; do
	# echo "Sleeping"
	sleep 1;
done
#to be sure its started
sleep 1;


VBoxManage --nologo guestcontrol "Master" exec --image "/bin/sed" --username root --password pass --wait-exit -- -i "'s/debian/master/g'" /etc/hosts
VBoxManage --nologo guestcontrol "Master" exec --image "/bin/sed" --username root --password pass --wait-exit -- -i "'s/debian/master/g'" /etc/hostname

# ssh-keygen -t rsa -P ""
# cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys
# ssh localhost

# mit root
# ---> an /etc/sysctl.conf anhaengen
# #disable ipv6
# net.ipv6.conf.all.disable_ipv6 = 1
# net.ipv6.conf.default.disable_ipv6 = 1
#net.ipv6.conf.lo.disable_ipv6 = 1
# --> an /etc/fstab anhaengen
# /dev/sdb1	/media/hadoop	ext4	defaults	0	0
# chown chgrp

# shutdown -r now

# scp hadoop-0.23.0.tar.gz user1@192.168.56.101:/media/hadoop
# ssh -->
# cd /media/hadoop/
# tar -xf hadoop-0.23.0.tar.gz 
# rm hadoop-0.23.0.tar.gz 
# mkdir fs
# mv hadoop-0.23.0 hadoop



