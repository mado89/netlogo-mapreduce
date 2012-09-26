#!/bin/bash

MASTERIP=192.168.56.101
MASTER="Master"
NODE="Node0"
NODENAME="node0"
NODEIP=192.168.56.102
NEWMAC=0800279B4ED7
GATEWAY=192.168.56.255
NETMASK=255.255.255.0

master_preclone () {
	#Check if machine is running
	VMR=`VBoxManage list runningvms | grep $MASTER | wc -l`
	if [ $VMR -eq "0" ]
	then
		#start machine
		VBoxManage startvm $MASTER > /dev/null;
		
		echo "Waiting for startup";
		while [ `VBoxManage showvminfo $MASTER --details --machinereadable | grep GuestAdditionsRunLevel | grep -o "[0-9]*"` -lt "2" ]; do
			# echo "Sleeping"
			sleep 1;
		done
		#to be sure its started
		sleep 1;
	fi
	
	ssh root@$MASTERIP "rm /etc/udev/rules.d/70-persistent-net.rules; shutdown -h now"
	#Waiting for shutdown
	while [ `VBoxManage list runningvms | grep $MASTER | wc -l` -gt "0" ]; do
		# echo "Sleeping"
		sleep 1;
	done
}

# master_preclone

echo "Cloning VM"
VBoxManage clonevm $MASTER --name $NODE --register > /dev/null
VBoxManage modifyvm $NODE --macaddress1 $NEWMAC
sleep 1

echo "Starting VM"
VBoxManage startvm $NODE > /dev/null &


# echo "Sleeping for 30 seconds"
# sleep 30
echo "Waiting for startup";

# AAA=`VBoxManage showvminfo $NODE --details --machinereadable | grep GuestAdditionsRunLevel | grep -o "[0-9]*"`
# echo $AAA
sleep 2
while [ `VBoxManage showvminfo $NODE --details --machinereadable | grep GuestAdditionsRunLevel | grep -o "[0-9]*"` -lt "2" ]; do
	# echo "Sleeping"
	sleep 1;
done
#to be sure its started
sleep 1;

#ping -b -c1 $GATEWAY > /dev/null

#NEWIP=`arp -a | grep $NEWMAC | sed 's/^.*[^0-9]\([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\).*$/\1/'`

echo "Machine is running on $MASTERIP"

# ssh root@$MASTERIP "sed -i 's/master/$NODENAME/g' /etc/hostname;sed -i 's/master/$NODENAME/g' /etc/hosts;sed -i 's/iface eth0 inet dhcp/iface eth0 inet static/g' /etc/network/interfaces;echo '   address $NODEIP' >> /etc/network/interfaces;echo '   netmask $NETMASK' >> /etc/network/interfaces;echo '   broadcast $GATEWAY' >> /etc/network/interfaces;shutdown -h now"
ssh root@$MASTERIP "sed -i 's/master/$NODENAME/g' /etc/hostname;sed -i 's/master/$NODENAME/g' /etc/hosts;shutdown -h now"

