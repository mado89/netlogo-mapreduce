#!/bin/bash

VBoxHeadless --startvm Master --vrdp=off &
echo "Master started"
sleep 3
ssh root@10.0.1.2 "echo \"\" > /etc/udev/rules.d/70-persistent-net.rules;shutdown -h now" 
VBoxManage clonevm Master --name "Node0" --register

