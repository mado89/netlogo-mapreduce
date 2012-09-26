#!/bin/bash

scp ../../../input1/*.txt user1@192.168.56.101:/tmp/input1
ssh user1@192.168.56.101 "mkdir -p /tmp/input1; cd /media/hadoop/hadoop;bin/hadoop dfs -copyFromLocal /tmp/input1 /; bin/hadoop dfs -ls /input1"

