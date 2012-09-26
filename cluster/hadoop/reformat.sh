#!/bin/bash

USER=user1
MASTER=192.168.56.101
NODE0=192.168.56.102
NODE1=192.168.56.103
CMDS="rm -r /media/hadoop/dfs/data/*; rm -r /media/hadoop/dfs/name/*; rm -r /media/hadoop/tmp/*"
ssh $USER@$NODE0 "$CMDS"
ssh $USER@$NODE1 "$CMDS"
ssh $USER@$MASTER "$CMDS"
ssh $USER@$MASTER "/media/hadoop/hadoop/bin/hadoop namenode -format"
