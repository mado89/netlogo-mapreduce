#!/bin/bash
scp *-site.xml hadoop-env.sh masters slaves user1@192.168.56.101:/media/hadoop/hadoop/conf/
scp *-site.xml hadoop-env.sh user1@192.168.56.102:/media/hadoop/hadoop/conf/

