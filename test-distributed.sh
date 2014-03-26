#!/bin/sh

rm output.txt
scala -classpath /media/data/Uni/DA/netlogo-5.0.2/NetLogo.jar:/media/data/Uni/DA/mapreduce/mapreduce/mapreduce.jar:. RunCommand WC-Distributed.nlogo server wc &
sleep 1
scala -classpath /media/data/Uni/DA/netlogo-5.0.2/NetLogo.jar:/media/data/Uni/DA/mapreduce/mapreduce/mapreduce.jar:/media/data/Uni/DA/mapreduce/mapreduce/client-lib.jar:. RunCommand WC-Distributed.nlogo client 
