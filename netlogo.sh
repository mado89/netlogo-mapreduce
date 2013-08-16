#!/bin/sh
cd "`dirname "$0"`"             # the copious quoting is for handling paths with spaces
# -Djava.library.path=./lib     ensure JOGL can find native libraries 
# -Djava.ext.dirs=              ignore any existing JOGL installation
# -XX:MaxPermSize=128m          avoid OutOfMemory errors for large models
# -Xmx1024m                     use up to 1GB RAM (edit to increase)
# -Dfile.encoding=UTF-8         ensure Unicode characters in model files are compatible cross-platform
# -jar NetLogo.jar              specify main jar
# "$@"                          pass along any command line arguments
CP="$CLASSPATH:/media/data/Uni/DA/netlogo-5.0/NetLogo.jar:/media/data/Uni/DA/mapreduce/mapreduce/framework.jar:/media/data/Uni/DA/mapreduce/mapreduce/client-lib.jar"
/usr/lib/jvm/java-6-sun-1.6.0.26/bin/java -cp $CP -Djava.library.path=./lib -Djava.ext.dirs= -XX:MaxPermSize=128m -Xmx1024m -Dfile.encoding=UTF-8 org.nlogo.app.App "$@"

