#!/bin/sh

# Set the path to NetLogo
NETLOGO="/media/data/netlogo-5.0.5/"

# Path to mapreduce install
MAPREDUCE="/media/data/netlogo-5.0.5/extensions/"


# -Djava.library.path=./lib     ensure JOGL can find native libraries 
# -Djava.ext.dirs=              ignore any existing JOGL installation
# -XX:MaxPermSize=128m          avoid OutOfMemory errors for large models
# -Xmx1024m                     use up to 1GB RAM (edit to increase)
# -Dfile.encoding=UTF-8         ensure Unicode characters in model files are compatible cross-platform
# -jar NetLogo.jar              specify main jar
# "$@"                          pass along any command line arguments

cd "$NETLOGO"             # the copious quoting is for handling paths with spaces

CP="$CLASSPATH:${NETLOGO}NetLogo.jar:${MAPREDUCE}mapreduce/mapreduce.jar:${MAPREDUCE}mapreduce/client-lib.jar"
java -cp $CP -Djava.library.path=./lib -Djava.ext.dirs= -XX:MaxPermSize=128m -Xmx1024m -Dfile.encoding=UTF-8 org.nlogo.app.App "$@"

