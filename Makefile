ifeq ($(origin JAVA_HOME), undefined)
  JAVA_HOME=/usr
endif

ifeq ($(origin NETLOGO), undefined)
  NETLOGO=/media/data/netlogo-5.0.5
endif

ifeq ($(origin SCALA_HOME), undefined)
  SCALA_HOME=/usr
endif

JAVAC = $(JAVA_HOME)/bin/javac


# SRCS=$(wildcard src/org/nlogo/extensions/mapreduce/*.java src/org/nlogo/extensions/mapreduce/commands/*.java src/org/nlogo/extensions/mapreduce/commands/config/*.java src/at/dobiasch/mapreduce/*.java src/at/dobiasch/mapreduce/framework/*.java src/at/dobiasch/mapreduce/framework/partition/*.java src/at/dobiasch/mapreduce/framework/task/*.java src/at/dobiasch/mapreduce/framework/controller/*.java src/at/dobiasch/mapreduce/framework/inputparser/*.java src/at/dobiasch/mapreduce/framework/multi/*.java)
SRCS=$(wildcard src/org/nlogo/extensions/mapreduce/*.java src/org/nlogo/extensions/mapreduce/commands/*.java src/org/nlogo/extensions/mapreduce/commands/config/*.java)
SRCS2=$(wildcard src/at/dobiasch/mapreduce/*.java src/at/dobiasch/mapreduce/framework/*.java src/at/dobiasch/mapreduce/framework/partition/*.java src/at/dobiasch/mapreduce/framework/task/*.java src/at/dobiasch/mapreduce/framework/controller/*.java src/at/dobiasch/mapreduce/framework/inputparser/*.java src/at/dobiasch/mapreduce/framework/multi/*.java)
SRCSLIB=$(wildcard client/org/nlogo/*.scala)

all: mapreduce/mapreduce.jar mapreduce/mapreduce-framework.jar mapreduce/client-lib.jar

clean:
	rm mapreduce/mapreduce.jar
	rm mapreduce/mapreduce-framework.jar
	
export: samples
	rm $(wildcard samples/WordCount/output*.txt)
	tar cvfz MapReduce-xx.tar.gz src/ mapreduce netlogo.sh samples/ client 

mapreduce/mapreduce.jar: $(SRCS) manifest.txt mapreduce/mapreduce-framework.jar
	mkdir -p classes
	$(JAVAC) -g -Xlint:all -encoding us-ascii -source 1.5 -target 1.5 -classpath $(NETLOGO)/NetLogo.jar:/usr/share/java/log4j-1.2.jar:mapreduce/client-lib.jar:mapreduce/mapreduce-framework.jar -d classes $(SRCS)
	jar cmf manifest.txt mapreduce/mapreduce.jar -C classes .
	rm -r classes

mapreduce/mapreduce-framework.jar: $(SRCS2) mapreduce/client-lib.jar
	mkdir -p classes
	$(JAVAC) -g -Xlint:all -encoding us-ascii -source 1.5 -target 1.5 -classpath $(NETLOGO)/NetLogo.jar:/usr/share/java/log4j-1.2.jar:mapreduce/client-lib.jar -d classes $(SRCS2)
	jar cf mapreduce/mapreduce-framework.jar -C classes .
	rm -r classes

mapreduce/client-lib.jar: client/org/nlogo/BasicClient.scala
	mkdir -p tmp
	$(SCALA_HOME)/bin/scalac -deprecation -unchecked -encoding us-ascii -classpath $(NETLOGO)/NetLogo.jar -d tmp $(SRCSLIB)
	jar cf mapreduce/client-lib.jar -C tmp .
	rm -r tmp

