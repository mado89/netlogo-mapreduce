ifeq ($(origin JAVA_HOME), undefined)
  JAVA_HOME=/usr
endif

ifeq ($(origin NETLOGO), undefined)
  NETLOGO=/media/data/Uni/DA/netlogo-5.0
endif

ifeq ($(origin SCALA_HOME), undefined)
  SCALA_HOME=../..
endif

JAVAC = $(JAVA_HOME)/bin/javac


SRCS=$(wildcard src/org/nlogo/extensions/mapreduce/*.java src/org/nlogo/extensions/mapreduce/commands/*.java)
SRC2=$(wildcard src/at/dobiasch/mapreduce/*.java src/at/dobiasch/mapreduce/framework/*.java)
SRCSLIB=$(wildcard client/org/nlogo/*.scala)

mapreduce/mapreduce.jar: $(SRCS) manifest.txt mapreduce/framework.jar
	mkdir -p classes
	$(JAVAC) -g -encoding us-ascii -source 1.5 -target 1.5 -classpath $(NETLOGO)/NetLogo.jar:/usr/share/java/log4j-1.2.jar:mapreduce/framework.jar -d classes $(SRCS)
	jar cmf manifest.txt mapreduce/mapreduce.jar -C classes .

mapreduce/framework.jar: $(SRC2) mapreduce/client-lib.jar
	mkdir -p classes
	$(JAVAC) -g -encoding us-ascii -source 1.5 -target 1.5 -classpath $(NETLOGO)/NetLogo.jar:/usr/share/java/log4j-1.2.jar:mapreduce/client-lib.jar -d classes $(SRC2)
	jar cf mapreduce/framework.jar -C classes .

mapreduce/client-lib.jar:
	mkdir -p tmp
	$(SCALA_HOME)/bin/scalac -deprecation -unchecked -encoding us-ascii -classpath $(NETLOGO)/NetLogo.jar -d tmp $(SRCSLIB)
	jar cf mapreduce/client-lib.jar -C tmp .
	rm -r tmp

