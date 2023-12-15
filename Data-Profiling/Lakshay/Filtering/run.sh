#!/bin/bash
rm *.class

javac -classpath `hadoop classpath` *.java

jar cvf ${1}.jar *.class

hadoop jar ${1}.jar Filter sample.csv  combine/${1}

hadoop fs -cat combine/${1}/part-r-00000  > ${1}
