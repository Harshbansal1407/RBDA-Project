#!/bin/bash

# Removing output dir
echo "Removing output dir"
hadoop fs -rm -r project/output

# Compiling & Running
echo "Compiling & Running job"
javac -classpath $(hadoop classpath) *.java

jar cvf dataClean.jar *.class

hadoop jar dataClean.jar DataCleaning project/motorVehicleCollisions project/output

#hadoop fs -get project/output/part-r-00000
#hadoop fs -get project/output/data_profiling_count/part-r-00000
hadoop fs -get project/output/data_profiling_value/part-r-00000

# Delete files
#echo "Deleting source code files"
#rm *.java *.class *.jar