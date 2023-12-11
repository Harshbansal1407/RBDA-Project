#!/bin/bash


### Write dataset on HDFS
hadoop fs -mkdir ProjectData
hadoop fs -put DOT_Traffic_Speeds_NBE_20231125.csv ProjectData/traffic_speed.csv


### Get subset (random sampling) for initial analysis

# Create jar file
javac -classpath `hadoop classpath` RandomSampling*.java
jar cvf randomSampler.jar RandomSampling*.class
# Create hdfs folder to store data
hadoop fs -mkdir RandomSampler
# Execute map-reduce job
hadoop jar randomSampler.jar ProjectData/traffic_speed.csv RandomSampler/output



### Word Count Profiling

# Create jar file
javac -classpath `hadoop classpath` ProjectWordCount*.java
jar cvf wordCounter.jar ProjectWordCount*.class
# Create hdfs folder to store data
hadoop fs -mkdir WordCounter
# Execute map-reduce job for "BOROUGH" column
hadoop jar wordCounter.jar ProjectData/traffic_speed.csv WordCounter/output1 11
# Execute map-reduce job for "STATUS" column
hadoop jar wordCounter.jar ProjectData/traffic_speed.csv WordCounter/output2 3



### Statistical Anlysis Profiling

# Create jar file
javac -classpath `hadoop classpath` StatisticalAnalysis*.java
jar cvf statAnalysis.jar StatisticalAnalysis*.class
# Create hdfs folder to store data
hadoop fs -mkdir StatAnalysis
# Execute map-reduce job for "SPEED" column
hadoop jar statAnalysis.jar ProjectData/traffic_speed.csv StatAnalysis/output1 1
# Execute map-reduce job for "TIME_TAKEN" column
hadoop jar statAnalysis.jar ProjectData/traffic_speed.csv StatAnalysis/output2 2



### Final job to Pre-process the data

# Create jar file
javac -classpath `hadoop classpath` PreProcessor*.java
jar cvf preProcessor.jar PreProcessor*.class
# Create hdfs folder to store data
hadoop fs -mkdir PreProcessor
# Execute map-reduce job
hadoop jar preProcessor.jar ProjectData/traffic_speed.csv PreProcessor/output

