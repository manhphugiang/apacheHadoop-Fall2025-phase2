#!/bin/bash

# Task 1: Automated batch loading of consumption data using Phase 1 MapReduce approach

# Compile the job
javac -classpath $(hadoop classpath) DataLoaderTask1.java
jar cf dataloader.jar DataLoaderTask1*.class

# Upload consumption data to HDFS
hdfs dfs -mkdir -p /input/data
hdfs dfs -put data/Final\ Dataset/consumption_*.txt /input/data/

# Run the MapReduce job (automated batch process)
hadoop jar dataloader.jar DataLoaderTask1 /input/data /outputProject1

echo "Task 1: Consumption data loaded to Phase 1 structure in /outputProject1"
