#!/bin/bash

# Task 1: Automated batch loading of consumption data from spool directory to HDFS

# Create spool directory and copy consumption files there
mkdir -p /home/hadoopuser/spooldir
cp ../../data/Final\ Dataset/consumption_*.txt /home/hadoopuser/spooldir/

# Compile the job
javac -classpath $(hadoop classpath) -d . *.java
jar cf phase2-task1.jar projPhase1/*.class

# Run the MapReduce job (reads from spool directory)
hadoop jar phase2-task1.jar projPhase1.ReduceJob file:///home/hadoopuser/spooldir hdfs://192.168.56.5:9820/phase2

# Mark files as completed for Flume (rename to .COMPLETED)
for file in /home/hadoopuser/spooldir/consumption_*.txt; do
    if [ -f "$file" ]; then
        mv "$file" "$file.COMPLETED"
    fi
done

echo "Task 1: Consumption data loaded from spool directory to Phase 1 structure in HDFS"
echo "Files marked as .COMPLETED to prevent Flume reprocessing"
