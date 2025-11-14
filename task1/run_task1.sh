#!/bin/bash

# Task 1: Automated batch loading of consumption data from spool directory to HDFS

# create spool directory and move initial file into first so that hadoop have data to process as a batch
# mkdir -p /home/hadoopuser/flume/spooldir


# make sure to build the artifact first

# run the commnad MapReduce job (reads from spool directory) 
hadoop jar home/hadoop-user/phase2-task1.jar file:///home/hadoopuser/flume/spooldir hdfs://192.168.56.5:9820/projectPhase2 
# your jar file location can be different
# your output can be different
# your initial file path can be different


# mark files as completed for Flume (rename to .COMPLETED) so that flume do not pick up the files that already been written to hdfs by Hadoop
for file in /home/hadoopuser/spooldir/consumption_*.txt; do
    if [ -f "$file" ]; then
        mv "$file" "$file.COMPLETED"
    fi
done

echo "Task 1: Consumption data loaded from spool directory to Phase 1 structure in HDFS"
echo "Files marked as .COMPLETED to prevent Flume reprocessing"
