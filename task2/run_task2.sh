#!/bin/bash

# Task 2: Real-time streaming pipeline for consumption data

# Create spool directory for incoming consumption files
mkdir -p /tmp/spool_directory
mkdir -p /tmp/spool_directory/.flume

# Start Flume agent for streaming consumption data
flume-ng agent \
  --conf-file task2-streaming.conf \
  --name agent \
  --conf $FLUME_HOME/conf \
  -Dflume.root.logger=INFO,console

echo "Task 2: Streaming pipeline started for consumption data"
echo "Place new consumption_*.txt files in /tmp/spool_directory to stream to HDFS"
