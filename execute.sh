#!/bin/bash

DIR="$(cd "`dirname "$0"`"; pwd)"
JAR=target/scala-2.10/btworld_2.10-0.1.jar

MASTER=$1
APPLICATION="Workflow"

function exit_with_usage {
  echo "Usage:"
  echo "./execute.sh sparkMaster"
  echo ""
  exit 1
}


if [ -d $MASTER ]; then
   exit_with_usage
fi

HDFS=hdfs://$MASTER.ib.cluster:54321

INPUT=$HDFS/Scrapes/btworld/*
OUTPUT=$HDFS/results
DIR=$HDFS/checkpoint

$HADOOP_HOME/bin/hdfs dfs -rmr $OUTPUT $DIR

$SPARK_HOME/bin/spark-submit --class "$APPLICATION" $JAR $INPUT $OUTPUT $DIR\
                   --master spark://$MASTER.cm.cluster:7077 \
                   --deploy-mode cluster \
                   --driver-memory 10g
