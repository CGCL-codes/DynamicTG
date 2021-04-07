#!/usr/bin/env bash


/root/spark-3.0.1-bin-hadoop2.7/bin/spark-submit \
  --class tg.Split \
  --master spark://172.25.187.52:7077 \
  --executor-memory 54g \
  --driver-memory 54g \
    DynamicTG-spark-1.0-SNAPSHOT-jar-with-dependencies.jar "$@"
