#!/usr/bin/env bash

wl=$1
cores=$2
mkdir -p log
/root/spark-3.0.1-bin-hadoop2.7/bin/spark-submit \
  --class tg.Main \
  --master spark://172.25.187.52:7077 \
  --executor-memory 54g \
  --driver-memory 54g \
  --total-executor-cores $cores \
  --conf spark.default.parallelism=$((cores*2)) \
    DynamicTG-spark-1.0-SNAPSHOT-jar-with-dependencies.jar stock2 -c \
     --path data/stock/601990.dup \
     --wl $wl

cat out | grep -a "using" > log/$cores.$wl.out