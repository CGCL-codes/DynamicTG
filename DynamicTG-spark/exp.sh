#!/usr/bin/env bash

step=108000000
cores="80 40 20 10"
for c in $cores;do
  for i in {1..8} ; do
      wl=$(($step * $i))
     timeout -s 9 3600s bash run.sh $wl $c
  done
done