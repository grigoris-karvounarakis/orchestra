#!/bin/bash

BASEPARAMS="--cutoff 1024 --skip 0 --grigoris --dbalias DEFEAT --password todd9807 --username tjgreen --seed 0 --inout --relsize 9"

echo
echo Experiment 'effect of cycles'
echo

PEERS=5
FANOUT=2
COVERAGE=1.0

for CYCLES in 0 1 2 3
do
  for INS in 1000
    do
    for DFRAC in 1000
      do
      DEL=$(($INS/$DFRAC))
      FILENAME="tukwila/${PEERS}p${FANOUT}n-${CYCLES}c${COVERAGE}v-${INS}i${DEL}d"
      PARAMS="$BASEPARAMS --peers $PEERS --fanout $FANOUT --coverage $COVERAGE --mincycles $CYCLES --maxcycles $CYCLES --insertions $INS --deletions $DEL --filename $FILENAME"
      rm -f $FILENAME*
      COMMAND="python workload.py $PARAMS"
      echo $COMMAND
      $COMMAND
    done
  done
done
