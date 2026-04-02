#!/bin/bash

BASEPARAMS="--cutoff 4 --skip 0 --grigoris --dbalias DEFEAT --password todd9807 --username tjgreen --seed 0 --inout --relsize 9"

echo
echo Scalability experiment \(\integers\)
echo

FANOUT=0
COVERAGE=1.0
CYCLES=0

for PEERS in 20 #2 5 10
do
  INS=10000
  SKIP=0
  DEL=0

  FILENAME="tukwila/${PEERS}p${FANOUT}n-${CYCLES}c${COVERAGE}v-${INS}i${DEL}d-int"
  PARAMS="$BASEPARAMS --peers $PEERS --fanout $FANOUT --coverage $COVERAGE --mincycles $CYCLES --maxcycles $CYCLES --insertions $INS --deletions $DEL --filename $FILENAME --skip $SKIP"
  rm -f $FILENAME*
  COMMAND="python workload.py $PARAMS"
  echo $COMMAND
  $COMMAND

  for INS in 100 500 1000
  do
    DEL=0
    SKIP=20000

    FILENAME="tukwila/${PEERS}p${FANOUT}n-${CYCLES}c${COVERAGE}v-${INS}i${DEL}d-int"
    PARAMS="$BASEPARAMS --peers $PEERS --fanout $FANOUT --coverage $COVERAGE --mincycles $CYCLES --maxcycles $CYCLES --insertions $INS --deletions $DEL --filename $FILENAME --skip $SKIP"
    rm -f $FILENAME*
    COMMAND="python workload.py $PARAMS"
    echo $COMMAND
    $COMMAND
  done
done
