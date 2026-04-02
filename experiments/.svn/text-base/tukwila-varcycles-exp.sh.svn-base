#!/bin/bash

source ./set-vars.sh

PARAMS="-Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.TukwilaMap -insert=true -delete=false -debug=true -fulldebug=false -outerunion=false -migrate=true -apply=true -workdir=c:\\trunc\\experiments\\tukwila -runs=5"

#
# Run 'varying number of cycles' experiment
#
mkdir -p tukwila/output
for CYCLES in 0 1 2 3
do
  FILENAME="5p2n-${CYCLES}c1.0v-1000i1d"
  CMDLINE="$PARAMS -schema=$FILENAME -workload=$FILENAME"
  for SUFFIX in destroy create insert delete
    do
    DB2="db2cmd /c /w /i db2 -tf tukwila/${FILENAME}.${SUFFIX}"
    echo $DB2
    $DB2
  done
  echo "java $CMDLINE >&tukwila/output/${FILENAME}.log"
  java $CMDLINE >&tukwila/output/${FILENAME}.log
done
