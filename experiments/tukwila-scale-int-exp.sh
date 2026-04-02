#!/bin/bash

source ./set-vars.sh

PARAMS="-Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.TukwilaMap -insert=true -delete=false -debug=true -fulldebug=false -outerunion=false -apply=true -workdir=c:\\trunc\\experiments\\tukwila -runs=1"

#
# Run scalability experiment
#
mkdir -p tukwila/output
rm -rf tukwila/output/scale-int.log
INS1=10000
for RUNS in 1 2 3
do
  echo >>tukwila/output/scale-int.log
  echo "############ Run $RUNS #############" >>tukwila/output/scale-int.log
  echo >>tukwila/output/scale-int.log
  for PEERS in 20 #2 5 10
    do
    echo >>tukwila/output/scale-int.log
    echo "------------ $PEERS peers ------------" >>tukwila/output/scale-int.log
    echo >>tukwila/output/scale-int.log
    for INS2 in 100 500 1000
      do
      FILENAME1="${PEERS}p0n-0c1.0v-${INS1}i0d-int"
      FILENAME2="${PEERS}p0n-0c1.0v-${INS2}i0d-int"
      CMDLINE1="$PARAMS -schema=$FILENAME1 -workload=$FILENAME1 -migrate=true"
      CMDLINE2="$PARAMS -schema=$FILENAME2 -workload=$FILENAME2 -migrate=false"
      for SUFFIX in destroy create insert delete
	do
	DB2="db2cmd /c /w /i db2 -tf tukwila/${FILENAME1}.${SUFFIX}"
	echo $DB2 >>tukwila/output/scale-int.log 2>&1
	$DB2 >>tukwila/output/scale-int.log 2>&1
      done
      echo java $CMDLINE1 >>tukwila/output/scale-int.log 2>&1
      java $CMDLINE1 >>tukwila/output/scale-int.log 2>&1

      for SUFFIX in destroy create insert delete
	do
	DB2="db2cmd /c /w /i db2 -tf tukwila/${FILENAME2}.${SUFFIX}"
	echo $DB2 >>tukwila/output/scale-int.log 2>&1
	$DB2 >>tukwila/output/scale-int.log 2>&1
      done
      echo java $CMDLINE2 >>tukwila/output/scale-int.log 2>&1
      java $CMDLINE2 >>tukwila/output/scale-int.log 2>&1
    done
  done
done
