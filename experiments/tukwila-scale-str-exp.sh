#!/bin/bash

source ./set-vars.sh

PARAMS="-Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.TukwilaMap -insert=true -delete=false -debug=true -fulldebug=false -outerunion=false -apply=true -workdir=c:\\trunc\\experiments\\tukwila -runs=1"

#
# Run scalability experiment
#
mkdir -p tukwila/output
rm -rf tukwila/output/scale-str.log
INS1=10000
for RUNS in 1 2 3 #4 5
do
  echo >>tukwila/output/scale-str.log
  echo "############ Run $RUNS #############" >>tukwila/output/scale-str.log
  echo >>tukwila/output/scale-str.log
  for PEERS in 10 5 2
    do
    echo >>tukwila/output/scale-str.log
    echo "------------ $PEERS peers ------------" >>tukwila/output/scale-str.log
    echo >>tukwila/output/scale-str.log
    for INS2 in 1000 100 #500 
      do
      FILENAME1="${PEERS}p0n-0c1.0v-${INS1}i0d"
      FILENAME2="${PEERS}p0n-0c1.0v-${INS2}i${INS2}d"
      CMDLINE1="$PARAMS -schema=$FILENAME1 -workload=$FILENAME1 -migrate=true"
      CMDLINE2="$PARAMS -schema=$FILENAME2 -workload=$FILENAME2 -migrate=false"
      for SUFFIX in destroy create insert delete
	do
	DB2="db2cmd /c /w /i db2 -tf tukwila/${FILENAME1}.${SUFFIX}"
	echo $DB2 >>tukwila/output/scale-str.log 2>&1
	$DB2 >>tukwila/output/scale-str.log 2>&1
      done
      echo java $CMDLINE1 >>tukwila/output/scale-str.log 2>&1
      java $CMDLINE1 >>tukwila/output/scale-str.log 2>&1

      for SUFFIX in destroy create insert delete
	do
	DB2="db2cmd /c /w /i db2 -tf tukwila/${FILENAME2}.${SUFFIX}"
	echo $DB2 >>tukwila/output/scale-str.log 2>&1
	$DB2 >>tukwila/output/scale-str.log 2>&1
      done
      echo java $CMDLINE2 >>tukwila/output/scale-str.log 2>&1
      java $CMDLINE2 >>tukwila/output/scale-str.log 2>&1
    done
  done
done
