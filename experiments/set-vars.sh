#!/bin/bash

schemapath=${SHARQ_HOME}"\experiments\tests\\"$1

cp=${SHARQ_HOME}"\engine\target\classes;"${SHARQ_HOME}"\engine\target\test-classes;"${SHARQ_HOME}"\engine\target\dependency"

cd ${SHARQ_HOME}"\engine\target\dependency"

for i in *.jar
do
#	echo $i
	export cp=$cp";${SHARQ_HOME}\engine\target\dependency\\"$i
done
cd ${SHARQ_HOME}"\experiments"

echo ${schemapath}

cp=$cp";."

echo $cp

#testName=inc
#testName=noninc
#testName=dred
testName=$1

#dataType=integer
#dataType=string
dataType=$2

#baseDataIncrement=2000
baseDataIncrement=$3

#maxBaseData=10000
#maxBaseData=2000
maxBaseData=$4

maxPeersFor10kInt=20
maxPeersFor10kStr=8
maxPeersFor10kBidir=6

if [ $dataType = "integer" ] || [ $dataType = "cycles" ]
then
  maxPeersFor10kInt=$6
elif [ $dataType = "string" ]
then
  maxPeersFor10kStr=$6
elif [ $dataType = "bidir" ]
then
  maxPeersFor10kBidir=$6
fi


# Fixed peers != 0 runs experiments for different data sizes for the specified number of peers. Otherwise, the number of peers is decreased as the base size increases
#fixedPeers=0
#fixedPeers=10
fixedPeers=$5

updateIncr=$7

maxRuns=5
scriptsPath="orchestraScripts/tods"
outputsPath="outputs/tods"
testsPath="tests/tods"

wd=`pwd`
wd=`cygpath -alw ${wd}`
#fullwd="/cygwin"$wd
fullwd=$wd

parsescript=$outputsPath"/parse-command"
