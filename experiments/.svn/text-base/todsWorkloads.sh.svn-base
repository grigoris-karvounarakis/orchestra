#!/bin/bash

source ./set-vars.sh $1 $2 $3 $4 $5 $6 $7

# PARAMS="-Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.TukwilaMap -insert=true -delete=false -debug=true -fulldebug=false -outerunion=false -apply=true -workdir=c:\\trunc\\experiments\\tukwila -runs=1"

# allParams="--filename 6p1n-0c1v-1000i10d --peers 6 --coverage 1 --cutoff 8 --relsize 15 --dbalias EXPERIM --username orchestra --password apollo13! --seed 2 --skip 0 --olivier --inout --insertions 1000 --deletions 10 "
commonParams=" --dbalias EXPERIM --username orchestra --password apollo13! --seed 2 --inout --olivier --noreject --relsize 15 "
commonParamsPy=" --dbalias EXPERIM --username orchestra --password apollo13! --seed 2 --olivier --relsize 15 --xmlformat "

noNulls=" --coverage 1 "
noCycles="  --mincycles 0 --maxcycles 0 "
chain=" --fanout 0 "
oneBranch=" --fanout 1 "
stringData=" --cutoff 1024 "
intData=" --cutoff 4 "
#fanout=1
#nCycles=0
#coverage=1
#cutoff=8

function incrWorkloads {
  if [ $testName = "inc" ] || [ $testName = "incdel" ] || [ $testName = "se-nse" ] 
  then
    del=$ins
    skip=0
    namePrefix=$nPeers'p-'$ins'i'$del'd'
    echo "GENERATE: --filename $namePrefix --insertions $initialData --deletions $del --skip $skip"
    $cmd --filename $namePrefix --insertions $initialData --deletions $del --skip $skip
    mkdir -p $subdir'/incr-deletions'
    mv $namePrefix'.delete' $subdir'/incr-deletions'
    touch $subdir'/incr-deletions/'$namePrefix'.insert'
    mv $namePrefix'.'*'DEL' $subdir'/incr-deletions'
    rm $namePrefix'.'*
  fi

  if [ $testName = "inc" ] || [ $testName = "incins" ]
  then
    del=0
    skip=$[ $initialData*$nPeers ]
    namePrefix=$nPeers'p-'$ins'i'$del'd'
    echo "GENERATE: --filename $namePrefix --insertions $ins --deletions $del --skip $skip"
    $cmd --filename $namePrefix --insertions $ins --deletions $del --skip $skip
    mkdir -p $subdir'/incr-insertions'
    mv $namePrefix'.insert' $subdir'/incr-insertions'
    touch $subdir'/incr-insertions/'$namePrefix'.delete'
    mv $namePrefix'.'*'INS' $subdir'/incr-insertions'
    rm $namePrefix'.'*
  fi
}

function createWorkload {
  cmd="$cmdparams --peers $nPeers"
  skip=0

  echo $cmd
  # Create initial data
  namePrefix=$nPeers'p'
  subdir=$outputPath'/'$nPeers'p'
  echo "GENERATE: --filename $namePrefix --insertions $initialData --deletions 0 --skip $skip $bidir"
  $cmd --filename $namePrefix --peers $nPeers --insertions $initialData --deletions 0 --skip $skip $bidir
  mkdir -p $subdir
  mkdir -p $subdir'/initial'
  mv $namePrefix'.schema' $outputPath
  mv $namePrefix'.insert' $subdir'/initial'
  touch $subdir'/initial/'$namePrefix'.delete'
  mv $namePrefix'.'*'INS' $subdir'/initial'
  rm $namePrefix'.'*
  
  if [ $testName != "initial" ]
  then 
# Incremental insertions/deletions
    if [ $updateIncr -eq 0 ]
    then
      for ins in $[ $initialData/100 ] $[ $initialData/10 ] $[ $initialData/2 ]
      do
        incrWorkloads
      done
    else
      ins=$[ $initialData/$updateIncr ]
      while [ $ins -le $initialData ] 
      do
        incrWorkloads
        ins=$[ $ins + $initialData/$updateIncr]
      done      
    fi
  fi
}

function createAllDataSizes {
temp=$initialData
while [ $initialData -le $maxBaseData ]
do
#  for dataType in $1 $2
  #for i in scalabilityInt scalabilityString
#  do
    echo "Create $dataType workloads for $initialData base data"
    bidir=""
    if [ $dataType = "bidir"  ]
    then
      bidir="--bidir"
      maxPeers=$[ $maxPeersFor10kBidir*10000/$initialData ]
      if [ $testName = "se-nse" ]
			then
        peersIncrement=2
			else
        peersIncrement=$[ $maxPeers/$maxPeersFor10kBidir ]
			fi
      cmdparams="java -classpath ${cp} edu.upenn.cis.orchestra.workloadgenerator.Generator $params $intData"
    elif [ $dataType = "integer" ] || [ $dataType = "cycles" ]
    then
      maxPeers=$[ $maxPeersFor10kInt*10000/$initialData ]
      if [ $testName = "initial" ]
      then
        peersIncrement=$[ $maxPeers/$maxPeersFor10kInt ]
      else
#        peersIncrement=$[ 2*$maxPeers/$maxPeersFor10kInt ]
        peersIncrement=$[ $maxPeers/5 ]
      fi
      if [ $testName = "incdel" ]
      then
        echo "Use python workload generator"
        cmdparams="python workload.py $params $intData --todd"
      else
        cmdparams="java -classpath ${cp} edu.upenn.cis.orchestra.workloadgenerator.Generator $params $intData"
      fi
    elif [ $dataType = "string" ] 
    then
      maxPeers=$[ $maxPeersFor10kStr*10000/$initialData ]
      peersIncrement=$[ $maxPeers/$maxPeersFor10kStr ]
      if [ $testName = "incdel" ]
      then
        echo "Use python workload generator"
        cmdparams="python workload.py $params $stringData --todd"
      else
        cmdparams="java -classpath ${cp} edu.upenn.cis.orchestra.workloadgenerator.Generator $params $stringData"
      fi
    else
      echo "Invalid dataType $dataType"
    fi
    
    if [ $dataType = "cycles" ]
    then
      outputPath=$testsPath"/autogen/"$dataType"/"$numCycles"-cycles/"$initialData
    else
      outputPath=$testsPath"/autogen/"$dataType"/"$initialData
    fi

#    if [ $[ $peersIncrement%2 ] -ne 0 ] && [ $[ $peersIncrement%5 ] -ne 0 ]
#    then
#      peersIncrement=2
#    fi
  
#    rm -rf $outputPath
    mkdir -p $outputPath
  
    if [ $fixedPeers -eq 0 ]
    then
      nPeers=$peersIncrement
  #    for nPeers in 2 4 6 8 10 12 14 16 18 20
      while [ $nPeers -le $maxPeers ]
      do
        createWorkload

        nPeers=$[ $nPeers + $peersIncrement ]
      done
    else
      nPeers=$fixedPeers
      createWorkload
    fi
#  done
  initialData=$[ $initialData + $baseDataIncrement ]
done
initialData=$temp
}

initialData=$baseDataIncrement

oneCycles="  --mincycles $numCycles --maxcycles $numCycles "
oneCycles="  --mincycles 1 --maxcycles 1 "
twoCycles="  --mincycles 2 --maxcycles 2 "
oneBranch=" --fanout 1 "

if [ $dataType = "cycles" ]
then
  numCycles=0
  while [ $numCycles -le 2 ]
  do
    params="$commonParams $noNulls --mincycles $numCycles --maxcycles $numCycles --fanout $numCycles "
    createAllDataSizes
    initialData=$baseDataIncrement
    numCycles=$[ $numCycles + 1 ]
  done
else
  if [ $testName = "incdel" ]
  then
    params="$commonParamsPy $noNulls $noCycles $chain"
  else
    params="$commonParams $noNulls $noCycles $chain"
  fi
  createAllDataSizes
fi
