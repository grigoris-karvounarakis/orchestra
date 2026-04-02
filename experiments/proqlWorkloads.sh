#!/bin/bash

source ./set-vars-proql.sh $1 $2 $3 $4 $5 $6

# PARAMS="-Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.TukwilaMap -insert=true -delete=false -debug=true -fulldebug=false -outerunion=false -apply=true -workdir=c:\\trunc\\experiments\\tukwila -runs=1"

# allParams="--filename 6p1n-0c1v-1000i10d --peers 6 --coverage 1 --cutoff 8 --relsize 15 --dbalias EXPERIM --username orchestra --password apollo13! --seed 2 --skip 0 --olivier --inout --insertions 1000 --deletions 10 "
commonParams=" --dbalias EXPERIM --username orchestra --password apollo13! --seed 2 --inout --olivier --noreject --nolocal --relsize 15 "

noNulls=" --coverage 1 "
noCycles="  --mincycles 0 --maxcycles 0 "
stringData=" --cutoff 128 "
intData=" --cutoff 4 "
fanout=10
#nCycles=0
#coverage=1
#cutoff=8
#topology 0-7
#redundancy


function createWorkload {
  cmd="$cmdparams --peers $nPeers"
  skip=0

#  echo $cmd
  # Create initial data
  namePrefix=$nPeers'p-topo'$topology'-red'$redundancy'-modl'$modlocal
  subdir=$outputPath'/'$nPeers'p-topo'$topology'-red'$redundancy'-modl'$modlocal
  echo "GENERATE: --filename $namePrefix --insertions $initialData --deletions 0 --skip $skip $bidir --topology $topology --redundancy $redundancy --modlocal $modlocal"

  $cmd --filename $namePrefix --peers $nPeers --insertions $initialData --deletions 0 --skip $skip $bidir --topology $topology --redundancy $redundancy --modlocal $modlocal
  mkdir -p $subdir
  mv $namePrefix'.schema' $outputPath
  mv $namePrefix'.insert' $subdir
	mv $namePrefix'.'*'INS' $subdir
  rm $namePrefix'.'*
}

function createAllDataSizes {
temp=$initialData

while [ $initialData -le $maxBaseData ]
do
#  for dataType in $1 $2
  #for i in scalabilityInt scalabilityString
#  do
    echo "Create $dataType workloads for $initialData base data"
		if [ $dataType = "integer" ] 
		then
      cmdparams="java -classpath ${cp} edu.upenn.cis.orchestra.workloadgenerator.Generator $params $intData"
		elif [ $dataType = "string" ] 
		then
      cmdparams="java -classpath ${cp} edu.upenn.cis.orchestra.workloadgenerator.Generator $params $stringData"
    else
      echo "Invalid dataType $dataType"
    fi

    outputPath=$testsPath"/autogen/"$dataType"/"$initialData
    mkdir -p $outputPath
  
    nPeers=$initialPeers
    while [ $nPeers -le $maxPeers ]
    do
		  if [ $[$nPeers % $modlf] -ne 0 ]
	  	then
	      modlocal=$[ $[ $nPeers/$modlf ] + 1 ]
	  	else
	      modlocal=$[ $nPeers/$modlf ]
	  	fi
      createWorkload

      nPeers=$[ $nPeers + $initialPeers ]
    done
  initialData=$[ $initialData + $baseDataIncrement ]
done
initialData=$temp
}

initialData=$baseDataIncrement
params="$commonParams $noNulls $noCycles"

redundancy=1
topology=$7
modlf=$8

createAllDataSizes
