#!/bin/bash

function createCleanupCommands {
  echo "
#          clear
#           stopUSS
          drop
          quit
  " >> $scriptname
}

function createCommonVarAssgn {
  echo "
        set apply=true
        set batch=false
        set incremental=true
        set debug=false
        set wideprov=true
        set autocommit=false
        set prepare=true
        set union=false
        set indexall=true
      set rejectionTables=false
        set stratified=true

        set workdir="$schemadir"
        set schema="$dataPrefix"
        set skipFakeMappings=true
        set queryCutoff=5000000
        set transactionCutoff=500000000
#            set queryCutoff=500
#            set transactionCutoff=50000
        set runStatistics="$statistics"

        startUSS
        create-noscript
	" >> $scriptname
	if [ $testName = "join" ] || [ $testName = "ojoin" ] || [ $testName = "lojoin" ] || [ $testName = "rojoin" ] || [ $testName = "overjoin" ]
	then
	  if [ $topology -eq 2 ]
  	then
  	  i=$[ $nPeers-2 ]
  	  while [ $i -ge 0 ]
    	do
	      j=0
	  		if [ $i -gt 0 ]
	  		then
	  		  expr=$testName
	  	    while [ $j -lt $joinWidth ] && [ $i -ge 0 ]
	  		  do
	  	  		expr=$expr" M"$i
	  		  	i=$[ $i - 1 ]
	  			  j=$[ $j + 1 ]
	  		  done
	  		  echo $expr  >> $scriptname
	  		else
	  		  i=$[ $i-$joinWidth ]
	  		fi
     	done
		elif [ $topology -eq 9 ]
		then
		  if [ $nPeers -eq 30 ]
			then
			  if [ $testName = "overjoin" ]
		  	then
			    echo "join M20 M4 M0" >> $scriptname
			    echo "join M19 M4 M0" >> $scriptname
			    echo "join M21 M4 M0" >> $scriptname
			    echo "join M22 M4 M0" >> $scriptname
			    echo "join M15 M3 M0" >> $scriptname
			    echo "join M16 M3 M0" >> $scriptname
			    echo "join M17 M3 M0" >> $scriptname
			    echo "join M18 M3 M0" >> $scriptname
			    echo "join M11 M2 M0" >> $scriptname
			    echo "join M12 M2 M0" >> $scriptname
			    echo "join M13 M2 M0" >> $scriptname
			    echo "join M14 M2 M0" >> $scriptname
			    echo "join M10 M1 M0" >> $scriptname
			    echo "join M7 M1 M0" >> $scriptname
			    echo "join M8 M1 M0" >> $scriptname
			    echo "join M9 M1 M0" >> $scriptname
			    echo "join M28 M6 M0" >> $scriptname
			    echo "join M27 M6 M0" >> $scriptname
			    echo "join M23 M5 M0" >> $scriptname
			    echo "join M24 M5 M0" >> $scriptname
			    echo "join M25 M5 M0" >> $scriptname
			    echo "join M26 M5 M0" >> $scriptname
			  else
				  echo $testName" M20 M4 M0" >> $scriptname
			    echo $testName" M15 M3" >> $scriptname
			    echo $testName" M12 M2" >> $scriptname
			    echo $testName" M10 M1" >> $scriptname
			    echo $testName" M28 M6" >> $scriptname
			    echo $testName" M24 M5" >> $scriptname
				fi
			fi
  	elif [ $topology -eq 6 ]
		then
		  branchSize=$[ $nPeers/3 ]
		  i=$[ $nPeers-2-$branchSize ]
		  limit=$[ $nPeers-1-$branchSize ]
  	  while [ $i -ge 0 ]
    	do
	      j=0
	  		if [ $i -gt 0 ]
	  		then
	  		  expr=$testName
	  	    while [ $j -lt $joinWidth ] && [ $i -ge 0 ]
	  		  do
	  	  		expr=$expr" M"$i
	  		  	i=$[ $i - 1 ]
	  			  j=$[ $j + 1 ]
	  		  done
	  		  echo $expr  >> $scriptname
	  		else
	  		  i=$[ $i-$joinWidth ]
	  		fi
     	done
			i=$[ $nPeers-2 ]
  	  while [ $i -ge $limit ]
    	do
	      j=0
	  		if [ $i -gt $limit ]
	  		then
	  		  expr=$testName
	  	    while [ $j -lt $joinWidth ] && [ $i -ge $limit ]
	  		  do
	  	  		expr=$expr" M"$i
	  		  	i=$[ $i - 1 ]
	  			  j=$[ $j + 1 ]
	  		  done
	  		  echo $expr  >> $scriptname
	  		else
	  		  i=$[ $i-$joinWidth ]
	  		fi
     	done
		elif [ $topology -eq 11 ]
		then
		  if [ $testName = "overjoin" ] && [ $nPeers -eq 30 ]
			then
# I think overlapping doesn't help much in this case
			else
		    branchSize=$[ $nPeers/4 ]
		    i=$[ $nPeers-2-2*$branchSize ]
  	    while [ $i -ge 0 ]
    	  do
	        j=0
	  		  if [ $i -gt 0 ]
	  		  then
	  		    expr=$testName
	  	      while [ $j -lt $joinWidth ] && [ $i -ge 0 ]
	  		    do
	  	  		  expr=$expr" M"$i
	  		  	  i=$[ $i - 1 ]
	  			    j=$[ $j + 1 ]
	  		    done
	  		    echo $expr  >> $scriptname
	  		  else
	  		    i=$[ $i-$joinWidth ]
	  		  fi
     	  done
			  for k in 1 2
			  do
			    i=$[ $nPeers-2-$[ $k-1 ]*$branchSize ]
		      limit=$[ $nPeers-1-$k*$branchSize ]
  	      while [ $i -ge $limit ]
    	    do
	          j=0
	  		    if [ $i -gt $limit ]
	  		    then
	  		      expr=$testName
	  	        while [ $j -lt $joinWidth ] && [ $i -ge $limit ]
	  		      do
	  	  		    expr=$expr" M"$i
	  		  	    i=$[ $i - 1 ]
	  			      j=$[ $j + 1 ]
	  		      done
	  		      echo $expr  >> $scriptname
	  		    else
	  		      i=$[ $i-$joinWidth ]
	  		    fi
     	    done
			  done
			fi
		fi
  fi
  echo "
        migrate

        set workdir="$fulltestdir"
  " >> $scriptname
}

function createInitialScript {
  createCommonVarAssgn

if [ $topology -eq 11 ]
then
  middlePeer=$[ $nPeers/4 ]
else
  middlePeer=$[ $nPeers/2 ]
fi

# Create initial instance, for subsequent "incremental" experiment
  echo "
         # Compute initial
         set delete=false
         set insert=true
				 set applybaseDel=false
         set applybaseIns=false
         generateDeltaRules
         import-bulk $fulltestdir"/"$dataPrefix
         translate P0
         import-bulk $fulltestdir"/"$dataPrefix
         translate P0

         set applybaseDel=false
         set applybaseIns=true
         generateDeltaRules
     
         import-bulk $fulltestdir"/"$dataPrefix
         translate P0
				 runStats
  " >> $scriptname

         query="proqlBFS [P0.S0.P0_S0_R0] *- []"
				 runQuery
#				 query="proqlBFS [P0.S0.P0_S0_R0] *- []|EVALUATE TRUST ASSIGNING EACH|DEFAULT SET 1"
#				 runQuery
#				 query="proqlBFS [P0.S0.P0_S0_R0] *- []|EVALUATE TRUST ASSIGNING EACH|DEFAULT SET CAST (1 AS SMALLINT)"
#				 runQuery
#				 query="proqlBFS [P0.S0.P0_S0_R0] *- []|EVALUATE TRUST ASSIGNING EACH|DEFAULT SET CAST (1 AS DECIMAL(1))"
#				 runQuery
#				 query="proqlBFS [P0.S0.P0_S0_R0] *- []|EVALUATE TRUST ASSIGNING EACH|DEFAULT SET DECIMAL(1,1,0)"
#				 runQuery
				 query="proqlBFS [P"$middlePeer".S"$middlePeer".P"$middlePeer"_S"$middlePeer"_R0] *- []"
				 runQuery
#				 query="proqlBFS [P"$middlePeer".S"$middlePeer".P"$middlePeer"_S"$middlePeer"_R0] *- []|EVALUATE TRUST ASSIGNING EACH|DEFAULT SET 1"
#				 runQuery
       
  createCleanupCommands
}

function runQuery {
  echo "#"$query >> $scriptname
#runs=$maxRuns
runs=7
  run=1
  while [ $run -le $runs ]
    do
  echo $query >> $scriptname
	    run=$[ $run + 1 ]
  done
}

function setFileNames {
  if [ $testName = "join" ] || [ $testName = "rojoin" ] || [ $testName = "ojoin" ] || [ $testName = "lojoin" ] || [ $testName = "overjoin" ]
  then
    namePrefix=$nPeers'p-topo'$topology'-red'$redundancy'-modl'$modlocal'-'$testName$joinWidth
    dataPrefix=$nPeers'p-topo'$topology'-red'$redundancy'-modl'$modlocal
	else
    namePrefix=$nPeers'p-topo'$topology'-red'$redundancy'-modl'$modlocal
    dataPrefix=$nPeers'p-topo'$topology'-red'$redundancy'-modl'$modlocal
	fi
# Create output file
  outfile=$outputsPath"/"$dataType"/"$initialData"/"$namePrefix".out"
  rm -rf $outfile
# Create Orchestra batch files for $dataType
  mkdir -p $scriptsPath"/"$dataType"/"$initialData
  scriptname=$wd"/"$scriptsPath"/"$dataType"/"$initialData"/test-"$namePrefix
  rm -rf $scriptname
  fulltestdir=$fullwd"/"$testsPath"/autogen/"$dataType"/"$initialData"/"$dataPrefix
  schemadir=$fullwd"/"$testsPath"/autogen/"$dataType"/"$initialData"/"
}

function runScript {
# Run Orchestra on these batch files
  params=" -classpath ${cp} edu.upenn.cis.orchestra.console.Console " 
  cmd="java -Xms1024m -Xmx1024m $params -batch "$scriptname
  echo "#PEERS: " $namePrefix >>$outfile 2>&1
  echo "===========" >>$outfile 2>&1
  echo $scriptname >>$outfile 2>&1
  echo "RUN: " $scriptname
  $cmd >>$outfile 2>&1
    
  #$parsescript $outfile
}

function runExp {
  redundancy=1
  echo "TOPOLOGY: "$topology
  echo "MODLOCAL: "$modlocal
  
	setFileNames
  createInitialScript
  runScript
}

source ./set-vars-proql.sh $1 $2 $3 $4 $5 $6

mkdir -p $scriptsPath

topology=$7
modlf=$8
joinWidth=$9

initialData=$baseDataIncrement
while [ $initialData -le $maxBaseData ]
do
  if [ $dataType = "integer" ] 
  then
    statistics="false"
  else
    statistics="true"
  fi

  mkdir -p $outputsPath"/"$dataType"/"$initialData
  nPeers=$initialPeers
	while [ $nPeers -le $maxPeers ]
	do
		echo $nPeers
		if [ $[$nPeers % $modlf] -ne 0 ]
	  then
	    modlocal=$[ $[ $nPeers/$modlf ] + 1 ]
	  else
	    modlocal=$[ $nPeers/$modlf ]
	  fi
    runExp

		nPeers=$[ $nPeers + $initialPeers ]
		  echo $nPeers
	done
  initialData=$[ $initialData + $baseDataIncrement ]
done

