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
  " >> $scriptname

	if [ $dataType = "bidir" ]
	then
	  echo "
# bidir needs these!
        set rejectionTables=true
  " >> $scriptname
  else
	  echo "
      set rejectionTables=false
  " >> $scriptname
	fi

  echo "
        set stratified=true

        set workdir="$schemadir"
        set schema="$nPeers"p
        set skipFakeMappings=true
        set queryCutoff=5000000
        set transactionCutoff=500000000
#            set queryCutoff=500
#            set transactionCutoff=50000
        set runStatistics="$statistics"

        startUSS
        create-noscript
        migrate

        set workdir="$fulltestdir"initial
  " >> $scriptname
}

function createInitialScript {
  if [ $dataType = "bidir" ]
  then
    echo "
      set bidirectional=true
    " >> $scriptname
  fi

  createCommonVarAssgn

  echo "
          # Compute initial
          set delete=false
          set insert=true
          set applybaseDel=false
          set applybaseIns=false
          generateDeltaRules" >> $scriptname

# Run "initial" experiment, i.e., cost of creating initial instance
      run=2
  while [ $run -le $maxRuns ]
    do
        echo "
       import-bulk "$fulltestdir"initial/"$nPeers"p
       translate P0
    " >> $scriptname
    run=$[ $run + 1 ]
  done

# Actually create initial instance, for subsequent "incremental" experiment
  echo "
         set applybaseDel=false
         set applybaseIns=true
         generateDeltaRules
     
         import-bulk "$fulltestdir"initial/"$nPeers"p
         translate P0
       
  " >> $scriptname

  createCleanupCommands
}

function createIncScript {

  createCommonVarAssgn

  echo "
          # Compute initial
          set delete=false
          set insert=true
          set applybaseDel=false
          set applybaseIns=false
          generateDeltaRules" >> $scriptname

# Run "initial" experiment, i.e., cost of creating initial instance
      run=2
  while [ $run -le $maxRuns ]
    do
        echo "
       import-bulk "$fulltestdir"initial/"$nPeers"p
       translate P0
    " >> $scriptname
    run=$[ $run + 1 ]
  done

# Actually create initial instance, for subsequent "incremental" experiment
  echo "
         set applybaseDel=false
         set applybaseIns=true
         generateDeltaRules
     
         import-bulk "$fulltestdir"initial/"$nPeers"p
         translate P0
       
           set runStatistics="$statistics"
  " >> $scriptname

  if [ $testName = "inc" ] || [ $testName = "incdel" ]
	then
    echo "
           # Incremental deletions
           set delete=true
           set insert=false
           set applybaseDel=false
           set applybaseIns=false
           generateDeltaRules
       
           set workdir="$fulltestdir"incr-deletions
       
    " >> $scriptname

    # Incremental deletions
    incrDeletionRuns
	fi

  if [ $testName = "inc" ] || [ $testName = "incins" ]
	then
  echo "
         # Incremental insertions
         set delete=false
         set insert=true
         set applybaseDel=false
         set applybaseIns=false
         generateDeltaRules

         set workdir="$fulltestdir"incr-insertions

  " >> $scriptname

    if [ $updateIncr -eq 0 ]
    then
# Incremental insertions
      for nUpd in $[ $initialData/100 ] $[ $initialData/10 ] $[ $initialData/2 ]
      do
        run=1
        while [ $run -le $maxRuns ]
        do
          echo "
             import-bulk "$fulltestdir"incr-insertions/"$nPeers"p-"$nUpd"i"0"d 
             translate P0
          " >> $scriptname
          run=$[ $run + 1 ]
        done
      done
		else
		  nUpd=$[ $initialData/$updateIncr ]
		  while [ $nUpd -le $initialData ] 
			do
        run=1
        while [ $run -le $maxRuns ]
        do
          echo "
             import-bulk "$fulltestdir"incr-insertions/"$nPeers"p-"$nUpd"i"0"d 
             translate P0
          " >> $scriptname
          run=$[ $run + 1 ]
        done
				nUpd=$[ $nUpd + $initialData/$updateIncr]
      done
    fi
  fi

  createCleanupCommands
}

function createNonIncScript {

  createCommonVarAssgn

  echo "
     
        # Compute initial
        set delete=false
        set insert=true
        set applybaseDel=false
        set applybaseIns=false
        generateDeltaRules" >> $scriptname

# Non-incremental deletions
  if [ $updateIncr -eq 0 ]
  then
    for nUpd in $[ $initialData/100 ] $[ $initialData/10 ] $[ $initialData/2 ]
    do
      run=1
      while [ $run -le $maxRuns ]
      do
        echo "
# Non-incremental deletions
          set workdir="$fulltestdir"initial
          import-bulk "$fulltestdir"initial/"$nPeers"p
          set workdir="$fulltestdir"incr-deletions
          import-bulk "$fulltestdir"incr-deletions/"$nPeers"p-"$nUpd"i"$nUpd"d 
          subtractLInsDel
          translate P0
        " >> $scriptname
        run=$[ $run + 1 ]
      done
    done
	else
		nUpd=$[ $initialData/$updateIncr ]
	  while [ $nUpd -le $initialData ] 
		do
      run=1
      while [ $run -le $maxRuns ]
      do
        echo "
# Non-incremental deletions
          set workdir="$fulltestdir"initial
          import-bulk "$fulltestdir"initial/"$nPeers"p
          set workdir="$fulltestdir"incr-deletions
          import-bulk "$fulltestdir"incr-deletions/"$nPeers"p-"$nUpd"i"$nUpd"d 
          subtractLInsDel
          translate P0
        " >> $scriptname
        run=$[ $run + 1 ]
      done
      nUpd=$[ $nUpd + $initialData/$updateIncr]
		done
	fi

# Non-incremental insertions
  if [ $updateIncr -eq 0 ]
  then
    for nUpd in $[ $initialData/100 ] $[ $initialData/10 ] $[ $initialData/2 ]
    do
      run=1
      while [ $run -le $maxRuns ]
      do
        echo "
# Non-incremental insertions
          set workdir="$fulltestdir"initial
          import-bulk "$fulltestdir"initial/"$nPeers"p
          set workdir="$fulltestdir"incr-insertions
          import-bulk "$fulltestdir"incr-insertions/"$nPeers"p-"$nUpd"i"0"d 
          translate P0
        " >> $scriptname
        run=$[ $run + 1 ]
      done
    done
  else
		nUpd=$[ $initialData/$updateIncr ]
	  while [ $nUpd -le $initialData ] 
		do
      run=1
      while [ $run -le $maxRuns ]
      do
        echo "
# Non-incremental insertions
          set workdir="$fulltestdir"initial
          import-bulk "$fulltestdir"initial/"$nPeers"p
          set workdir="$fulltestdir"incr-insertions
          import-bulk "$fulltestdir"incr-insertions/"$nPeers"p-"$nUpd"i"0"d 
          translate P0
        " >> $scriptname
        run=$[ $run + 1 ]
      done
			nUpd=$[ $nUpd + $initialData/$updateIncr]
		done
	fi

  createCleanupCommands
}

function createDRedScript {

  createCommonVarAssgn

  echo "
    
        # Compute initial
        set delete=false
        set insert=true
       set applybaseDel=false
       set applybaseIns=true
       generateDeltaRules
   
       import-bulk "$fulltestdir"initial/"$nPeers"p
       translate P0
     
         # DRed deletions
         set delete=true
         set insert=false
         set applybaseDel=false
         set applybaseIns=false
         set dred=true
         generateDeltaRules
     
         set workdir="$fulltestdir"incr-deletions
     
    " >> $scriptname

  incrDeletionRuns

  createCleanupCommands
}

function incrDeletionRuns {
# Incremental deletions
  if [ $updateIncr -eq 0 ]
  then
    for nUpd in $[ $initialData/100 ] $[ $initialData/10 ] $[ $initialData/2 ]
    do
      run=1
      while [ $run -le $maxRuns ]
      do
        echo "
         import-bulk "$fulltestdir"incr-deletions/"$nPeers"p-"$nUpd"i"$nUpd"d 
         translate P0
        " >> $scriptname
        run=$[ $run + 1 ]
      done
    done
	else
    nUpd=$[ $initialData/$updateIncr ]
    while [ $nUpd -le $initialData ] 
    do
      run=1
      while [ $run -le $maxRuns ]
      do
        echo "
         import-bulk "$fulltestdir"incr-deletions/"$nPeers"p-"$nUpd"i"$nUpd"d 
         translate P0
        " >> $scriptname
       run=$[ $run + 1 ]
      done
			nUpd=$[ $nUpd + $initialData/$updateIncr]
    done
	fi
}

function createSeNseScript {

  createCommonVarAssgn

  echo "
       # Compute initial
       set delete=false
       set insert=true
       set bidirectional=true
       set applybaseDel=false
       set applybaseIns=true
       generateDeltaRules
   
       import-bulk "$fulltestdir"initial/"$nPeers"p
       translate P0
    " >> $scriptname
     
  echo "
       # Deletions allowing side effects
       set delete=true
       set insert=false
       set applybaseDel=false
       set applybaseIns=false
       set allowSideEffects=true
       generateDeltaRules
   
       set workdir="$fulltestdir"incr-deletions
    " >> $scriptname

# Deletions allowing side effects
  incrDeletionRuns

	echo "
       # Deletions not allowing side effects
       set delete=true
       set insert=false
       set applybaseDel=false
       set applybaseIns=false
       set allowSideEffects=false
       generateDeltaRules
   
       set workdir="$fulltestdir"incr-deletions
    " >> $scriptname

# Deletions not allowing side effects
  incrDeletionRuns

  createCleanupCommands
}

function setFileNames {
  if [ $dataType = "cycles" ]
	then
# Create output file
    mkdir -p $outputsPath"/"$dataType"/"$numCycles"-cycles/"$initialData"/"
    outfile=$outputsPath"/"$dataType"/"$numCycles"-cycles/"$initialData"/"$nPeers"p-"$testName".out"
    rm -rf $outfile
# Create Orchestra batch files for $dataType
    mkdir -p $scriptsPath"/"$dataType"/"$numCycles"-cycles/"$initialData
    scriptname=$wd"/"$scriptsPath"/"$dataType"/"$numCycles"-cycles/"$initialData"/test-"$nPeers"p-"$testName
    rm -rf $scriptname
    fulltestdir=$fullwd"/"$testsPath"/autogen/"$dataType"/"$numCycles"-cycles/"$initialData"/"$nPeers"p/"
    schemadir=$fullwd"/"$testsPath"/autogen/"$dataType"/"$numCycles"-cycles/"$initialData"/"
    mkdir -p $fulltestdir
	else
# Create output file
    outfile=$outputsPath"/"$dataType"/"$initialData"/"$nPeers"p-"$testName".out"
    rm -rf $outfile
# Create Orchestra batch files for $dataType
    mkdir -p $scriptsPath"/"$dataType"/"$initialData
    scriptname=$wd"/"$scriptsPath"/"$dataType"/"$initialData"/test-"$nPeers"p-"$testName
    rm -rf $scriptname
    fulltestdir=$fullwd"/"$testsPath"/autogen/"$dataType"/"$initialData"/"$nPeers"p/"
    schemadir=$fullwd"/"$testsPath"/autogen/"$dataType"/"$initialData"/"
	fi
}

function runScript {
# Run Orchestra on these batch files
  params=" -classpath ${cp} edu.upenn.cis.orchestra.console.Console " 
  cmd="java -Xms768m -Xmx768m $params -batch "$scriptname
  echo "#PEERS: " $nPeers >>$outfile 2>&1
  echo "===========" >>$outfile 2>&1
  echo $scriptname >>$outfile 2>&1
  echo "RUN: " $scriptname
  $cmd >>$outfile 2>&1
    
  $parsescript $outfile
}

function runExp {
  if [ $testName = "inc" ] || [ $testName = "incins" ] || [ $testName = "incdel" ] 
  then
    setFileNames
    createIncScript
    runScript
  elif [ $testName = "noninc" ] || [ $testName = "nonincdel" ] 
  then
    setFileNames
    createNonIncScript
    runScript
  elif [ $testName = "dred" ] || [ $testName = "dreddel" ] 
  then
    setFileNames
    createDRedScript
    runScript
  elif [ $testName = "se-nse" ] 
  then
    setFileNames
    createSeNseScript
    runScript
  elif [ $testName = "initial" ] 
  then
	  if [ $dataType = "cycles" ]
		then
	  	numCycles=0
			while [ $numCycles -le 2 ]
			do
      setFileNames
      createInitialScript
      runScript
      numCycles=$[ $numCycles + 1 ]
			done
		else
      setFileNames
      createInitialScript
      runScript
		fi
  else
    echo "Invalid test name $testName"
  fi
}

source ./set-vars.sh $1 $2 $3 $4 $5 $6 $8

mkdir -p $scriptsPath

initialPeers=$7

initialData=$baseDataIncrement
while [ $initialData -le $maxBaseData ]
do
#  for dataType in $1 $2
  #for dataType in scalabilityInt scalabilityString
#  do
    if [ $dataType = "bidir"  ]
    then
      bidir="--bidir"
      statistics="false"
      maxPeers=$[ $maxPeersFor10kBidir*10000/$initialData ]
			if [ $testName = "se-nse" ]
			then
        peersIncrement=2
			else
        peersIncrement=$[ $maxPeers/$maxPeersFor10kBidir ]
			fi
    elif [ $dataType = "integer" ] || [ $dataType = "cycles" ]
    then
      statistics="false"
      maxPeers=$[ $maxPeersFor10kInt*10000/$initialData ]
      if [ $testName = "initial" ]
      then
        peersIncrement=$[ $maxPeers/$maxPeersFor10kInt ]
      else
#        peersIncrement=$[ 2*$maxPeers/$maxPeersFor10kInt ]
        peersIncrement=$[ $maxPeers/5 ]
      fi
    else
      statistics="true"
      maxPeers=$[ $maxPeersFor10kStr*10000/$initialData ]
      peersIncrement=$[ $maxPeers/$maxPeersFor10kStr ]
    fi
  
#    if [ $[ $peersIncrement%2 ] -ne 0 ] && [ $[ $peersIncrement%5 ] -ne 0 ]
#    then
#      peersIncrement=2
#    fi

    mkdir -p $outputsPath"/"$dataType"/"$initialData
  

    if [ $fixedPeers -eq 0 ]
    then
      if [ $initialPeers -ne 0 ]
      then
        nPeers=$initialPeers
      else
        nPeers=$peersIncrement
      fi

      while [ $nPeers -le $maxPeers ]
  #   for nPeers in 2 4 6 8 10 12 14 16 18 20
      do
        runExp

        nPeers=$[ $nPeers + $peersIncrement ]
      done
    else
      nPeers=$fixedPeers
      runExp
    fi
#  done
  initialData=$[ $initialData + $baseDataIncrement ]
done
