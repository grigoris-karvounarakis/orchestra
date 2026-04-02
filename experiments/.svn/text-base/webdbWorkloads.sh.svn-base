#!/bin/bash

export path='tests/webdb/autogen'
export fanout=1
export nCycles=0
export coverage=1
export cutoff=8
export db='BIOTBG'

# 2p1n
export nPeers=2
for nPeers in 2 5 10
do
  export namePrefix=$nPeers'p'$fanout'n-'$nCycles'c'$coverage'v'
  export subdir=$path'/'$namePrefix
  ./one-workload-bidir.sh $nPeers $fanout $nCycles $coverage 2000 0 $cutoff $db 0
  mkdir -p $subdir
  mkdir -p $subdir'/insertions'
  mv $namePrefix'-2000i0d.schema' $path'/'$namePrefix'.schema'
  mv $namePrefix'-2000i0d.insert' $subdir'/insertions'
  touch $subdir'/insertions/'$namePrefix'-2000i0d.delete'
  mv $namePrefix'-2000i0d.'*'INS' $subdir'/insertions'
  ./one-workload-bidir.sh $nPeers $fanout $nCycles $coverage 200 200 $cutoff $db 0
  mkdir -p $subdir'/localdeletions'
  mv $namePrefix'-200i200d.delete' $subdir'/localdeletions'
  touch $subdir'/localdeletions/'$namePrefix'-200i200d.insert'
  mv $namePrefix'-200i200d.'*'DEL' $subdir'/localdeletions'
  ./one-workload-bidir.sh $nPeers $fanout $nCycles $coverage 200 0 $cutoff $db 20000
  mkdir -p $subdir'/localinsertions'
  mv $namePrefix'-200i0d.insert' $subdir'/localinsertions'
  touch $subdir'/localinsertions/'$namePrefix'-200i0d.delete'
  mv $namePrefix'-200i0d.'*'INS' $subdir'/localinsertions'
  ./one-workload.sh $nPeers $fanout $nCycles $coverage 0 0 $cutoff $db 
  mv $namePrefix'-0i0d.schema' $path'/'$namePrefix'-uni.schema'
  rm $namePrefix'-'*
done

