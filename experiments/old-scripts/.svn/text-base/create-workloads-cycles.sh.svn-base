#!/bin/bash

./one-workload-cycles.sh $1 $2 $3 $4 1 1 $5

./one-workload-cycles.sh $1 $2 $3 $4 1000 100 $5
./one-workload-cycles.sh $1 $2 $3 $4 10 1 $5

cp $1p$2n-$3c$4v-1i1d.create tests/$1p$2n-$3c$4v.create
cp $1p$2n-$3c$4v-1i1d.schema tests/$1p$2n-$3c$4v.schema
cp $1p$2n-$3c$4v-1i1d.cycles tests/$1p$2n-$3c$4v.cycles
#./copy-schema.sh tests/$1p$2n.schema
mv *.insert tests/
mv *.delete tests/
mv *_INS tests/
mv *_DEL tests/
rm *.create
rm *.destroy
rm *.schema
rm *.cycles
chmod a+x tests/*.insert
chmod a+x tests/*.delete
