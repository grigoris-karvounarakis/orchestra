#!/bin/bash

./one-workload-small2.sh $1 $2 $3 $4 100 100 $5
./one-workload-small2.sh $1 $2 $3 $4 500 500 $5

./one-workload-small2.sh $1 $2 $3 $4 1000 1000 $5

./one-workload-small2.sh $1 $2 $3 $4 10000 0 $5

cp $1p$2n-$3c$4v-100i100d.create tests/$1p$2n-$3c$4v.create
cp $1p$2n-$3c$4v-100i100d.schema tests/$1p$2n-$3c$4v.schema
cp $1p$2n-$3c$4v-100i100d.cycles tests/$1p$2n-$3c$4v.cycles
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
