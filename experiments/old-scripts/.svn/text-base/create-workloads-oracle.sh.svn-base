#!/bin/bash

./one-workload-oracle.sh $1 $2 $3 $4 1 1 $5

./one-workload-oracle.sh $1 $2 $3 $4 10 1 $5
./one-workload-oracle.sh $1 $2 $3 $4 10 2 $5
./one-workload-oracle.sh $1 $2 $3 $4 10 3 $5
./one-workload-oracle.sh $1 $2 $3 $4 10 4 $5
./one-workload-oracle.sh $1 $2 $3 $4 10 5 $5
./one-workload-oracle.sh $1 $2 $3 $4 10 6 $5
./one-workload-oracle.sh $1 $2 $3 $4 10 7 $5
./one-workload-oracle.sh $1 $2 $3 $4 10 8 $5
./one-workload-oracle.sh $1 $2 $3 $4 10 9 $5
./one-workload-oracle.sh $1 $2 $3 $4 10 10 $5

./one-workload-oracle.sh $1 $2 $3 $4 100 10 $5
./one-workload-oracle.sh $1 $2 $3 $4 100 20 $5
./one-workload-oracle.sh $1 $2 $3 $4 100 30 $5
./one-workload-oracle.sh $1 $2 $3 $4 100 40 $5
./one-workload-oracle.sh $1 $2 $3 $4 100 50 $5
#./one-workload-oracle.sh $1 $2 $3 $4 100 60 $5
#./one-workload-oracle.sh $1 $2 $3 $4 100 70 $5
#./one-workload-oracle.sh $1 $2 $3 $4 100 80 $5
#./one-workload-oracle.sh $1 $2 $3 $4 100 90 $5
#./one-workload-oracle.sh $1 $2 $3 $4 100 100 $5

./one-workload-oracle.sh $1 $2 $3 $4 1000 100 $5
./one-workload-oracle.sh $1 $2 $3 $4 1000 200 $5
./one-workload-oracle.sh $1 $2 $3 $4 1000 300 $5
./one-workload-oracle.sh $1 $2 $3 $4 1000 400 $5
./one-workload-oracle.sh $1 $2 $3 $4 1000 500 $5
#./one-workload-oracle.sh $1 $2 $3 $4 1000 600 $5
#./one-workload-oracle.sh $1 $2 $3 $4 1000 700 $5
#./one-workload-oracle.sh $1 $2 $3 $4 1000 800 $5
#./one-workload-oracle.sh $1 $2 $3 $4 1000 900 $5
#./one-workload-oracle.sh $1 $2 $3 $4 1000 1000 $5

#./one-workload-oracle.sh $1 $2 $3 $4 10000 1000 $5
#./one-workload-oracle.sh $1 $2 $3 $4 10000 2000 $5
#./one-workload-oracle.sh $1 $2 $3 $4 10000 3000 $5
#./one-workload-oracle.sh $1 $2 $3 $4 10000 4000 $5
#./one-workload-oracle.sh $1 $2 $3 $4 10000 5000 $5
#./one-workload-oracle.sh $1 $2 $3 $4 10000 6000 $5
#./one-workload-oracle.sh $1 $2 $3 $4 10000 7000 $5
#./one-workload-oracle.sh $1 $2 $3 $4 10000 8000 $5
#./one-workload-oracle.sh $1 $2 $3 $4 10000 9000 $5
#./one-workload-oracle.sh $1 $2 $3 $4 10000 10000 $5

#./one-workload-oracle.sh $1 $2 $3 $4 10000 10000 $5
#./one-workload-oracle.sh $1 $2 $3 $4 50000 50000 $5
#./one-workload-oracle.sh $1 $2 $3 $4 100 25 $5
#./one-workload-oracle.sh $1 $2 $3 $4 1000 250 $5
#./one-workload-oracle.sh $1 $2 $3 $4 10000 2500 $5

cp $1p$2n-$3c$4v-1i1d.create tests/$1p$2n-$3c$4v.create
cp $1p$2n-$3c$4v-1i1d.schema tests/$1p$2n-$3c$4v.schema
cp $1p$2n-$3c$4v-1i1d.cycles tests/$1p$2n-$3c$4v.cycles
#./copy-schema.sh tests/$1p$2n.schema
mv *.insert tests/
mv *.delete tests/
mv *_INS tests/
mv *_DEL tests/
mv *.ctl tests/
rm *.create
rm *.destroy
rm *.schema
rm *.cycles
chmod a+x tests/*.insert
chmod a+x tests/*.delete
