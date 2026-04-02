#!/bin/bash

source ./set-vars.sh

# Run experiment
cd tests
# NO CYCLES
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -scalability1=true -schema=$1p0n-0c1v >& ../outputs/scalability/$1p0n-0c1v-1.out
cd ..
