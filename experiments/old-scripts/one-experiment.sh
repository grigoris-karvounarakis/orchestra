#!/bin/bash

source ./set-vars.sh

# Run experiment
cd tests
javaw -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -autocommit=false -outerunion=true -schema=$1 -workload=$1-1000i100d >& ../outputs/$1-1000i100d.OU.out
javaw -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -autocommit=false -outerunion=false -schema=$1 -workload=$1-1000i100d >& ../outputs/$1-1000i100d.out
cd ..


