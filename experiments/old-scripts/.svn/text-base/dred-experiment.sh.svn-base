#!/bin/bash

source ./set-vars.sh

# Run experiment
cd tests
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -dred=true -outerunion=false -schema=$1 >& ../outputs/$1.dred.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -dred=true -outerunion=true -schema=$1 >& ../outputs/$1.OU.dred.out
cd ..


