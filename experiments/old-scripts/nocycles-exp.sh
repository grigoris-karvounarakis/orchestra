#!/bin/bash

source ./set-vars.sh

# Run experiment
cd tests
# NO CYCLES
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=5p0n-0c0.9v >& ../outputs/5p0n-nocycles.9nulls.out
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=5p0n-0c0.7v >& ../outputs/5p0n-nocycles.7nulls.out
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=5p0n-0c0.5v >& ../outputs/5p0n-nocycles.5nulls.out
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=5p0n-0c0.3v >& ../outputs/5p0n-nocycles.3nulls.out
cd ..
