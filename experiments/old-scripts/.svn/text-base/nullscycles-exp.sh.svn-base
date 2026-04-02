#!/bin/bash

source ./set-vars.sh

# Run experiment
cd tests
# NULLS + CYCLES
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p1n-0c0.9v >& ../outputs/5p1n-nulls0cycles.9nulls.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p1n-1c0.9v >& ../outputs/5p1n-nullscycles.9nulls.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p1n-1c0.7v >& ../outputs/5p1n-nullscycles.7nulls.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p1n-1c0.5v >& ../outputs/5p1n-nullscycles.5nulls.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p1n-1c0.3v >& ../outputs/5p1n-nullscycles.3nulls.out
##java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p1n-1c0.9v >& ../outputs/5p1n-nulls1cycles.9nulls.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p1n-2c0.9v >& ../outputs/5p1n-nulls2cycles.9nulls.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p1n-3c0.9v >& ../outputs/5p1n-nulls3cycles.9nulls.out
##java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p2n-1c0.9v >& ../outputs/5p2n-nulls1cycles.9nulls.out
##java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p2n-2c0.9v >& ../outputs/5p2n-nulls2cycles.9nulls.out
##java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p2n-3c0.9v >& ../outputs/5p2n-nulls3cycles.9nulls.out
cd ..
