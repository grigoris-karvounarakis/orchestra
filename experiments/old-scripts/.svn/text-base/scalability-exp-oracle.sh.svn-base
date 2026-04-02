#!/bin/bash

source ./set-vars.sh

# Run experiment
cd tests
# NO NULLS
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=2p1n-1c1v >& ../outputs/scalability/2p1n-nonulls.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=3p1n-1c1v >& ../outputs/scalability/3p1n-nonulls.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=4p1n-1c1v >& ../outputs/scalability/4p1n-nonulls.stratified.out
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=false -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -workload=5p1n-1c1v-1000i100d -schema=5p1n-1c1v >& ../outputs/scalability/5p1n-nonulls.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=6p1n-1c1v >& ../outputs/scalability/6p1n-nonulls.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=7p1n-1c1v >& ../outputs/scalability/7p1n-nonulls.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=10p1n-1c1v >& ../outputs/scalability/10p1n-nonulls.stratified.out
# NO CYCLES
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=2p0n-0c0.9v >& ../outputs/scalability/2p0n-nocycles.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=3p0n-0c0.9v >& ../outputs/scalability/3p0n-nocycles.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=4p0n-0c0.9v >& ../outputs/scalability/4p0n-nocycles.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=5p0n-0c0.9v >& ../outputs/scalability/5p0n-nocycles.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=6p0n-0c0.9v >& ../outputs/scalability/6p0n-nocycles.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -schema=10p0n-0c0.9v >& ../outputs/scalability/10p0n-nocycles.stratified.out
cd ..
