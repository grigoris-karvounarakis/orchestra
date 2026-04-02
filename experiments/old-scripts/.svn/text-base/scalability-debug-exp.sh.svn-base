#!/bin/bash

source ./set-vars.sh

# Run experiment
cd tests
# NO NULLS
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=2p1n-1c1v >& ../outputs/scalability/2p1n-nonulls.debug.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=3p1n-1c1v >& ../outputs/scalability/3p1n-nonulls.debug.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=4p1n-1c1v >& ../outputs/scalability/4p1n-nonulls.debug.stratified.out
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=5p1n-1c1v >& ../outputs/scalability/5p1n-nonulls.debug.stratified.out
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=6p1n-1c1v >& ../outputs/scalability/6p1n-nonulls.debug.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=7p1n-1c1v >& ../outputs/scalability/7p1n-nonulls.debug.stratified.out
# NO CYCLES
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=2p0n-0c0.9v >& ../outputs/scalability/2p0n-nocycles.debug.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=3p0n-0c0.9v >& ../outputs/scalability/3p0n-nocycles.debug.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=4p0n-0c0.9v >& ../outputs/scalability/4p0n-nocycles.debug.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=5p0n-0c0.9v >& ../outputs/scalability/5p0n-nocycles.debug.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=6p0n-0c0.9v >& ../outputs/scalability/6p0n-nocycles.debug.stratified.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -debug=true -union=false -autocommit=false -stratified=true -outerunion=false -schema=7p0n-0c0.9v >& ../outputs/scalability/7p0n-nocycles.debug.stratified.out
cd ..
