#!/bin/bash

source ./set-vars.sh

# Run experiment
cd tests
# NO CYCLES
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -scalability=true -schema=2p0n-0c1v >& ../outputs/scalability/2p0n-0c1v.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -scalability=true -schema=5p0n-0c1v >& ../outputs/scalability/5p0n-0c1v.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -scalability=true -schema=10p0n-0c1v >& ../outputs/scalability/10p0n-0c1v.out
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=true -scalability=true -schema=5p0n-0c1v >& ../outputs/scalability/5p0n-0c1v.OU.out
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=true -scalability=true -schema=10p0n-0c1v >& ../outputs/scalability/10p0n-0c1v.OU.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -scalability=true -schema=15p0n-0c1v >& ../outputs/scalability/15p0n-0c1v.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -scalability=true -schema=20p0n-0c1v >& ../outputs/scalability/20p0n-0c1v.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -scalability=true -schema=30p0n-0c1v >& ../outputs/scalability/30p0n-0c1v.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -scalability=true -schema=50p0n-0c1v >& ../outputs/scalability/50p0n-0c1v.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -scalability=true -schema=100p0n-0c1v >& ../outputs/scalability/100p0n-0c1v.out
cd ..
