#!/bin/bash

source ./set-vars.sh

# Run experiment
cd tests
# NO NULLS
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -skipdelpost=true -stratified=true -outerunion=false -schema=5p0n-0c1v >& ../outputs/inc-noninc/5p0n-0c.out
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -skipdelpost=true -stratified=true -outerunion=false -schema=5p1n-1c1v >& ../outputs/inc-noninc/5p1n-1c.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -skipdelpost=true -stratified=true -outerunion=false -schema=5p1n-1c0.9v >& ../outputs/inc-noninc/5p1n-1c-nulls.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -skipdelpost=true -stratified=true -outerunion=true -schema=5p1n-1c1v >& ../outputs/inc-noninc/5p1n-1c.OU.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -skipdelpost=true -stratified=true -outerunion=true -schema=5p0n-0c1v >& ../outputs/inc-noninc/5p0n-0c.OU.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -skipdelpost=true -stratified=true -outerunion=true -schema=5p1n-1c0.9v >& ../outputs/inc-noninc/5p1n-1c-nulls.OU.out
