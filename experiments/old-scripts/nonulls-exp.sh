#!/bin/bash

source ./set-vars.sh

# Run experiment
cd tests
# NO NULLS
java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p2n-0c1v >& ../outputs/5p2n-0c-nonulls.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p2n-1c1v >& ../outputs/5p2n-1c-nonulls.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p2n-2c1v >& ../outputs/5p2n-2c-nonulls.out
#java -Xms768m -Xmx768m -classpath ${cp} edu.upenn.cis.orchestra.SqlMap -prepare=true -debug=false -union=false -autocommit=false -stratified=true -outerunion=false -nullscycles=true -schema=5p2n-3c1v >& ../outputs/5p2n-3c-nonulls.out
