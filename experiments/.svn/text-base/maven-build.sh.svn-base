#!/bin/bash

\rm -rf ../mappings/target/dependency
cd ..
mvn clean
mvn compile
mvn -Dmaven.test.skip=true source:jar install
cd mappings
mvn dependency:copy-dependencies
cd ../experiments
