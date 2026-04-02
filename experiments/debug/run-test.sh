#!/bin/bash

source ./set-vars.sh

# Run experiment
java -classpath ${cp} edu.upenn.cis.orchestra.Test >&  outputs/baseline
