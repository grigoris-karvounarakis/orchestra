#!/bin/bash

for i in $1/*.out
do
	echo $i
	./out2csv.sh $i
done

