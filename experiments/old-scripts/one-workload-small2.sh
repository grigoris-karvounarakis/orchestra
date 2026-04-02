#!/bin/bash

python workload.py --filename=$1p$2n-$3c$4v-$5i$6d --peers=$1 --fanout=$2 --mincycles=$3 --maxcycles=$3 --coverage=$4 --insertions=$5 --deletions=$6 --cutoff=32 --relsize=9 --inout --dbalias=$7 --user=GKARVOUN --password=GREG#00 --olivier --grigoris --xmlformat
