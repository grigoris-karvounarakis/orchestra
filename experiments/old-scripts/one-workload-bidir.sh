#!/bin/bash
python workload.py --filename=$1p$2n-$3c$4v-$5i$6d --peers=$1 --fanout=$2 --mincycles=$3 --maxcycles=$3 --coverage=$4 --insertions=$5 --deletions=$6 --cutoff=$7 --relsize=9 --dbalias=$8 --olivier --xmlformat --bidir --user=GKARVOUN --password=GREG#00 --seed=2 --skip=$9 
