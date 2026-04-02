#!/bin/bash

# Create all workloads
#=====================
# 2k int tuples/peer for diff peers
./todsWorkloads.sh inc integer 2000 2000 0 20 0

# 10k int tuples/peer for diff peers
./todsWorkloads.sh inc integer 10000 10000 0 50 0

# Diff numbers of int data for 10 peers
./todsWorkloads.sh inc integer 2000 10000 10 20 0

# 2k str tuples/peer for diff peers
./todsWorkloads.sh inc string 2000 2000 0 8 0

# Diff numbers of str data for 10 peers
./todsWorkloads.sh inc string 2000 10000 10 8 0

# Unidirectional vs. Bidirectional, for up to 30 peers and 2k int tuples/peer
./todsWorkloads.sh initial integer 2000 2000 0 6 0
./todsWorkloads.sh initial bidir 2000 2000 0 6 0

# Bidirectional deletions with/out side-effects detection
./todsWorkloads.sh se-nse bidir 2000 2000 0 3 0
./todsWorkloads.sh se-nse bidir 2000 2000 10 3 10

# Face-off between deletion algorithms for 10 peers and 10k tuples/peer
# integer data
./todsWorkloads.sh incdel integer 10000 10000 10 20 10
# string data
#./todsWorkloads.sh incdel string 2000 2000 10 8 10

# Initial computation for mappings with different numbers of cycles
./todsWorkloads.sh initial cycles 2000 2000 0 10 0

# Run all experiments
#=====================
# Scalability vs peers for 2k int tuples/peer
./todsExperiments.sh inc integer 2000 2000 0 20 0 0

# Scalability vs peers for 10k int tuples/peer
./todsExperiments.sh inc integer 10000 10000 0 50 0 0

# Scalability vs int data for 10 peers
./todsExperiments.sh inc integer 2000 10000 10 20 0 0

# Scalability of non-incremental algorithms vs peers for 2k int tuples/peer
./todsExperiments.sh noninc integer 2000 2000 0 20 0 0

# Scalability of non-incremental algorithms vs int data for 10 peers
./todsExperiments.sh noninc integer 2000 8000 10 20 0 0

# Scalability of DRed deletions vs peers for 2k int tuples/peer
./todsExperiments.sh dred integer 2000 2000 0 20 0 0

# Scalability of DRed deletions vs int data for 10 peers
./todsExperiments.sh dred integer 2000 10000 10 20 0 0

# 2k str tuples/peer for diff peers
./todsExperiments.sh inc string 2000 2000 0 8 0 0

# Diff numbers of str data for 10 peers
./todsExperiments.sh inc string 2000 8000 10 8 0 0

# Unidirectional vs. Bidirectional, for up to 30 peers and 2k int tuples/peer
./todsExperiments.sh initial integer 2000 2000 0 6 0 0
./todsExperiments.sh initial bidir 2000 2000 0 6 0 0

# Bidirectional deletions with/out side-effects detection
./todsExperiments.sh se-nse bidir 2000 2000 0 3 0 0
./todsExperiments.sh se-nse bidir 2000 2000 10 3 0 10

# Face-off between deletion algorithms for 10 peers and 10k tuples/peer
# integer data
./todsExperiments.sh incdel integer 10000 10000 10 20 0 10
./todsExperiments.sh nonincdel integer 10000 10000 10 20 0 10
./todsExperiments.sh dreddel integer 10000 10000 10 20 0 10
# string data
#./todsExperiments.sh incdel string 2000 2000 10 8 0 10
#./todsExperiments.sh nonincdel string 2000 2000 10 8 0 10
#./todsExperiments.sh dreddel string 2000 2000 10 8 0 10

# Initial computation for mappings with different numbers of cycles
./todsExperiments.sh initial cycles 2000 2000 0 10 0 0
