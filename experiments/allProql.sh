#!/bin/bash

#======================#
# Create all workloads #
#======================#

# 1k int tuples/peer, 2-8 peers, chain topology, data at every peer
./proqlWorkloads.sh scalability integer 1000 1000 2 2 2 2
./proqlWorkloads.sh scalability integer 1000 1000 4 4 2 4
./proqlWorkloads.sh scalability integer 1000 1000 6 6 2 6
./proqlWorkloads.sh scalability integer 1000 1000 8 8 2 8

# 10k int tuples/peer, 2-8 peers, chain topology, data at every peer
./proqlWorkloads.sh scalability integer 10000 10000 2 2 2 2
./proqlWorkloads.sh scalability integer 10000 10000 4 4 2 4
./proqlWorkloads.sh scalability integer 10000 10000 6 6 2 6
./proqlWorkloads.sh scalability integer 10000 10000 8 8 2 8

# Same, with fewer peers with data
./proqlWorkloads.sh scalability integer 1000 1000 8 8 2 4
./proqlWorkloads.sh scalability integer 1000 1000 8 8 2 2
./proqlWorkloads.sh scalability integer 1000 1000 8 8 2 1

./proqlWorkloads.sh scalability integer 1000 10000 8 8 2 4
./proqlWorkloads.sh scalability integer 1000 10000 8 8 2 2
./proqlWorkloads.sh scalability integer 1000 10000 8 8 2 1

# Scalability vs peers for chain topology, with data at leaves only
./proqlWorkloads.sh scalability integer 10000 10000 5 30 2 1

# Scalability vs peers for chain topology, with data at leaves and middle peers
./proqlWorkloads.sh scalability integer 10000 10000 5 30 2 2

# Scalability vs data for chain topology, with data at leaves or leaves and middle peers
./proqlWorkloads.sh scalability integer 2000 8000 30 30 2 1
./proqlWorkloads.sh scalability integer 2000 8000 30 30 2 2

# Scalability vs peers for 4-ary tree topology, with data at leaves and middle peers
./proqlWorkloads.sh scalability integer 10000 10000 5 30 9 1

# Scalability vs peers for 4-ary tree topology, with data at leaves only
./proqlWorkloads.sh scalability integer 10000 10000 5 30 9 2

# Scalability vs peers for branched topology, with data at leaves and middle peers
./proqlWorkloads.sh scalability integer 10000 10000 5 30 11 1

# Scalability vs peers for branched topology, with data at leaves only
./proqlWorkloads.sh scalability integer 10000 10000 5 30 11 2

./proqlWorkloads.sh scalability integer 30000 30000 30 30 11 1
./proqlWorkloads.sh scalability integer 30000 30000 30 30 11 2

#=====================#
# Run all experiments #
#=====================#

# 1k int tuples/peer, 2-8 peers, chain topology, data at every peer
./proqlExperiments.sh scalability integer 1000 1000 2 2 2 2
./proqlExperiments.sh scalability integer 1000 1000 4 4 2 4
./proqlExperiments.sh scalability integer 1000 1000 6 6 2 6
./proqlExperiments.sh scalability integer 1000 1000 8 8 2 8

# 10k int tuples/peer, 2-8 peers, chain topology, data at every peer
./proqlExperiments.sh scalability integer 10000 10000 2 2 2 2
./proqlExperiments.sh scalability integer 10000 10000 4 4 2 4
./proqlExperiments.sh scalability integer 10000 10000 6 6 2 6
./proqlExperiments.sh scalability integer 10000 10000 8 8 2 8

# Same, with fewer peers with data
./proqlExperiments.sh scalability integer 1000 1000 8 8 2 4
./proqlExperiments.sh scalability integer 1000 1000 8 8 2 2
./proqlExperiments.sh scalability integer 1000 1000 8 8 2 1

./proqlExperiments.sh scalability integer 10000 10000 8 8 2 4
./proqlExperiments.sh scalability integer 10000 10000 8 8 2 2
./proqlExperiments.sh scalability integer 10000 10000 8 8 2 1

# Scalability vs peers for chain topology, with data at leaves only
./proqlExperiments.sh scalability integer 10000 10000 5 30 2 1

# Scalability vs peers for chain topology, with data at leaves and middle peers
./proqlExperiments.sh scalability integer 10000 10000 5 30 2 2

# Scalability vs data for chain topology, with data at leaves or leaves and middle peers
./proqlExperiments.sh scalability integer 2000 8000 30 30 2 1
./proqlExperiments.sh scalability integer 2000 8000 30 30 2 2

# Scalability vs peers for 4-ary tree topology, with data at leaves only
./proqlExperiments.sh scalability integer 10000 10000 5 30 9 1

# Scalability vs peers for 4-ary tree topology, with data at leaves and middle peers
./proqlExperiments.sh scalability integer 10000 10000 5 30 9 2

# Scalability vs peers for branched topology, with data at leaves only
./proqlExperiments.sh scalability integer 10000 10000 5 30 11 1

# Scalability vs peers for branched topology, with data at leaves and middle peers
./proqlExperiments.sh scalability integer 10000 10000 5 30 11 2

# Inner joins for chain of 30 peers, data at leaves only
#./proqlExperiments.sh join integer 10000 10000 30 30 2 1 2
./proqlExperiments.sh join integer 10000 10000 30 30 2 1 3
./proqlExperiments.sh join integer 10000 10000 30 30 2 1 4
./proqlExperiments.sh join integer 10000 10000 30 30 2 1 5
./proqlExperiments.sh join integer 10000 10000 30 30 2 1 10
./proqlExperiments.sh join integer 10000 10000 30 30 2 1 15

# Inner joins for chain of 30 peers, data at leaves and middle
#./proqlExperiments.sh join integer 10000 10000 30 30 2 2 2
./proqlExperiments.sh join integer 10000 10000 30 30 2 2 3
./proqlExperiments.sh join integer 10000 10000 30 30 2 2 4
./proqlExperiments.sh join integer 10000 10000 30 30 2 2 5
./proqlExperiments.sh join integer 10000 10000 30 30 2 2 10
./proqlExperiments.sh join integer 10000 10000 30 30 2 2 15

# Inner joins for 4-ary tree of 30 peers, data at leaves only
./proqlExperiments.sh join integer 10000 10000 30 30 9 1 2-3

# Inner joins for 4-ary tree of 30 peers, data at leaves and middle
./proqlExperiments.sh join integer 10000 10000 30 30 9 2 2-3

# Joins for branched topology of 30 peers, data at leaves only
./proqlExperiments.sh join integer 10000 10000 30 30 11 1 3
./proqlExperiments.sh ojoin integer 10000 10000 30 30 11 1 3
./proqlExperiments.sh lojoin integer 10000 10000 30 30 11 1 3

./proqlExperiments.sh join integer 10000 10000 30 30 11 1 5
./proqlExperiments.sh ojoin integer 10000 10000 30 30 11 1 5
./proqlExperiments.sh lojoin integer 10000 10000 30 30 11 1 5

./proqlExperiments.sh join integer 10000 10000 30 30 11 1 7
./proqlExperiments.sh ojoin integer 10000 10000 30 30 11 1 7
./proqlExperiments.sh lojoin integer 10000 10000 30 30 11 1 7

# Inner joins for branched topology of 30 peers, data at leaves only
./proqlExperiments.sh foo integer 30000 30000 30 30 11 1
./proqlExperiments.sh join integer 30000 30000 30 30 11 1 3
./proqlExperiments.sh join integer 30000 30000 30 30 11 1 5
./proqlExperiments.sh join integer 30000 30000 30 30 11 1 7

./proqlExperiments.sh foo integer 30000 30000 30 30 11 2
./proqlExperiments.sh join integer 30000 30000 30 30 11 2 3
./proqlExperiments.sh join integer 30000 30000 30 30 11 2 5
./proqlExperiments.sh join integer 30000 30000 30 30 11 2 7

# Joins for branched topology of 30 peers, data at leaves and middle peers
./proqlExperiments.sh join integer 10000 10000 30 30 11 2 3
./proqlExperiments.sh ojoin integer 10000 10000 30 30 11 2 3
./proqlExperiments.sh lojoin integer 10000 10000 30 30 11 2 3

./proqlExperiments.sh join integer 10000 10000 30 30 11 2 5
./proqlExperiments.sh ojoin integer 10000 10000 30 30 11 2 5
./proqlExperiments.sh lojoin integer 10000 10000 30 30 11 2 5

./proqlExperiments.sh join integer 10000 10000 30 30 11 2 7
./proqlExperiments.sh ojoin integer 10000 10000 30 30 11 2 7
./proqlExperiments.sh lojoin integer 10000 10000 30 30 11 2 7

./proqlExperiments.sh join integer 10000 10000 30 30 11 2 10
./proqlExperiments.sh ojoin integer 10000 10000 30 30 11 2 10
./proqlExperiments.sh lojoin integer 10000 10000 30 30 11 2 10

# Left and outer joins for 30 peers in 4-ary tree topology
./proqlExperiments.sh lojoin integer 10000 10000 30 30 9 1 2-3
./proqlExperiments.sh ojoin integer 10000 10000 30 30 9 1 2-3

./proqlExperiments.sh lojoin integer 10000 10000 30 30 9 2 2-3
./proqlExperiments.sh ojoin integer 10000 10000 30 30 9 2 2-3

