#!/bin/bash
#echo "First arg: $1"
#echo "Second arg: $2"

dataset=$1
cd /data/dd-analysis/$dataset/blat
faToTwoBit raw_sequences.fa raw_sequences.2bit
gfServer -tileSize=8 -minMatch=2 start 0.0.0.0 8080 raw_sequences.2bit

