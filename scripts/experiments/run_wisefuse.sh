#! /bin/bash

set -e
set -x

RPS=$1
VM=$2

# declare -a modes=("trace" "bundled" "fused" "wisefuse")
declare -a modes=("fused" "wisefuse")

for mode in "${modes[@]}" 
do
    mkdir -p data/results/"$mode"
    time python -m noserver -mode trace -trace data/"$mode"_dags.pkl -vm "$VM" -rps "$RPS" \
        -logfile "$mode".log
    mv data/results/*.csv data/results/"$mode"
done
