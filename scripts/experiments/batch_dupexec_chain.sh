#! /bin/bash

set -e
set -x

declare -a rates=("0.1" "0.4" "0.7")
#
# * Duplicated execution 
#

# * Pure chains.
for rate in "${rates[@]}"
do
    for ((i=2;i<=1024;i*=2))
    do
        python -m noserver --mode benchmark --width 1 --depth $i --rps $(bc <<< "scale=9; 1/$i") --invocations 2048 --config.policy.DUP_EXECUTION --config.policy.DUP_EXECUTION_THRESHOLD="$rate" -hvm 992dc435cd1e -logfile b-1_d-"$i"_dup1_r-"$rate".log
    done
done
