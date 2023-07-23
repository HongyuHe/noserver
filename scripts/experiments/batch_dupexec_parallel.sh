#! /bin/bash

set -e
set -x

declare -a rates=("0.1" "0.4" "0.7")
#
# * Duplicated execution 
#

# * Pure parallel DAGs.
for rate in "${rates[@]}"
do
    for ((i=2;i<=1024;i*=2))
    do
        python -m noserver --mode benchmark --width $i --depth 1 --rps $(bc <<< "scale=9; 1/$i") --invocations 2048 --config.policy.DUP_EXECUTION --config.policy.DUP_EXECUTION_THRESHOLD="$rate" -hvm 992dc435cd1e -logfile b-"$i"_d-1_dup1_r-"$rate".log
    done
done
