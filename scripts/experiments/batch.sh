#! /bin/bash

set -e
set -x

TOTAL=2048
RANGE=512

#
# * No duplicated execution 
#

# * Pure parallel DAGs.
for ((i=64;i<=$RANGE;i*=2))
do
    python -m noserver --mode benchmark --width $i --depth 1 --rps $(bc <<< "scale=9; 1/$i") --invocations $TOTAL -hvm 992dc435cd1e -logfile w-"$i"_d-1_dup0.log
done

# # * Pure chains.
# for ((i=2;i<=$RANGE;i*=2))
# do
#     python -m noserver --mode benchmark --width 1 --depth $i --rps $(bc <<< "scale=9; 1/$i") --invocations $TOTAL -hvm 992dc435cd1e -logfile w-1_d-"$i"_dup0.log
# done

# # * Hybrid.
# for ((j=2;j<=1024;j*=2))
# do
#     for ((i=2;i<=1024;i*=2))
#     do
#         python -m noserver --mode benchmark --width $i --depth $j --rps $(bc <<< "scale=9; 1/($i+$j)") --invocations 2048 -logfile b-"$i"_d-"$j"_dup0.log
#     done
# done
