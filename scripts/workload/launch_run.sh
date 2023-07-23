#!/bin/bash
# set -e
# set -x
# daglen=(2 5 8 10 16 24 32)
daglen=(10 15 20 25 30)
for i in ${daglen[@]}; 
    do echo $i; 
    python -m noserver --mode dag --stages=$i
    cd data/post_analysis
    # echo ""
    python parse_request.py 
    cd -
done

