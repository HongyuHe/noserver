# #!/usr/bin/env bash
set -e
set -x
# STAGES=$1
# TOTAL=$2
LOAD=${1:-1}
SCRIPTS_DIR='workloads/generation'
# python $SCRIPTS_DIR/generate_invocation_poisson.py -s $STAGES -n $TOTAL && \
#     python $SCRIPTS_DIR/generate_serial_dag_config.py -s $STAGES
#!/bin/bash
# set -e
# set -x
# daglen=(2 5 8 10 16 24 32)
# daglen=(5 10 15 20 25 30 35 40)
daglen=(1 2 4 8 16 32 64 128 256 512 1024)
# daglen=(5 10 15 20 25 30)
for STAGES in ${daglen[@]}; do
    python $SCRIPTS_DIR/generate_serial_dag_config.py -s $STAGES && python $SCRIPTS_DIR/generate_invocation_poisson.py -s $STAGES -l $LOAD 
done


