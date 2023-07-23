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

# #!/usr/bin/env bash
set -e
set -x
# STAGES=$1
# TOTAL=$2
LOAD=${1:-1}
SCRIPTS_DIR='workloads/generation'
# python $SCRIPTS_DIR/generate_invocation_poisson.py -s $STAGES -n $TOTAL && \
#     python $SCRIPTS_DIR/generate_parallel_dag_config.py -s $STAGES

dagdepth=(1 2 3 4 5 6 7 8)
dagwidth=(1 2 4 8 16 32 64) 
# daglen=(1 2 4)

# daglen=(5 10 15 20 25 30)
for STAGES in ${dagdepth[@]}; do
    for WIDTHS in ${dagwidth[@]}; do
        python $SCRIPTS_DIR/generate_hybrid_dag_config.py -d $STAGES -w $WIDTHS && python $SCRIPTS_DIR/generate_invocation_hybrid_poisson.py -d $STAGES -w $WIDTHS -l $LOAD 
    done
done



