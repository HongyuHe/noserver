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
        python $SCRIPTS_DIR/generate_parallel_dag_config.py -s $STAGES && python $SCRIPTS_DIR/generate_invocation_hybrid_poisson.py -s $STAGES -l $LOAD 
    done
done


