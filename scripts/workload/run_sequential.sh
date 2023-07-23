#!/usr/bin/env bash
set -e 
set -x
RESULTS_DIR='data/results/new_sequential_JSQ'
CSV_DIR='data/results'
for i in {0..8}
do
    STAGES=$((2**$i))
    # ./generate_serial_dag.sh 
    python -m noserver --mode dag --stages $STAGES --placement=PlacementMode.JSQ # --noconfig.harvestvm.ENABLE_HARVEST
    mkdir -p $RESULTS_DIR/s$STAGES/
    mv $CSV_DIR/*.csv $RESULTS_DIR/s$STAGES/
done