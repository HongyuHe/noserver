#!/usr/bin/env bash
set -e 
set -x

WORKERS=${1:-8}
PLACEMENT="${2:-PlacementMode.MSW}" # PlacementMode.MSW or PlacementMode.JSQ
DAGSHAPE="${3:-parallel}" # parallel or sequential
LOAD="${4:-30}"
for i in {0..10}
do
    STAGES=$((2**$i))
    # ./generate_serial_dag.sh 
    CSV_DIR=data/results/w$WORKERS/$PLACEMENT/$DAGSHAPE/s${STAGES}
    python -m noserver --mode dag --stages $STAGES --placement=$PLACEMENT --dagshape=$DAGSHAPE  --numworkers=$WORKERS --load=$LOAD&
    # --noconfig.harvestvm.ENABLE_HARVEST
    # mkdir -p $RESULTS_DIR/s$STAGES/
    # mv $CSV_DIR/*.csv $RESULTS_DIR/s$STAGES/
done

wait
echo "Finished all jobs"
exit 1