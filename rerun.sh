#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

JOBID=$1

echo "Rerun job ${JOBID} in a local environment to test the pipeline"
python rerun.py $JOBID
