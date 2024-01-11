#!/bin/bash --login

set -o errexit
set -o pipefail

source /app/src/env.sh

MD5_ORIGIN=`find /predict_occultation/pipeline -type f -exec md5sum {} \; | sort -k 2 | md5sum`
MD5_PIPE=`find ${PIPELINE_PATH} -type f -exec md5sum {} \; | sort -k 2 | md5sum`

if [[ "$MD5_ORIGIN" != "$MD5_PIPE" ]]; then
    echo "${PIPELINE_PATH}: directory content does not correspond to the container, copying container content..."
    cp -r /predict_occultation/pipeline/* ${PIPELINE_PATH}/
fi

# while true; do sleep 10; COUNT=$((COUNT + 10)); echo $COUNT; done;
#bash -c 'python run_daemon.py; exit' &

while true; do python /app/run_daemon.py ; sleep 30; done
