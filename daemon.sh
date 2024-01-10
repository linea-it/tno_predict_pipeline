#!/bin/bash --login

set -o errexit
set -o pipefail

source /app/src/env.sh

# while true; do sleep 10; COUNT=$((COUNT + 10)); echo $COUNT; done;
#bash -c 'python run_daemon.py; exit' &

while true; do python /app/run_daemon.py ; sleep 30; done
