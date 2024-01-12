#!/bin/bash --login

set -o errexit
set -o pipefail

source /app/src/env.sh

while true; do python /app/run_daemon.py ; sleep 30; done
