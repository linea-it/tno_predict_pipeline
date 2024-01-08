#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# while true; do sleep 10; COUNT=$((COUNT + 10)); echo $COUNT; done;
#bash -c 'python run_daemon.py; exit' &

while true; do python run_daemon.py ; sleep 30; done
