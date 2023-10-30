#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

cd /usr/src/app
echo "Start TNO Pipeline"

/bin/sh -c "while sleep 1000; do :; done"