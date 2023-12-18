#!/bin/bash --login

set -o errexit
set -o pipefail
set -o nounset

conda activate py3

python run_parsl.py