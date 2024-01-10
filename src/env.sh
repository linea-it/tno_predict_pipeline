#!/bin/bash --login

conda activate py3

export PYTHONPATH=${PYTHONPATH}:${PIPELINE_ROOT}:${PIPELINE_PATH}:${PIPELINE_PREDIC_OCC}

ulimit -s 100000
ulimit -u 100000
umask 0002
