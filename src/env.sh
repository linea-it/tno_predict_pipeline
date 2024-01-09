#!/bin/bash --login

export PIPELINE_ROOT=/app/src
export PIPELINE_PREDIC_OCC=/app/predict_occultation
export PIPELINE_PATH=/app/predict_occultation/pipeline
export PYTHONPATH=$PYTHONPATH:$PIPELINE_ROOT:$PIPELINE_PATH:$PIPELINE_PREDIC_OCC
export WORKFLOW_PATH=$PIPELINE_ROOT
export EXECUTION_PATH=$PIPELINE_ROOT

ulimit -s 100000
ulimit -u 100000
umask 0002
