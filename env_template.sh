#!/bin/bash
export CONDAPATH=
export PIPELINE_ROOT=
export PIPELINE_PATH=
export PIPELINE_PRAIA=
export PYTHONPATH=$PYTHONPATH:$PIPELINE_ROOT:$PIPELINE_PATH:$PIPELINE_PRAIA

source $CONDAPATH/activate
conda activate tno_pipeline

export WORKFLOW_PATH=$PIPELINE_ROOT
export EXECUTION_PATH=$PIPELINE_ROOT

export DB_URI=

umask 0002
