#!/bin/bash
export CONDAPATH=/lustre/t1/tmp/tno/miniconda3/bin
export PIPELINE_ROOT=/lustre/t1/tmp/tno/pipelines
export PYTHONPATH=$PYTHONPATH:$PIPELINE_ROOT

source $CONDAPATH/activate
conda activate tno_pipeline

zsh

umask 0002
