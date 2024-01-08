#!/bin/bash --login

export PIPELINE_ROOT=/app/src
export PIPELINE_PREDIC_OCC=/app/predict_occultation
export PIPELINE_PATH=/app/predict_occultation/pipeline
export PYTHONPATH=$PYTHONPATH:$PIPELINE_ROOT:$PIPELINE_PATH:$PIPELINE_PREDIC_OCC

conda activate py3

export WORKFLOW_PATH=$PIPELINE_ROOT
export EXECUTION_PATH=$PIPELINE_ROOT

# export DB_URI=postgresql+psycopg2://untrustedprod:untrusted@desdb4.linea.org.br:5432/prod_gavo
export DB_URI=postgresql+psycopg2://untrustedprod:untrusted@host.docker.internal:3307/prod_gavo
export PARSL_ENV=local # local or linea (slurm cluster)

ulimit -s 100000
ulimit -u 100000
umask 0002
