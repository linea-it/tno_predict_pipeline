#!/bin/bash --login

rsync -r /predict_occultation/ ${PIPELINE_PREDIC_OCC}/

#echo "CONDA_ENVS: ${CONDA_ENVS}";
#
#if [ -n "$CONDA_ENVS" ]; then 
#    rsync -a /opt/conda/envs ${CONDA_ENVS}
#fi

exec "$@"
