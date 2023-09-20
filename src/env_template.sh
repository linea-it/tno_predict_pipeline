export CONDAPATH=/home/glauber.costa/miniconda3/bin
export PIPELINE_ROOT=/archive/des/tno/dev/pipelines
export PYTHONPATH=$PYTHONPATH:$PIPELINE_ROOT

source $CONDAPATH/activate
conda activate tno_pipeline

umask 0002
