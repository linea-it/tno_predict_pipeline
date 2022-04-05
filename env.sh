export CONDAPATH=/home/glauber.costa/miniconda3/bin
export PIPELINE_ROOT=/archive/des/tno/dev/nima/pipeline
export PYTHONPATH=$PYTHONPATH:$PIPELINE_ROOT

source $CONDAPATH/activate
conda activate nima_htc

umask 0002
