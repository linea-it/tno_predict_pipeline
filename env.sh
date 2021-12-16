export NIMA=/archive/des/tno/dev/nima
export PYTHONPATH=$PYTHONPATH:$NIMA
export CONDAPATH=/home/glauber.costa/miniconda3/bin

source $CONDAPATH/activate
conda activate nima_htc

umask 0002
