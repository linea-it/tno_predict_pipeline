#!/bin/bash --login

echo "Environment: "$PARSL_ENV

if [[ "$PARSL_ENV" = "linea" ]]
then
    echo "Setup remote env at LIneA(Slurm)..."
    export EUPS_USERDATA=/tmp/`whoami`/eups
    . /mnt/eups/linea_eups_setup.sh
    setup gcc 4.9.3+1
    export PATH=$PATH:$PIPELINE_PATH/bin
fi

conda activate py2

ulimit -s 100000
ulimit -u 100000