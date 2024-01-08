#!/bin/bash

export EUPS_USERDATA=/tmp/eups/`whoami`/

. /mnt/eups/linea_eups_setup.sh
setup gcc 4.9.3+1

gfortran -v
