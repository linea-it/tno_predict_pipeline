#!/bin/bash --login

_name=$1
_start=$2
_end=$3
_number=$4
_path=$5

TMPDIR=`echo $RANDOM | md5sum | head -c 5; echo;`
export DIR_DATA=/tmp/$TMPDIR
echo 'DIR_DATA: '$DIR_DATA

export APP_PATH_ORI=$PIPELINE_PATH
export APP_PATH=$_path/run-$TMPDIR
export PATH=$PATH:$APP_PATH_ORI/bin
echo 'APP_PATH: '$APP_PATH

mkdir $APP_PATH
cd $APP_PATH

ln -s $APP_PATH_ORI/naif0012.tls .
ln -s $APP_PATH_ORI/de440.bsp .
ln -s $APP_PATH_ORI/*.py .
ln -s $APP_PATH_ORI/praia_occ_input_template.txt .
ln -s $APP_PATH_ORI/nima_input_template.txt .

shift $#

source $PIPELINE_PATH/env_pipeline.sh

python run.py $_name $_start $_end --number $_number --path $_path

rm $DIR_DATA

echo "Done!"
