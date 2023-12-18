#!/bin/bash --login

conda activate py2
python /app/pipe2.7/pipeline.py

ulimit -s 100000

# geradata