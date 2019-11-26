#!bin/bash

ray start --head --redis-port=3301
python -u kafka_py.py $1 $2 $3