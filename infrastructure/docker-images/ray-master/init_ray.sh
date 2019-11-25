#!bin/bash

ray start --head --redis-port=3301
python kafka_py.py $1 $2 $3