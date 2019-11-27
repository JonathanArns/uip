#!bin/bash

ray start --head --redis-port=3301
python -u kafka_py.py -in $1 -out $2 -broker $3