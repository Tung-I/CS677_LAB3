#! /bin/bash
host=$1
port=$2
n_request=$3
prob=$4

python3 concurrent_client.py --host $host --port $port --n_request $n_request --prob $prob --seed 1 &
python3 concurrent_client.py --host $host --port $port --n_request $n_request --prob $prob --seed 2 &
python3 concurrent_client.py --host $host --port $port --n_request $n_request --prob $prob --seed 3 &
python3 concurrent_client.py --host $host --port $port --n_request $n_request --prob $prob --seed 4 &
python3 concurrent_client.py --host $host --port $port --n_request $n_request --prob $prob --seed 5
