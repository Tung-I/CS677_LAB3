## Environment
pip install python-dotenv

## How to run the servers
1. Run a catalog service
```shell
cd src/catalog
python catalog.py --out_dir ./data 
```

2. Run three order services
```shell
cd src/order
python order.py --id 3 --out_dir ./data
```
```shell
cd src/order
python order.py --id 2 --out_dir ./data
```
```shell
cd src/order
python order.py --id 1 --out_dir ./data
```

3. Run a frontend service
```shell
cd src/frontend
python frontend.py --cache_size 3 --log ./log.json
```

## How to test
1. Send the first set of testing requests
```shell
cd src/client
python frontend.py --cache_size 3 --log ./log.json
```
2. Ctrl+C, shutdown the highest ID order server 
3. Send the second set of testing requests
```shell
python frontend.py --cache_size 3 --log ./log.json
```
The order server with second highest ID will become the leader, and records in order3.csv is lagging behind.
4. Restart the order server with the highest ID
```shell
cd src/order
python order.py --id 3 --out_dir ./data --resume
```
3. Send the third set of testing requests
```shell
python frontend.py --cache_size 3 --log ./log.json
```
Now the database of each replica is up-to-date