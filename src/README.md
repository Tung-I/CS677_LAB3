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
python quantity_test.py
```
Whenever there is a lookup request, the items inside the cache will be dump to /frontend/log.json, which can be used for part1 debugging.

2. Ctrl+C, shutdown the order server with the highest ID 

3. Send the testing requests again
```shell
python quantity_test.py
```
The order server with second highest ID will become the leader, and it can be seen that order3.csv is lagging behind.

4. Restart the order server with the highest ID
```shell
cd src/order
python order.py --id 3 --out_dir ./data --resume
```

5. Send the testing requests again
```shell
python quantity_test.py
```
Check each replica's database. All of them should be up-to-date.