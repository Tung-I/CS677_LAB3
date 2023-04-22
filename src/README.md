## Environment
YAML Python Library
```shell
pip install PyYAML
```

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

## Design

### REST APIs
1.  Front-end

    The front-end service exposes 3 different REST APIs to the clients:

    *   `GET /stocks/<stock_name>`
    *   `POST /orders`
    *   `GET /orders/<order_number>`

    `GET /stocks/<stock_name>` allows clients to query the current price of stocks; `POST /orders` allows clients to place an order; `GET /orders/<order_number>` allows clients to query existing order records saved in the order server database.

2. Order

    The order service exposes the following APIs to allow the front-end to query and allow the replicas to synchronize with each other:

    *   `GET /orders/<order_number>`
    *   `POST /orders`
    *   `GET /synchronize`
    *   `POST /propagate`
    
    `GET /orders/<order_number>` returns the query order details to the frontend; `POST /orders` forwards the order request the the catalog server; `GET /synchronize` allows a replica coming back online from a crash to ask the other replicas what's the latest order number currently; `POST /propagate` allows the leader to propagate the information of a sucessful trade to the follower nodes. 

3. Catalog

    The catalog service exposes `POST /orders` to the order server and `GET /stocks/<stock_name>` to the front-end server as they were defined in lab2.