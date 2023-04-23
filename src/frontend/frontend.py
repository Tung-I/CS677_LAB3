import socket
import http.server
import requests
import argparse
import threading
from typing import Callable, Sequence, Union, List
from http.server import HTTPServer
from socketserver import ThreadingMixIn
import json
import time
import yaml
import sys, os
import select

from contextlib import contextmanager
from collections import OrderedDict
from threading  import Lock


class RWLock(object):
    """ RWLock class based on https://gist.github.com/tylerneylon/a7ff6017b7a1f9a506cf75aa23eacfd6
        Usage:
            my_obj_rwlock = RWLock()

            # When reading from my_obj:
            with my_obj_rwlock.r_locked():
                do_read_only_things_with(my_obj)

            # When writing to my_obj:
            with my_obj_rwlock.w_locked():
                mutate(my_obj)
    """
    def __init__(self):
        self.w_lock = Lock()
        self.num_r_lock = Lock()
        self.num_r = 0

    # ___________________________________________________________________
    # Reading methods.
    def r_acquire(self):
        self.num_r_lock.acquire()
        self.num_r += 1
        if self.num_r == 1:
            self.w_lock.acquire()
        self.num_r_lock.release()

    def r_release(self):
        assert self.num_r > 0
        self.num_r_lock.acquire()
        self.num_r -= 1
        if self.num_r == 0:
            self.w_lock.release()
        self.num_r_lock.release()

    @contextmanager
    def r_locked(self):
        try:
            self.r_acquire()
            yield
        finally:
            self.r_release()

    # ___________________________________________________________________
    # Writing methods.

    def w_acquire(self):
        self.w_lock.acquire()

    def w_release(self):
        self.w_lock.release()

    @contextmanager
    def w_locked(self):
        try:
            self.w_acquire()
            yield
        finally:
            self.w_release()



class LRUCache:
    def __init__(self, capacity: int, log_path: str):
        self.capacity = capacity
        self.items = OrderedDict()
        self.rwlock = RWLock()
        self.timestamp = 0
        self.log_path = log_path

        # Initialize the log file
        with open(self.log_path, 'w', encoding='utf-8') as f:
            json.dump({}, f, ensure_ascii=False, indent=4)

    def get(self, key):
        """
        Return:
            returns -1 if the key is not found
        """
        if key not in self.items:
            return -1
        else:
            # Pop the item before putting it into the cache
            # So the item can be the newest in the cache
            with self.rwlock.w_locked():
                self.items[key] = self.items.pop(key)
                return self.items[key]

    def put(self, key, item):
        with self.rwlock.w_locked():
            if key not in self.items:
                if len(self.items) == self.capacity:
                    self.items.popitem(last=False)
            else:
                # Pop the item before putting it into the cache
                self.items.pop(key)
            self.items[key] = item

    def pop(self, key):
        """
        Return:
            returns 1 if operation succeeds, -1 if the key is not found
        """
        with self.rwlock.w_locked():
            if key not in self.items:
                return -1
            else:
                self.items.pop(key)
                return 1

    def dump(self):
        with self.rwlock.r_locked():
            with open(self.log_path, 'r') as f:
                data = json.load(f)

        data[str(self.timestamp)] = self.items

        with self.rwlock.w_locked():
            with open(self.log_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                self.timestamp += 1


def leader_selection(server):
    # Try to connect to an order server, starting from the highest id
    server.leader_id = None
    while server.leader_id is None:
        for order_id in ['3', '2', '1']:
            # Create a socket
            order_host, order_port = server.order_leader_broadcast_addrs[order_id]
            s = socket.socket()
            # Ping the server and see if there is a receipt
            try:
                s.connect((order_host, order_port))
                s.send("Ping".encode("ascii"))
                data = s.recv(1024)
                # If receive 'OK', pick it as the leader
                if data.decode() == 'OK':
                    s.send("You win".encode("ascii"))
                    server.leader_id = order_id
                    print(f'Now send requests to Order ID {server.leader_id}')
                    s.close()
                    break
            except:
                continue

    # Notify all the replicas that the leader has been selected
    for order_id in ['3', '2', '1']:
        if order_id != server.leader_id:
            # Create a socket
            order_host, order_port = server.order_leader_broadcast_addrs[order_id]
            s = socket.socket()
            # If no response, just continue
            try:
                s.connect((order_host, order_port))
                s.send(server.leader_id.encode("ascii"))      
            except:
                continue


def health_check(server, time_interval):
    """
    Active health check.
    If the leader is dead, re-do leader selection
    """
    while True:
        time.sleep(time_interval)
        leader_host, leader_port = server.order_health_check_addrs[server.leader_id]
        # Send a health check
        try:
            # print(f'Check {leader_host, leader_port}')
            s = socket.socket()
            s.connect((leader_host, leader_port))
            s.send("Ping".encode("ascii"))
            data = s.recv(1024)
            # If receive 'OK', the leader is alive
            if data.decode() == 'OK':
                s.close()
            else:
                raise Exception('No response from the leader.')
        except:
            # re-select leader
            with server.rwlock.w_locked():
                leader_selection(server)


# Define a stock request handler class that handles HTTP GET and POST requests
class StockRequestHandler(http.server.BaseHTTPRequestHandler):
    # override the default do_GET() method
    def do_GET(self):
        # Handle a GET request.
        if self.path.startswith('/lookup'):

            # Check the validity of URL
            if self.path.split('?')[0] != '/lookup' or self.path.split('?')[-1].split('=')[0] != 'stock':
                self.send_response(404)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                response = {
                    "error": {
                        "code": 404, 
                        "message": f"invalid URL: {self.path}"
                    }
                }
                self.wfile.write(json.dumps(response).encode())
                return

            # Extract the stock name from the URL query parameter
            stock_name = self.path.split('=')[-1]

            # Check whether the request can be served from the cache
            cache_item = self.server.cache.get(stock_name)

            # If it's a cache miss
            if cache_item  == -1:
                # Forward the request to Catalog 
                url = f'{self.server.catalog_host_url}:{self.server.catalog_port}/lookup?stock={stock_name}'
                response = requests.get(url)

                # Check the response 
                if response.status_code == 200:
                    # Send the response to the client
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(response.content)

                    # Update the cache
                    response_content = json.loads(response.content.decode())
                    item_to_save = {
                        "name": response_content['data']["name"],
                        "price": response_content['data']["price"],
                        "quantity": response_content['data']["quantity"]
                    }
                    self.server.cache.put(stock_name, item_to_save)

                # If the stock name does not exist in the catalog
                elif response.status_code == 404:
                    self.send_response(404)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(response.content)
                    return
                else:
                    raise RuntimeError('Unknown status code')

            # If it's a cache hit
            else:
                response = {
                    "data": {
                        "name": cache_item ["name"],
                        "price": cache_item ["price"],
                        "quantity": cache_item ["quantity"]
                    }
                }
                # Send the response to the client
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response).encode())

            # Dump the log after every lookup request
            self.server.cache.dump()

        # Query existing orders
        elif self.path.startswith("/order?order_number"):
            order_number = self.path.split('=')[-1]

            # Forward the request to the order server
            # The leader order server could be dead, so a try-except section is used to capture the connection error
            # If there is no response from the server, redo the leader selection and then send the request again 
            response = None
            while response == None: 
                try:
                    leader_host, leader_port = self.server.order_request_addrs[self.server.leader_id]
                    url = f'http://{leader_host}:{leader_port}/order?order_number={order_number}'
                    response = requests.get(url)
                except requests.exceptions.RequestException as e:
                    # Passive health check
                    print(f'An request exception occurs: {e}. Re-select a leader.')
                    with self.server.rwlock.w_locked():
                        leader_selection(self.server)
                    

            # If the order number exists
            if response.status_code == 200:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(response.content)
                return
            # If the order number does not exist
            elif response.status_code == 404:
                self.send_response(404)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(response.content)
                return
            # If the URL is invalid
            else:
                raise RuntimeError("Frontend should check the URL for the order service")

        # The URL of the GET request is invalid -> raise error 404 
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            response = {
                "error": {
                    "code": 404, 
                    "message": f"invalid URL: {self.path}"
                }
            }
            self.wfile.write(json.dumps(response).encode())


        

    # override the default do_POST() method
    def do_POST(self):

        # Handle a POST request.
        if self.path.startswith('/order'):

            # Check the validity of URL
            params = self.path.split('?')[1].split('&amp;')  # ex. stock=AAPL&amp;quantity=-100&amp;type=buy
            if self.path.split('?')[0] != '/order' or params[0].split('=')[0] != 'stock' \
                or params[1].split('=')[0] != 'quantity' or params[2].split('=')[0] != 'type':
                self.send_response(400)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                response = {
                    "error": {
                        "code": 400, 
                        "message": f"invalid URL: {self.path}"
                    }
                }
                self.wfile.write(json.dumps(response).encode())
                return

            # If URL is valid, extract the stock name, quantity and trade type from the URL query parameter
            try:
                stock_name = params[0].split('=')[1]
                quantity = int(params[1].split('=')[1])
                order_type = params[2].split('=')[1]
            except:
                self.send_response(400)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                response = {
                    "error": {
                        "code": 400, 
                        "message": f"invalid URL: {self.path}"
                    }
                }
                self.wfile.write(json.dumps(response).encode())
                return


            # Forward the request to the order server
            response = None
            while response == None: 
                try:
                    leader_host, leader_port = self.server.order_request_addrs[self.server.leader_id]
                    url = f'http://{leader_host}:{leader_port}/order?stock={stock_name}&amp;quantity={quantity}&amp;type={order_type}'
                    response = requests.post(url, data={"stock": stock_name, "quantity": quantity, "type": order_type})
                except requests.exceptions.RequestException as e:
                    # Passive health check
                    print(f'An request exception occurs: {e}. Re-select a leader.')
                    with self.server.rwlock.w_locked():
                        leader_selection(self.server)

            # Send the response to the client
            if response.status_code == 200:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(response.content)
            elif response.status_code == 404:
                self.send_response(404)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(response.content)
            elif response.status_code == 400:
                self.send_response(400)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(response.content)
            else:
                raise RuntimeError('Unknown status code')

        # Invalidation request from Catalog
        elif self.path.startswith("/invalidation?stock="):
            stock_name = self.path.split('=')[-1]
            result = self.server.cache.pop(stock_name)

            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write("The item was successfully removed from the cache".encode())

        else:
            # The URL of the POST request does not start wtih "/order" -> raise error 404 
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            response = {
                "error": {
                    "code": 404, 
                    "message": f"invalid URL: {self.path}"
                }
            }
            self.wfile.write(json.dumps(response).encode())

# Define a threaded HTTP server that allows for multiple concurrent requests.
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
     # Override the init function to save metadata in the server
    def __init__(self, host_port_tuple, streamhandler, config):
        super().__init__(host_port_tuple, streamhandler)
        self.protocol_version = 'HTTP/1.1'
        # Initialize cache
        self.cache = LRUCache(config["CACHE_SIZE"], config["CACHE_LOG_PATH"])
        # Order request address
        self.order_request_addrs = {
            '3': (config["ORDER_HOST3"], config["ORDER_PORT3"]),
            '2': (config["ORDER_HOST2"], config["ORDER_PORT2"]),
            '1': (config["ORDER_HOST1"], config["ORDER_PORT1"])
        }
        # Catalog request address
        self.catalog_host = config["CATALOG_HOST"]
        self.catalog_host_url = f'http://{self.catalog_host}'
        self.catalog_port = config["CATALOG_PORT"]
        # Leader selection broadcast address
        self.order_leader_broadcast_addrs = {
            '3': (config["ORDER_HOST3"], config["ORDER_LEADER_BROADCAST_PORT3"]),
            '2': (config["ORDER_HOST2"], config["ORDER_LEADER_BROADCAST_PORT2"]),
            '1': (config["ORDER_HOST1"], config["ORDER_LEADER_BROADCAST_PORT1"])
        }
        # Regular health check address
        self.order_health_check_addrs = {
            '3': (config["ORDER_HOST3"], config["ORDER_HEALTH_CHECK_PORT3"]),
            '2': (config["ORDER_HOST2"], config["ORDER_HEALTH_CHECK_PORT2"]),
            '1': (config["ORDER_HOST1"], config["ORDER_HEALTH_CHECK_PORT1"])
        }
        self.leader_id = None
        # Read-write lock
        self.rwlock = RWLock()

    # Override the server_bind function to enable socket reuse and bind to the server address
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

def main(args):
    # Creat a config object
    # Load a config file
    if args.config_path:
        with open(args.config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
    # Use env variables
    else:
        config = {}
        config["CATALOG_HOST"] = os.environ.get("CATALOG_HOST")
        config["CATALOG_PORT"] = int(os.environ.get("CATALOG_PORT"))

        config["ORDER_HOST3"] = os.environ.get("ORDER_HOST3")
        config["ORDER_PORT3"] = int(os.environ.get("ORDER_PORT3"))
        config["ORDER_LEADER_BROADCAST_PORT3"] = int(os.environ.get("ORDER_LEADER_BROADCAST_PORT3"))
        config["ORDER_HEALTH_CHECK_PORT3"] = int(os.environ.get("ORDER_HEALTH_CHECK_PORT3"))

        config["ORDER_HOST2"] = os.environ.get("ORDER_HOST2")
        config["ORDER_PORT2"] = int(os.environ.get("ORDER_PORT2"))
        config["ORDER_LEADER_BROADCAST_PORT2"] = int(os.environ.get("ORDER_LEADER_BROADCAST_PORT2"))
        config["ORDER_HEALTH_CHECK_PORT2"] = int(os.environ.get("ORDER_HEALTH_CHECK_PORT2"))

        config["ORDER_HOST1"] = os.environ.get("ORDER_HOST1")
        config["ORDER_PORT1"] = int(os.environ.get("ORDER_PORT1"))
        config["ORDER_LEADER_BROADCAST_PORT1"] = int(os.environ.get("ORDER_LEADER_BROADCAST_PORT1"))
        config["ORDER_HEALTH_CHECK_PORT1"] = int(os.environ.get("ORDER_HEALTH_CHECK_PORT1"))

        config["FRONTEND_HOST"] = os.environ.get("FRONTEND_HOST")
        config["FRONTEND_PORT"] = int(os.environ.get("FRONTEND_PORT"))

        config["OUTPUT_DIR"] = os.environ.get("OUTPUT_DIR")
        config["HEALTH_CHECK_INTERVAL"] = os.environ.get("HEALTH_CHECK_INTERVAL")
        config["CACHE_SIZE"] = os.environ.get("CACHE_SIZE")
        config["CACHE_LOG_PATH"] = os.environ.get("CACHE_LOG_PATH")


    # Set up the threaded HTTP server with the given port and request handler.
    httpd = ThreadedHTTPServer(("", config['FRONTEND_PORT']), StockRequestHandler, config)
    
    # Select the leader order service
    leader_selection(httpd)
    print(f"Serving on port {config['FRONTEND_PORT']}")

    # Run a daemon thread to actively send health check to the leader
    t = threading.Thread(target=health_check, daemon=True, args=[httpd, float(config['HEALTH_CHECK_INTERVAL'])])
    t.start()

    # Start serving requests.
    httpd.serve_forever()


if __name__ == "__main__":
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(description='Server.')
     # Load variables from config.yaml
    parser.add_argument('--config_path', dest='config_path', help='Path to config.yaml', default=None, type=str)    
    args = parser.parse_args()

    # Start the server with the given arguments.
    main(args)
