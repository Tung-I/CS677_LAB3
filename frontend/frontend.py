import socket
import http.server
import requests
import argparse
import threading
from typing import Callable, Sequence, Union, List
from http.server import HTTPServer
from socketserver import ThreadingMixIn
import json
import sys, os
import select

from contextlib import contextmanager
from collections import OrderedDict
from threading  import Lock
from dotenv import load_dotenv



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


def leader_selection(order_host, order_socket_ports, timeout=0.1):
    # Try to connect to an order server, starting from the highest id
    leader_id = None
    while leader_id is None:
        for order_id in ['3', '2', '1']:
            # Create a socket
            port = order_socket_ports[order_id]
            s = socket.socket()
            # Ping the server and see if there is a receipt
            try:
                s.connect((order_host, port))
                s.send("Ping".encode("ascii"))
                data = s.recv(1024)
                if data.decode() == 'OK':
                    s.send("You win".encode("ascii"))
                    leader_id = order_id
                    print(f'Now send requests to Order ID {leader_id}')
                    s.close()
                    break
            except:
                continue

                
    # Notify all the replicas that a leader has been selected
    for order_id in ['3', '2', '1']:
        if order_id != leader_id:
            # Create a socket
            port = order_socket_ports[order_id]
            s = socket.socket()
            # Notify the leader
            # If no response, just continue
            try:
                s.connect((order_host, port))
                s.send(leader_id.encode("ascii"))
                
            except:
                continue

    return leader_id



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

        # Query transaction records
        elif self.path.startswith("/order?order_number"):
            order_number = self.path.split('=')[-1]


            # Forward the request to the order server
            response = None
            while response == None: 
                try:
                    order_port = self.server.order_request_ports[self.server.order_leader_id]
                    url = f'{self.server.order_host_url}:{order_port}/order?order_number={order_number}'
                    response = requests.get(url)
                except requests.exceptions.RequestException as e:
                    print(f'An request exception occurs: {e}. Re-select a leader.')
                    order_leader_id = leader_selection(self.server.order_host, self.server.order_socket_ports)
                    self.server.order_leader_id = order_leader_id

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

        # Invalidation request from Catalog
        elif self.path.startswith("/invalidation?stock="):
            stock_name = self.path.split('=')[-1]
            result = self.server.cache.pop(stock_name)

            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write("The item was successfully removed from the cache".encode())

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
                    order_port = self.server.order_request_ports[self.server.order_leader_id]
                    url = f'{self.server.order_host_url}:{order_port}/order?stock={stock_name}&amp;quantity={quantity}&amp;type={order_type}'
                    response = requests.post(url, data={"stock": stock_name, "quantity": quantity, "type": order_type})
                except requests.exceptions.RequestException as e:
                    print(f'An request exception occurs: {e}. Re-select a leader.')
                    order_leader_id = leader_selection(self.server.order_host, self.server.order_socket_ports)
                    self.server.order_leader_id = order_leader_id



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
    def __init__(self, host_port_tuple, streamhandler, args):
        super().__init__(host_port_tuple, streamhandler)
        self.order_host_url = f'http://{args.order_host}'
        self.catalog_host_url = f'http://{args.catalog_host}'
        self.catalog_port = args.catalog_port
        self.protocol_version = 'HTTP/1.1'
        self.cache = LRUCache(args.cache_size, args.log_path)
        self.order_host = args.order_host
        self.order_request_ports = {'3': args.order_request_port3, '2': args.order_request_port2, '1': args.order_request_port1}
        self.order_socket_ports = {'3': args.order_socket_port3, '2': args.order_socket_port2, '1': args.order_socket_port1}
        self.order_leader_id = None

    # Override the server_bind function to enable socket reuse and bind to the server address
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

def main(args):
    # Set up the threaded HTTP server with the given port and request handler.
    httpd = ThreadedHTTPServer(("", args.port), StockRequestHandler, args)
    # Select the leader order service
    order_leader_id = leader_selection(httpd.order_host, httpd.order_socket_ports)
    httpd.order_leader_id = order_leader_id
    print(f"Serving on port {args.port}")
    # Start serving requests.
    httpd.serve_forever()


if __name__ == "__main__":
    # Load env variables
    load_dotenv()

    # Parse command-line arguments.
    parser = argparse.ArgumentParser(description='Server.')

    # Assign the listening port
    parser.add_argument('--port', dest='port', help='Port', default=os.environ.get('FRONTEND_PORT'), type=int)

    # Assign the host and the port of the order service 
    parser.add_argument('--order_host', dest='order_host', help='Order Host', default=os.environ.get('ORDER_HOST'), type=str)
    parser.add_argument('--order_request_port1', dest='order_request_port1', help='The first request port', default=os.environ.get('ORDER_REQUEST_PORT1'), type=int)
    parser.add_argument('--order_request_port2', dest='order_request_port2', help='The second request port', default=os.environ.get('ORDER_REQUEST_PORT2'), type=int)
    parser.add_argument('--order_request_port3', dest='order_request_port3', help='The third request port', default=os.environ.get('ORDER_REQUEST_PORT3'), type=int)
    parser.add_argument('--order_socket_port1', dest='order_socket_port1', help='The first socket port', default=os.environ.get('ORDER_SOCKET_PORT1'), type=int)
    parser.add_argument('--order_socket_port2', dest='order_socket_port2', help='The second socket port', default=os.environ.get('ORDER_SOCKET_PORT2'), type=int)
    parser.add_argument('--order_socket_port3', dest='order_socket_port3', help='The third socket port', default=os.environ.get('ORDER_SOCKET_PORT3'), type=int)

    # Assign the host and the port of the catalog service 
    parser.add_argument('--catalog_host', dest='catalog_host', help='Catalog Host', default=os.environ.get('CATALOG_HOST'), type=str)
    parser.add_argument('--catalog_port', dest='catalog_port', help='Catalog Port', default=os.environ.get('CATALOG_PORT'), type=int)

    # Assign the size of the cache
    parser.add_argument('--cache_size', dest='cache_size', help='Size of the cache', default=5, type=int)

    # Assign the path of the cache log file for debugging 
    parser.add_argument('--log', dest='log_path', help='Path to the cache log', default="./log.json", type=str)
    
    args = parser.parse_args()

    # Start the server with the given arguments.
    main(args)
