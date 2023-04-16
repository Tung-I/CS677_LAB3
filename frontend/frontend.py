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

from collections import OrderedDict


class LRUCache:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.values = OrderedDict()

    def get(self, key: int) -> int:
        if key not in self.values:
            return -1
        else:
            self.values[key] = self.values.pop(key)
            return self.values[key]

    def put(self, key: int, value: int) -> None:
        if key not in self.values:
            if len(self.values) == self.capacity:
                self.values.popitem(last=False)
        else:
            self.values.pop(key)
        self.values[key] = value


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

            # Forward the request to Catalog using the extracted stock name
            url = f'{self.server.catalog_host_url}:{self.server.catalog_port}/lookup?stock={stock_name}'
            response = requests.get(url)

            # Send the response to the client
            if response.status_code == 200:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(response.content)

                # Update the cache
                self.server.cahce.

            elif response.status_code == 404:
                self.send_response(404)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(response.content)
            else:
                raise RuntimeError('Unknown status code')

        else:
            # The URL of the GET request does not start wtih "/lookup" -> raise error 404 
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


            # forward the request to Order using the extracted information
            url = f'{self.server.order_host_url}:{self.server.order_port}/order?stock={stock_name}&amp;quantity={quantity}&amp;type={order_type}'
            response = requests.post(url, data={"stock": stock_name, "quantity": quantity, "type": order_type})

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
        self.order_port = args.order_port
        self.catalog_host_url = f'http://{args.catalog_host}'
        self.catalog_port = args.catalog_port
        self.protocol_version = 'HTTP/1.1'
        self.cache = LRUCache(args.cache_size)

    # Override the server_bind function to enable socket reuse and bind to the server address
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

def main(args):
    # Set up the threaded HTTP server with the given port and request handler.
    httpd = ThreadedHTTPServer(("", args.port), StockRequestHandler, args)
    print(f"Serving on port {args.port}")
    # Start serving requests.
    httpd.serve_forever()


if __name__ == "__main__":
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(description='Server.')
    # Assign the listening port
    parser.add_argument('--port', dest='port', help='Port', default=os.environ.get('FRONTEND_LISTENING_PORT'), type=int)
    # Assign the host and the port of the order and the catalog services 
    parser.add_argument('--order_host', dest='order_host', help='Order Host', default=os.environ.get('ORDER_HOST'), type=str)
    parser.add_argument('--order_port', dest='order_port', help='Order Port', default=os.environ.get('ORDER_LISTENING_PORT'), type=int)
    parser.add_argument('--catalog_host', dest='catalog_host', help='Catalog Host', default=os.environ.get('CATALOG_HOST'), type=str)
    parser.add_argument('--catalog_port', dest='catalog_port', help='Catalog Port', default=os.environ.get('CATALOG_LISTENING_PORT'), type=int)
    # Assign the size of the cache
    args = parser.parse_args()
    parser.add_argument('--cache_size', dest='cache_size', help='Size of the cache', default=5, type=int)

    # Start the server with the given arguments.
    main(args)
