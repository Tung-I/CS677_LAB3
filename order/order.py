import json
import sys, os
import socket
import http.server
import requests
import argparse
import threading
from typing import Callable, Sequence, Union, List
from http.server import HTTPServer
from contextlib import contextmanager
from threading  import Lock
from socketserver import ThreadingMixIn
import csv


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



def lookup(
    file_path: str,
    order_number: str,
    rwlock: RWLock
    ):
    """
    Return:
        returns the corresponding order details, or -1 if the number doesn't exist.
    """
    with rwlock.r_locked():
        with open(file_path, newline='') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if row[0] == order_number:
                    return row
            return -1
        

# Define a request handler that inherits from BaseHTTPRequestHandler
class OrderRequestHandler(http.server.BaseHTTPRequestHandler):
    # Handle the order number query from the frontend 
    def do_GET(self):   
        # If the request is for stock lookup
        if self.path.startswith("/order?order_number"):   
            # Check the validity of URL
            if self.path.split('?')[0] != '/order' or self.path.split('?')[-1].split('=')[0] != 'order_number':
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

            order_number = self.path.partition("=")[-1]

            # Look up the order number
            lookup_result = lookup(os.path.join(self.server.out_dir, 'order.csv'), order_number, self.server.rwlock)

            # If the order number is not found, return an error response
            if lookup_result == -1:
                self.send_response(404)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                response = {
                    "error": {
                        "code": 404,
                        "message": "Order number not found",
                    }
                }

            # If the order_number is found, return the data as a response
            else:
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                response = {
                    "data": {
                        "number": order_number,
                        "name": lookup_result[1],
                        "type": lookup_result[-1],
                        "quantity": lookup_result[2],
                    }
                }

            self.wfile.write(json.dumps(response).encode())
        
        # If the URL does not start with "/order?order_number"
        else:
            self.send_response(400)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            response = {
                "error": {
                    "code": 400, 
                    "message": "invalid URL"
                }
            }
            self.wfile.write(json.dumps(response).encode())



    # Handle POST requests
    def do_POST(self):
        # Handle stock order request
        # Get the length of the content of the request from the headers
        content_length = int(self.headers["Content-Length"])
        # Read the request content
        request_body = self.rfile.read(content_length).decode().split('&')  # ex. stock=AAPL&quantity=100&type=sell
        # Check the validity of URL
        if request_body[0].split('=')[0] != "stock" or request_body[1].split('=')[0] != "quantity" \
            or request_body[2].split('=')[0] != "type":

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

        # Extract the values of the parameters from the request_body
        stock_name = request_body[0].split('=')[-1]
        quantity = float(request_body[1].split('=')[-1])
        order_type = request_body[2].split('=')[-1]

        # Check if the quantity is valid
        if quantity <= 0:
            # If the quantity is not valid, send a 400 Bad Request response with an error message
            self.send_response(400)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            response = {
                "error": {
                    "code": 400, 
                    "message": "invalid order: invalid quantity"
                }
            }
            self.wfile.write(json.dumps(response).encode())
            return

        # Check if the order type is valid
        if order_type not in ["buy", "sell"]:
            # If the order type is not valid, send a 400 Bad Request response with an error message
            self.send_response(400)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            response = {
                "error": {
                    "code": 400, 
                    "message": "invalid order: invalid order type"
                }
            }
            self.wfile.write(json.dumps(response).encode())
            return

        # Use a write lock to ensure only one thread can
        # check the remaining quantity and place an order at a time 
        with self.server.rwlock.w_locked():
            # Call Lookup() in the catalog service so that we can check the stock availability
            url = f'{self.server.catalog_host_url}:{self.server.catalog_port}/lookup?stock={stock_name}'
            lookup_response = requests.get(url)

            # If Lookup() in the catalog service fails (stock not found)
            if lookup_response.status_code != 200:
                # If the stock is not found, send a 404 Not Found response with an error message
                self.send_response(404)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                response = {
                    "error": {
                        "code": 404, 
                        "message": "stock not found"
                    }
                }
                self.wfile.write(json.dumps(response).encode())
                return

            # Forward 'buy' requests to the catalog service
            if order_type == 'buy':
                # Check if there are enough stocks available for sale in the catalog service
                remaining_quantity = float(lookup_response.json()["data"]['quantity'])
                # If the remaining quantity is enough, place the order
                if remaining_quantity >= quantity:
                    # Place the order by contacting the catalog service
                    url = f'{self.server.catalog_host_url}:{self.server.catalog_port}/order?stock={stock_name}&amp;quantity={-quantity}&amp;type={order_type}'
                    trading_response = requests.post(url, data={"stock": stock_name, "quantity": -quantity, "type": order_type})

                    # Double check if the order is successful
                    if trading_response.status_code != 200:
                        raise RuntimeError(f"Invalid order {trading_response.status_code}: the order service should check the order for the catalog service.")
                
                    # Update the order log
                    self.server.orders.append(
                        {
                            "transaction number": int(self.server.transaction_number),
                            "stock name": stock_name,
                            "order type": order_type,
                            "quantity": quantity
                        }
                    )
                    # Write the order log into orders.csv
                    with open(os.path.join(self.server.out_dir, 'order.csv'), mode='w', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['transaction number', 'stock name', 'quantity', 'order type'])
                        for order in self.server.orders:
                            writer.writerow([order['transaction number'], order['stock name'], order['quantity'], order['order type']])

                    # Send a successful response to the client
                    self.send_response(200)
                    self.send_header("Content-Disposition", "attachment; filename=order.csv")
                    self.end_headers()
                    response = {
                        "data":{
                            "transaction_number": int(self.server.transaction_number)
                        }
                    }
                    self.wfile.write(json.dumps(response).encode())

                    # Increase the transaction number
                    self.server.transaction_number += 1

                # Send an error response to the client if there's not enough remaining quantity
                else:
                    self.send_response(400)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    response = {
                        "error": {
                            "code": 400, 
                            "message": "invalid order: excessive trading"
                        }
                    }
                    self.wfile.write(json.dumps(response).encode())


            # Forward 'sell' requests to the catalog service
            elif order_type == 'sell':
                # Place the order by contacting the catalog service
                url = f'{self.server.catalog_host_url}:{self.server.catalog_port}/order?stock={stock_name}&amp;quantity={quantity}&amp;type={order_type}'
                trading_response = requests.post(url, data={"stock": stock_name, "quantity": quantity, "type": order_type})

                # Double check if the order is successful
                if trading_response.status_code != 200:
                    raise RuntimeError("Invalid order {trading_response.status_code}: the order service should check the order for the catalog service.")
                
                # Send a successful response to the client
                self.send_response(200)
                self.send_header("Content-Disposition", "attachment; filename=order.csv")
                self.end_headers()
                response = {
                        "data":{
                            "transaction_number": int(self.server.transaction_number)
                        }
                }
                self.wfile.write(json.dumps(response).encode())

                # Update the order log
                self.server.orders.append(
                    {
                        "transaction number": int(self.server.transaction_number),
                        "stock name": stock_name,
                        "order type": order_type,
                        "quantity": quantity
                    }
                )
                with open(os.path.join(self.server.out_dir, 'order.csv'), mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(['transaction number', 'stock name', 'quantity', 'order type'])
                    for order in self.server.orders:
                        writer.writerow([order['transaction number'], order['stock name'], order['quantity'], order['order type']])         
                # Increase the transaction number
                self.server.transaction_number += 1

            # Send an error response to the client if the order type is invalid
            else:
                self.send_response(400)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                response = {
                    "error": {
                        "code": 400, 
                        "message": "invalid order type"
                    }
                }
                self.wfile.write(json.dumps(response).encode())
        
# Create a class that inherits from ThreadingMixIn and HTTPServer to implement a threaded HTTP server
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    # Override the init function to save metadata in the server
    def __init__(self, host_port_tuple, streamhandler, args):
        super().__init__(host_port_tuple, streamhandler)
        # Set the output directory and initialize the transaction number and orders
        self.out_dir = args.out_dir
        self.catalog_host_url = f'http://{args.catalog_host}'
        self.catalog_port = args.catalog_port
        self.transaction_number = 0
        self.orders = []
        self.rwlock = RWLock()
        self.protocol_version = 'HTTP/1.1'
    # Override the server_bind function to enable socket reuse and bind to the server address
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

# Define the main function to start the server
def main(args):
    # Create a threaded HTTP server with the given port and output directory
    httpd = ThreadedHTTPServer(("", args.port), OrderRequestHandler, args)
    # Print a message to indicate that the server is serving on the specified port
    print(f"Serving on port {args.port}")
    # Start the server and keep it running indefinitely
    httpd.serve_forever()

# Check if the script is being run as the main module
if __name__ == "__main__":
    # Create an argument parser with options for the port and output directory
    parser = argparse.ArgumentParser(description='Order Server.')
    # Assign the listening port
    parser.add_argument('--port', dest='port', help='Port', default=os.environ.get('ORDER_LISTENING_PORT'), type=int)
    # Assign the directory of orders.csv
    parser.add_argument('--out_dir', dest='out_dir', help='Output directory', default=os.environ.get('OUTPUT_DIR'), type=str)
    # Assign the host and the port of the catalog service 
    parser.add_argument('--catalog_host', dest='catalog_host', help='Catalog Host', default=os.environ.get('CATALOG_HOST'), type=str)
    parser.add_argument('--catalog_port', dest='catalog_port', help='Catalog Port', default=os.environ.get('CATALOG_LISTENING_PORT'), type=str)
    
    args = parser.parse_args()


    main(args)