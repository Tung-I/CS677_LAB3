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

from dotenv import load_dotenv


class RWLock(object):
    def __init__(self):
        self.w_lock = Lock()
        self.num_r_lock = Lock()
        self.num_r = 0

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


def lookup( file_path: str, order_number: str, rwlock: RWLock):
    """
    Return:
        returns the corresponding order record, -1 if the order number doesn't exist.
    """
    with rwlock.r_locked():
        with open(file_path, newline='') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if row[0] == order_number:
                    return row
            return -1
        


def daemon_worker(port, server):
    """
    For the daemon thread running in the background.
    It update the leader information whenever receiving the notification from the frontend.
    """
    # Create the socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    s.listen(5)
    print(f"Listening to: {port}")
    # Listen to the message
    while True:
        conn, info = s.accept()
        data = conn.recv(1024)
        while data:
            # Ping from the frontend
            if data.decode() == 'Ping':
                # Send OK mmessage back
                conn.send('OK'.encode("ascii"))
            # Be nominated as the leader
            elif data.decode() == 'You win':
                print('I am the leader now.')
                server.current_leader_id = server.service_id
            # Be aware of who's the leader
            else:
                print(f'Order ID {data.decode()} is the leader now.')
                server.current_leader_id = data.decode()
            # Block for the next message
            data = conn.recv(1024)


# Define a request handler that inherits from BaseHTTPRequestHandler
class OrderRequestHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):  
        # The query of order records
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

            # Look up the order number
            order_number = self.path.partition("=")[-1]
            lookup_result = lookup(os.path.join(self.server.out_dir, f'order{self.server.service_id}.csv'), order_number, self.server.rwlock)

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

            # Otherwise, return the record as a response
            else:
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                response = {
                    "data": {
                        "number": order_number,
                        "name": lookup_result[1],
                        "type": lookup_result[3],
                        "quantity": lookup_result[2],
                    }
                }

            self.wfile.write(json.dumps(response).encode())
        
        # Request of the current transaction number from other replicas
        elif self.path.startswith("/synchronization"):   
            response = str(self.server.transaction_number)
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(response.encode())

        # Invalid URL format
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


    def do_POST(self):
        # If this is a non-leader server
        if self.server.current_leader_id != self.server.service_id:
            # Information propagation for the most recent trading record
            if self.path.startswith('/propagate'):
                # Parse the request
                content_length = int(self.headers["Content-Length"])
                request_body = self.rfile.read(content_length).decode().split('&')  # ex. stock=AAPL, quantity=100, type=sell, number=5
                stock_name = request_body[0].split('=')[-1]
                quantity = float(request_body[1].split('=')[-1])
                order_type = request_body[2].split('=')[-1]
                order_number = request_body[3].split('=')[-1]

                # Save the new info into the local database
                self.server.orders.append(
                    {
                        "transaction number": int(order_number),
                        "stock name": stock_name,
                        "order type": order_type,
                        "quantity": abs(quantity)
                    }
                )
                self.server.transaction_number = int(order_number) + 1
                # Sort the list of order records
                self.server.orders = sorted(self.server.orders, key=lambda x: x["transaction number"], reverse=False)
                # Write the file
                with open(self.server.local_data_path, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    for order in self.server.orders:
                        writer.writerow([order['transaction number'], order['stock name'], order['quantity'], order['order type']])
                # Send the response to the leader
                self.send_response(200)
                self.send_header("Content-Disposition", "attachment; filename=order.csv")
                self.end_headers()
                response = "Information update successfully"
                self.wfile.write(response.encode())          
                return

            # Unknown URL received by a non-leader order service
            else:
                raise RuntimeError(f'Unknown URL received by a non-leader order service: {self.path}')


        # If this is a leader server
        # Read the request content
        content_length = int(self.headers["Content-Length"])
        request_body = self.rfile.read(content_length).decode().split('&')  # ex. stock=AAPL, quantity=100, type=sell
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

        # Use a write lock to ensure only one thread can check the remaining quantity and place an order at a time 
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
                    with open(self.server.local_data_path, mode='w', newline='') as file:
                        writer = csv.writer(file)
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

                    # Propagate the info to the follower nodes
                    for port in self.server.peer_request_ports:
                        try:
                            # Send an update request to the peer
                            url = f'http://{self.server.order_host}:{port}/propagate?stock={stock_name}&amp;quantity={-quantity}&amp;type={order_type}&amp;number={self.server.transaction_number}'
                            peer_response = requests.post(url, data={"stock": stock_name, "quantity": -quantity, "type": order_type, "number": self.server.transaction_number})
                        except requests.exceptions.RequestException as e:
                            continue

                    # Increase the transaction number
                    self.server.transaction_number += 1

                # If there's not enough remaining quantity
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
                with open(self.server.local_data_path, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    for order in self.server.orders:
                        writer.writerow([order['transaction number'], order['stock name'], order['quantity'], order['order type']])         
    
                # Propagate the info to the follower nodes
                for port in self.server.peer_request_ports:
                    try:
                        # Send an update request to the peer
                        url = f'http://{self.server.order_host}:{port}/propagate?stock={stock_name}&amp;quantity={quantity}&amp;type={order_type}&amp;number={self.server.transaction_number}'
                        peer_response = requests.post(url, data={"stock": stock_name, "quantity": quantity, "type": order_type, "number": self.server.transaction_number})
                    except requests.exceptions.RequestException as e:
                        continue

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
        self.protocol_version = 'HTTP/1.1'
        # Order records
        self.transaction_number = 0
        self.orders = []
        # Read-write lock
        self.rwlock = RWLock()
        # Leader node information
        self.current_leader_id = None
        self.service_id = args.service_id
        # Path to the database file
        self.out_dir = args.out_dir
        self.local_data_path = os.path.join(self.out_dir, f'order{self.service_id}.csv')
        # Port and Host information
        self.catalog_host_url = f'http://{args.catalog_host}'
        self.catalog_port = args.catalog_port
        self.order_host = os.environ.get('ORDER_HOST')
        if self.service_id == '3':
            self.request_listening_port = int(os.environ.get('ORDER_REQUEST_PORT3'))
            self.socket_listening_port = int(os.environ.get('ORDER_SOCKET_PORT3'))
            self.peer_request_ports = [int(os.environ.get('ORDER_REQUEST_PORT2')), int(os.environ.get('ORDER_REQUEST_PORT1'))]
            self.peer_socket_ports = [int(os.environ.get('ORDER_SOCKET_PORT2')), int(os.environ.get('ORDER_SOCKET_PORT1'))]
        elif self.service_id == '2':
            self.request_listening_port = int(os.environ.get('ORDER_REQUEST_PORT2'))
            self.socket_listening_port = int(os.environ.get('ORDER_SOCKET_PORT2'))
            self.peer_request_ports = [int(os.environ.get('ORDER_REQUEST_PORT3')), int(os.environ.get('ORDER_REQUEST_PORT1'))]
            self.peer_socket_ports = [int(os.environ.get('ORDER_SOCKET_PORT3')), int(os.environ.get('ORDER_SOCKET_PORT1'))]
        elif self.service_id == '1':
            self.request_listening_port = int(os.environ.get('ORDER_REQUEST_PORT1'))
            self.socket_listening_port = int(os.environ.get('ORDER_SOCKET_PORT1'))
            self.peer_request_ports = [int(os.environ.get('ORDER_REQUEST_PORT3')), int(os.environ.get('ORDER_REQUEST_PORT2'))]
            self.peer_socket_ports = [int(os.environ.get('ORDER_SOCKET_PORT3')), int(os.environ.get('ORDER_SOCKET_PORT2'))]
        else:
            raise RuntimeError("Invalid argument: service id {args.service_id}")
        # Run from scratch or resume from a crash
        if args.resume:
            self.resume()
        else:
            with open(self.local_data_path, mode='w', newline='') as file:
                pass



    # Override the server_bind function to enable socket reuse and bind to the server address
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

    # Resume after a crash
    def resume(self):
        # Look at the local database and get the latest transaction number before crash
        self.orders = []
        with self.rwlock.r_locked():
            with open(self.local_data_path, newline='') as f:
                csv_reader = csv.reader(f)
                for row in csv_reader:
                    # Recover the memory
                    if len(row) == 0:
                        continue
                    self.orders.append(
                        {
                            "transaction number": int(row[0]),
                            "stock name": row[1],
                            "order type": row[3],
                            "quantity": row[2]
                        }
                    )
            # Recover the local order number
            if len(self.orders) != 0:
                self.transaction_number = self.orders[-1]["transaction number"] + 1
            else:
                self.transaction_number = 0

        # Find the latest order number from the other replicas
        for port in self.peer_request_ports:
            response = None
            url = f'http://{self.order_host}:{port}/synchronization'
            try:
                response = requests.get(url)
            except requests.exceptions.RequestException as e:
                continue
            latest_order_number = int(response.content.decode())
            
            # If an update is needed
            print(latest_order_number, self.transaction_number)
            if latest_order_number > self.transaction_number:
                # Start scynchronization
                for order_number_to_ask in range(self.transaction_number, latest_order_number):
                    # Try to send a request to the replica 
                    try:
                        url = f'http://{self.order_host}:{port}/order?order_number={order_number_to_ask}'
                        response = requests.get(url)
                        # If the order number is found successfully
                        if response.status_code == 200:
                            response_content = json.loads(response.content.decode())
                            self.orders.append(
                                {
                                    "transaction number": order_number_to_ask,
                                    "stock name": response_content['data']["name"],
                                    "order type": response_content['data']["type"],
                                    "quantity": response_content['data']["quantity"],
                                }
                            )
                        # Order number not found
                        else:
                            continue
                    # If connection error
                    except requests.exceptions.RequestException as e:
                        continue    
                self.transaction_number = latest_order_number
                # Write the updated memory into the local file
                # This function is called before the server starts serving, so a write lock is unnecessary
                with open(self.local_data_path, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    for order in self.orders:
                        writer.writerow([order['transaction number'], order['stock name'], order['quantity'], order['order type']])


# Define the main function to start the server
def main(args):
    if args.service_id == '3':
        request_listening_port = int(os.environ.get('ORDER_REQUEST_PORT3'))
        socket_listening_port = int(os.environ.get('ORDER_SOCKET_PORT3'))
    elif args.service_id == '2':
        request_listening_port = int(os.environ.get('ORDER_REQUEST_PORT2'))
        socket_listening_port = int(os.environ.get('ORDER_SOCKET_PORT2'))
    elif args.service_id == '1':
        request_listening_port = int(os.environ.get('ORDER_REQUEST_PORT1'))
        socket_listening_port = int(os.environ.get('ORDER_SOCKET_PORT1'))
    else:
        raise RuntimeError("Invalid argument: service id {args.service_id}")

    # Create a threaded HTTP server with the given port and output directory
    httpd = ThreadedHTTPServer(("", request_listening_port), OrderRequestHandler, args)
    # Print a message to indicate that the server is serving on the specified port
    print(f"Serving on port {request_listening_port}")

    # Run a daemon thread to listen to the leader selection result from the frontend
    t = threading.Thread(target=daemon_worker, daemon=True, args=[socket_listening_port, httpd])
    t.start()

    # Start the server and keep it running indefinitely
    httpd.serve_forever()

# Check if the script is being run as the main module
if __name__ == "__main__":
    # Load env variables
    load_dotenv()

    # Create an argument parser with options for the port and output directory
    parser = argparse.ArgumentParser(description='Order Server.')
    # Assign the ID
    parser.add_argument('--id', dest='service_id', help='Service ID', type=str)
    # Assign the directory of orders.csv
    parser.add_argument('--out_dir', dest='out_dir', help='Output directory', default=os.environ.get('OUTPUT_DIR'), type=str)
    # Assign the host and the port of the catalog service 
    parser.add_argument('--catalog_host', dest='catalog_host', help='Catalog Host', default=os.environ.get('CATALOG_HOST'), type=str)
    parser.add_argument('--catalog_port', dest='catalog_port', help='Catalog Port', default=os.environ.get('CATALOG_PORT'), type=str)
    # Restart or Resume
    parser.add_argument('--resume', dest='resume', help='Resume from the last order record', default=False, action='store_true')
    
    args = parser.parse_args()
    main(args)