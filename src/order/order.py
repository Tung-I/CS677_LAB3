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
import yaml


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


def lookup( file_path: str, order_number: str):
    """
    Return:
        returns the corresponding order record, -1 if the order number doesn't exist.
    """
    with open(file_path, newline='') as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            if row[0] == order_number:
                return row
        return -1
        


def leader_broadcast(port, server):
    """
    A daemon thread running in the background
    Update the leader information whenever receiving a frontend notification
    """
    # Create the socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    s.listen(5)
    print(f"Listening to leader broadcast on: {port}")
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
                server.curr_leader_id = server.id
            # Be aware of who's the leader
            else:
                print(f'Order ID {data.decode()} is the leader now.')
                server.curr_leader_id = data.decode()
            # Block for the next message
            data = conn.recv(1024)

def health_check(port):
    """
    Reply the regular health check from the frontend
    """
    # Create the socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    s.listen(5)
    print(f"Listening to regular health check on: {port}")
    # Listen to the message
    while True:
        conn, info = s.accept()
        data = conn.recv(1024)
        while data:
            if data.decode() == 'Ping':
                # Send OK mmessage back
                conn.send('OK'.encode("ascii"))
            # Block for the next message
            data = conn.recv(1024)


# Define a request handler that inherits from BaseHTTPRequestHandler
class OrderRequestHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):  
        # Query existing orders
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
            lookup_result = lookup(os.path.join(self.server.out_dir, f'order{self.server.id}.csv'), order_number)

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

            # Otherwise, return the order info
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
        
        # Send the current order number to the replica coming back from a crash
        elif self.path.startswith("/synchronize"):   
            response = str(self.server.transaction_number)
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(response.encode())

        # Invalid URL
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
        # Receive propagation from the leader
        if self.path.startswith('/propagate'):
            # If this is the first propagation after coming back from a crash
            # Ask for the order data that do not exist in the local database
            with self.server.rwlock.w_locked():
                if self.server.resume:
                    for peer_host, peer_port in self.server.peer_request_addr:
                        response = None
                        url = f'http://{peer_host}:{peer_port}/synchronize'
                        try:
                            response = requests.get(url)
                        except requests.exceptions.RequestException as e:
                            continue
                        latest_order_number = int(response.content.decode())
                        
                        # If an update is needed
                        if latest_order_number > self.server.transaction_number:
                            # Start scynchronization
                            for order_number_to_ask in range(self.server.transaction_number, latest_order_number):
                                # Try to send a request to the replica 
                                try:
                                    url = f'http://{peer_host}:{peer_port}/order?order_number={order_number_to_ask}'
                                    response = requests.get(url)
                                    # If the order number is found successfully
                                    if response.status_code == 200:
                                        response_content = json.loads(response.content.decode())     
                                        self.server.orders.append(
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
                            self.server.transaction_number = latest_order_number
                    # Set "resume" back to False
                    self.server.resume = False

                # If this is a normal propagation request
                else:
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

        # If this is a GET /orders/<order_number> request
        elif self.path.startswith('/order'):
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

                        # Increase the transaction number
                        self.server.transaction_number += 1

                        # Propagate the info to the follower nodes
                        transaction_number_to_broadcast = self.server.transaction_number - 1
                        for peer_host, peer_port in self.server.peer_request_addr:
                            try:
                                # Send an update request to the peer
                                url = f'http://{peer_host}:{peer_port}/propagate?stock={stock_name}&amp;quantity={-quantity}&amp;type={order_type}&amp;number={transaction_number_to_broadcast}'
                                peer_response = requests.post(url, data={"stock": stock_name, "quantity": -quantity, "type": order_type, "number": transaction_number_to_broadcast})
                            except requests.exceptions.RequestException as e:
                                continue


                    # If there's no enough remaining quantity
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
        
                    # Increase the transaction number
                    self.server.transaction_number += 1

                    # Propagate the info to the follower nodes
                    transaction_number_to_broadcast = self.server.transaction_number - 1
                    for peer_host, peer_port in self.server.peer_request_addr:
                        try:
                            # Send an update request to the peer
                            url = f'http://{peer_host}:{peer_port}/propagate?stock={stock_name}&amp;quantity={quantity}&amp;type={order_type}&amp;number={transaction_number_to_broadcast}'
                            peer_response = requests.post(url, data={"stock": stock_name, "quantity": quantity, "type": order_type, "number": transaction_number_to_broadcast})
                        except requests.exceptions.RequestException as e:
                            continue


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
        # Request neither starting with '/order' nor 'propagate' 
        else:
            raise RuntimeError(f'Invalid URL: {self.path}')
        

# Create a class that inherits from ThreadingMixIn and HTTPServer to implement a threaded HTTP server
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    # Override the init function to save metadata in the server
    def __init__(self, host_port_tuple, streamhandler, config):
        super().__init__(host_port_tuple, streamhandler)
        self.protocol_version = 'HTTP/1.1'
        # Order records
        self.transaction_number = 0
        self.orders = []
        # Read-write lock
        self.rwlock = RWLock()
        # Leader node information
        self.curr_leader_id = None
        self.id = config['ID']
        # Paths to the local database
        self.out_dir = config['OUTPUT_DIR']
        self.local_data_path = os.path.join(self.out_dir, f'order{self.id}.csv')
        # Catalog information
        self.catalog_host = config['CATALOG_HOST']
        self.catalog_host_url = f'http://{self.catalog_host}'
        self.catalog_port = config['CATALOG_PORT']
        self.order_host = os.environ.get('ORDER_HOST')
        # Peer request address
        if self.id == '3':
            self.peer_request_addr = [
                (config["ORDER_HOST2"], config["ORDER_PORT2"]),
                (config["ORDER_HOST1"], config["ORDER_PORT1"])
                ]
        elif self.id == '2':
            self.peer_request_addr = [
                (config["ORDER_HOST3"], config["ORDER_PORT3"]),
                (config["ORDER_HOST1"], config["ORDER_PORT1"])
                ]
        elif self.id == '1':
           self.peer_request_addr = [
                (config["ORDER_HOST3"], config["ORDER_PORT3"]),
                (config["ORDER_HOST2"], config["ORDER_PORT2"])
                ]
        else:
            raise RuntimeError(f"Invalid argument: service id {config['ID']}")

        # Whether this server is just coming back from a crash
        self.resume = config['RESUME']

    # Override the server_bind function to enable socket reuse and bind to the server address
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)


# Define the main function to start the server
def main(args):
    # Check the validity of the assigned ID
    if args.id != '3' and args.id != '2' and args.id != '1':
          raise RuntimeError("Invalid argument: service id {args.id}")
    # Creat a config object
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

    # Add additional variables to config
    config["RESUME"] = args.resume
    config["ID"] = args.id   


    # Create a threaded HTTP server with the given port and output directory
    my_id = config["ID"]
    frontend_request_listening_port = config[f"ORDER_PORT{my_id}"]
    httpd = ThreadedHTTPServer(("", frontend_request_listening_port), OrderRequestHandler, config)

    # Initialize local database
    if not config["RESUME"]:
        with open(httpd.local_data_path, mode='w', newline='') as file:
            pass
    # Resume from a crash
    else:
        with httpd.rwlock.w_locked():
            # Look at the local database to get the last recorded order number 
            httpd.orders = []
            with open(httpd.local_data_path, newline='') as f:
                csv_reader = csv.reader(f)
                for row in csv_reader:
                    # Load the orders into memory
                    if len(row) == 0:
                        continue
                    httpd.orders.append(
                        {
                            "transaction number": int(row[0]),
                            "stock name": row[1],
                            "order type": row[3],
                            "quantity": row[2]
                        }
                    )
            # Update local transaction number
            if len(httpd.orders) != 0:
                httpd.transaction_number = httpd.orders[-1]["transaction number"] + 1
            else:
                httpd.transaction_number = 0

    # Run a daemon thread that listens to the leader selection broadcast from the frontend
    leader_broadcast_listening_port = config[f"ORDER_LEADER_BROADCAST_PORT{my_id}"]
    t1 = threading.Thread(target=leader_broadcast, daemon=True, args=[leader_broadcast_listening_port, httpd])
    t1.start()

    # Run a daemon thread to listen to the regular health check from the frontend
    health_check_port = config[f"ORDER_HEALTH_CHECK_PORT{my_id}"]
    t2 = threading.Thread(target=health_check, daemon=True, args=[health_check_port])
    t2.start()

    # Start the server and keep it running indefinitely
    print(f"Serving on port {frontend_request_listening_port}")
    httpd.serve_forever()


# Check if the script is being run as the main module
if __name__ == "__main__":
    # Create an argument parser with options for the port and output directory
    parser = argparse.ArgumentParser(description='Order Server.')
    # Assign the ID
    parser.add_argument('--id', dest='id', help='Service ID', type=str)
    parser.add_argument('--resume', dest='resume', help='Resume from the last order record', default=False, action='store_true')
    # Load variables from config.yaml
    parser.add_argument('--config_path', dest='config_path', help='Path to config.yaml', default=None, type=str)
    # Parse args
    args = parser.parse_args()

    main(args)