import json
import http.server
import argparse
import socket
import os
import yaml
import requests
from http.server import HTTPServer
from socketserver import ThreadingMixIn
from contextlib import contextmanager
from threading  import Lock


# Define the filename for the catalog JSON file
CATALOG_FILENAME = "catalog.json"


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



# Define a function to initialize a catalog file with some initial data
def init_catalog(
    file_path: str, 
    rwlock: RWLock,
    init_data: dict = None
    ):
    # Open the file in write mode and write the initial data to it as JSON
    with rwlock.w_locked():
        with open(file_path, 'w', encoding='utf-8') as f:
            if not init_data:
                init_data = {
                    "AAPL": {"name": "Apple Inc.", "price": 15.99, "remaining_quantity": 100, "accumulated_volume": 0},
                    "GOOG": {"name": "Alphabet Inc.", "price": 16.99, "remaining_quantity": 100, "accumulated_volume": 0},
                    "MSFT": {"name": "Microsoft Corporation", "price": 17.99, "remaining_quantity": 100, "accumulated_volume": 0}, 
                    "SPX": {"name": "S&P 500 Index", "price": 10.99, "remaining_quantity": 100, "accumulated_volume": 0}, 
                    "OEX": {"name": "S&P 100 Index", "price": 11.99, "remaining_quantity": 100, "accumulated_volume": 0}, 
                    "DJX": {"name": "Dow Jones Industrial Average", "price": 12.99, "remaining_quantity": 100, "accumulated_volume": 0}, 
                    "NDX": {"name": "NASDAQ 100 Stick Index", "price": 13.99, "remaining_quantity": 100, "accumulated_volume": 0}, 
                    "CPQ": {"name": "Compaq Computer Corp", "price": 14.99, "remaining_quantity": 100, "accumulated_volume": 0}, 
                    "INTC": {"name": "Intel Corp", "price": 9.99, "remaining_quantity": 100, "accumulated_volume": 0}, 
                    "IBM": {"name": "International Business Machines Corp", "price": 21.99, "remaining_quantity": 100, "accumulated_volume": 0} 
                }
            json.dump(init_data, f, ensure_ascii=False, indent=4)

# Define a function to perform Lookp for the catalog
def lookup(
    file_path: str,
    stock_name: str,
    rwlock: RWLock
    ):
    """
    Return:
        returns the price if the stock name exists in the file, or -1 if it doesn't.
    """
    # Open the file in read mode and return the price if the stockname is valid
    with rwlock.r_locked():
        with open(file_path, 'r') as f:
            data = json.load(f)
            if stock_name not in data.keys():
                return -1
            else:
                return data[stock_name]
        
# Define a function to perform Trade, returning an integer indicating the result of the trade
def trade(
    file_path: str,
    stock_name: str,
    trade_amount: float,
    rwlock: RWLock
    ):
    """
    Return:
        1: The trade succeeded.
        -1: The stock name was not found in the file.
        -2: The trading volume is invalid.
        -3: The trade amount exceeds the available stock quantity.
    """
    # Checks if the trading volume is equal to zero
    if trade_amount == 0:
        return -2
    
    # Reads the data
    with rwlock.r_locked():
        with open(file_path, 'r') as f:
            data = json.load(f)

    # If the stock name doesn't exist, it returns -1.
    if stock_name not in data.keys():
        return -1

    # Check if excessive trading will occur
    expected_remaining_quantity = data[stock_name]['remaining_quantity'] + trade_amount
    if expected_remaining_quantity < 0:
        return -3

    # Increases the quantity of the stock by the trade amount
    else:
        with rwlock.w_locked():
            with open(file_path, 'w', encoding='utf-8') as f:
                # Writes the updated data to the file, and returns 1
                data[stock_name]['remaining_quantity'] += trade_amount
                data[stock_name]['accumulated_volume'] += abs(trade_amount)
                json.dump(data, f, ensure_ascii=False, indent=4)
        return 1



# Define a class to handle HTTP requests for the stock catalog
class CatalogRequestHandler(http.server.BaseHTTPRequestHandler):
    # Handle GET requests from both the frontend and the order service
    def do_GET(self):   
        # If the request is for stock lookup
        if self.path.startswith("/lookup"):   
            # Check the validity of URL
            if self.path.split('?')[0] != '/lookup' or self.path.split('?')[-1].split('=')[0] != 'stock':
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

            # Parse the stock name from the request URL
            _, _, stock_name = self.path.partition("=")

            # Look up the stock in the catalog
            result = lookup(os.path.join(self.server.out_dir, "catalog.json"), stock_name, self.server.rwlock)

            # If the stock is not found, return an error response
            if result == -1:
                self.send_response(404)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                response = {
                    "error": {
                        "code": 404,
                        "message": "stock not found",
                    }
                }

            # If the stock is found, return its data as a JSON response
            else:
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                response = {
                    "data": {
                        "name": result["name"],
                        "price": result["price"],
                        "quantity": result["remaining_quantity"]
                    }
                }

            self.wfile.write(json.dumps(response).encode())
        
        # If the request is not for stock lookup, delegate to the base class
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
    
    # Handle POST requests from the order service
    def do_POST(self):
        # Parse the request body to get the stock name, quantity, and trade type
        content_length = int(self.headers["Content-Length"])
        request_body = self.rfile.read(content_length).decode().split('&')  # ex. stock=GOOG&quantity=100&type=buy
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

        # Parse the request body to get the stock name, quantity, and trade type
        stock_name = request_body[0].split('=')[-1]
        quantity = request_body[1].split('=')[-1]

        # Trade the specified stock according to the request
        result = trade(os.path.join(self.server.out_dir, "catalog.json"), stock_name, float(quantity), self.server.rwlock)

        # Send the appropriate response based on the result of the trade operation
        if result == 1:
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write("Order successful".encode())
        elif result == -1:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write("Stock not found".encode())
        else:
            self.send_response(400)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write("Invalid trading volumn or excessive trading occurs".encode())

        # Send an invalidation request to the frontend
        url = f'http://{self.server.frontend_host}:{self.server.frontend_port}/invalidation?stock={stock_name}'
        frontend_response = requests.post(url)
        if frontend_response.status_code != 200:
            raise RuntimeError('Problematic response to the invalidation request')


# Define a subclass of HTTPServer that uses threading to handle multiple requests concurrently
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    # Override the init function to save metadata in the server
    def __init__(self, host_port_tuple, streamhandler, config):
        super().__init__(host_port_tuple, streamhandler)
        self.out_dir = config["OUTPUT_DIR"]
        self.rwlock = RWLock()
        self.protocol_version = 'HTTP/1.1'
        self.frontend_host = config['FRONTEND_HOST']
        self.frontend_port = config['FRONTEND_PORT']
        self.stock_name_list = ["AAPL", "GOOG", "MSFT", "SPX", "OEX", "DJX", "NDX", "CPQ", "INTC", "IBM"]

    # Override the server_bind method to set a socket option for reusing the address
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

# Define the main function that runs the server
def main(args):
    # Creat a config object
    if args.config_path:
        with open(args.config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
    # Use env variables
    else:
        config = {}
        config["CATALOG_HOST"] = os.environ.get("CATALOG_HOST")
        config["CATALOG_PORT"] = int(os.environ.get("CATALOG_PORT"))

        config["FRONTEND_HOST"] = os.environ.get("FRONTEND_HOST")
        config["FRONTEND_PORT"] = int(os.environ.get("FRONTEND_PORT"))

        config["OUTPUT_DIR"] = os.environ.get("OUTPUT_DIR")

    
    # Create a threaded HTTP server that listens on the specified port and handles requests with the CatalogRequestHandler class
    httpd = ThreadedHTTPServer(("", config['CATALOG_PORT']), CatalogRequestHandler, config)

    # Initiate the stocks and prices in the catalog
    init_catalog(os.path.join(config["OUTPUT_DIR"], "catalog.json"), httpd.rwlock)

    # Start serving requests on the specified port
    print(f"Serving on port {config['CATALOG_PORT']}")
    httpd.serve_forever()


if __name__ == "__main__":
    # Create an argument parser to read command-line arguments
    parser = argparse.ArgumentParser(description='Server.')
    # Load variables from config.yaml
    parser.add_argument('--config_path', dest='config_path', help='Path to config.yaml', default=None, type=str)
    # Parse the command-line arguments
    args = parser.parse_args()

     # Call the main function with the parsed arguments
    main(args)