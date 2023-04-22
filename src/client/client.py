import requests
import argparse
import http.client
import sys
import json
import random
import time


STOCK_NAMES = ["AAPL", "GOOG", "MSFT", "SPX", "OEX", "DJX", "NDX", "CPQ", "INTC", "IBM"]

# Define the main function that takes an 'args' parameter
def main(args):
    # Extract the parameters from 'args'
    host = args.host
    port = args.port
    order_prob = args.prob
    base_url = f'http://{host}:{port}'
    n_request = args.n_request
    seed = args.seed

    # List for saving the information of each successful order
    order_records = []

    # Make several requests to the same host and reuse the underlying TCP connection
    s = requests.Session()

    # Request
    for n in range(n_request):
        # Set a random seed for reproducibility
        random.seed(seed + n)
        # Choose a random stock
        stock_name = random.choice(STOCK_NAMES)
        # Send a GET request to the URL and print the response content
        url = f'{base_url}/lookup?stock={stock_name}'     
        response = s.get(url)
        print(response.content.decode())

        # With probability P, send an additional order request
        if order_prob >= random.uniform(0., 1.):
            order_type = random.choice(['buy', 'sell'])
            if order_type == 'buy':
                quantity = 20
            else:
                quantity = 10
            # Send a POST request to the URL with the order data, and print the response content
            url = f'{base_url}/order?stock={stock_name}&amp;quantity={quantity}&amp;type={order_type}'
            response = s.post(url, data={"name": stock_name, "quantity": quantity, "order_type": order_type})
            print(response.content.decode())

            # Record the order information if a trade request was successful
            if response.status_code == 200:
                number = json.loads(response.content.decode())["data"]["transaction_number"]
                order_record = {
                    "data": {
                        "number": str(number),
                        "name": stock_name,
                        "type": order_type,
                        "quantity": float(quantity),
                    }
                }
                order_records.append(order_record)
    
    # Before exiting, retrieve the order info and check if the server reply matches the locally stored data
    for order_record in order_records:
        order_record = order_record["data"]
        order_number  = order_record["number"]
        url = f"{base_url}/order?order_number={order_number}"
        response = s.get(url)
        # If the order number does not exist in the erver
        if response.status_code != 200:
            print(f"Order record does not exist: {order_record}")
        # Read the reply and see if the reply matches with the locally stored data
        order_info_from_server = json.loads(response.content.decode())["data"]
        if order_info_from_server["name"] == order_record["name"] and order_info_from_server["type"] == order_record["type"] \
            and float(order_info_from_server["quantity"]) == float(order_record["quantity"]):
            pass
        else:
            print(f"Order records inconsistent. Local:{order_record}, Remote:{order_info_from_server}")


if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser(description='Client.')
    # Add arguments 
    parser.add_argument('--port', dest='port', help='Port of front-end', default=8080, type=int)
    parser.add_argument('--host', dest='host', help='Host of front-end', default='0.0.0.0', type=str)
    parser.add_argument('--prob', dest='prob', help='Probability', default=0.5, type=float)
    parser.add_argument('--seed', dest='seed', help='Random seed', default=0, type=int)
    parser.add_argument('--n_request', dest='n_request', help='Number of the sequential requests', default=20, type=int)

    # Parse the arguments and store them in a variable called 'args'
    args = parser.parse_args()

    # Call the main function with the 'args' parameter
    main(args)