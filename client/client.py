import requests
import argparse
import http.client
import sys
import json
import random
import time


# Stock choices
STOCK_NAME_LIST = ["AAPL", "GOOG", "MSFT", "MSFTT"]

# Define the main function that takes an 'args' parameter
def main(args):
    # Extract the 'host', 'port', and 'prob (probablity)' parameters from 'args'
    host = args.host
    port = args.port
    prob = args.prob

    # Fix the random seed for debugging
    random.seed(1)

    # Make several requests to the same host and reuse the underlying HTTP connection
    s = requests.Session()
    
    for i in range(10000):
        time.sleep(0.2)
        # Randomly choose a stock from the list
        chosen_stock = random.choice(STOCK_NAME_LIST)
        print(chosen_stock)
        # Define the url of http requests 
        base_url = f'http://{host}:{port}'
        url = f'{base_url}/lookup?stock={chosen_stock}'     
        # Send a GET request to the URL and print the response content
        response = s.get(url)
        print(response.content.decode())
      
        # Send the next request if Lookup() fails
        if response.status_code != 200:
            continue

        # If the returned quantity is greater than zero
        if float(response.json()["data"]["quantity"]) > 0:
            # With probability args.prob, place a "buy" request 
            if prob >= random.uniform(0., 1.):
                print("Additional trading request:")
                order_type = 'buy'
                quantity = 200
                url = f'{base_url}/order?stock={chosen_stock}&amp;quantity={quantity}&amp;type={order_type}'
                # Send a POST request to the URL with the order data, and print the response content
                response = s.post(url, data={"name": chosen_stock, "quantity": quantity, "order_type": order_type})
                print(response.content.decode())

        # With probability 0.1, place a "sell" request
        if 0.1 >= random.uniform(0., 1.):
            order_type = 'sell'
            quantity = 100
            url = f'{base_url}/order?stock={chosen_stock}&amp;quantity={quantity}&amp;type={order_type}'
            # Send a POST request to the URL with the order data, and print the response content
            response = s.post(url, data={"name": chosen_stock, "quantity": quantity, "order_type": order_type})
            print(response.content.decode())


if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser(description='Client.')
    # Add arguments for the port, host, and probability
    parser.add_argument('--port', dest='port', help='Port', default=8080, type=int)
    parser.add_argument('--host', dest='host', help='Host', default='127.0.0.1', type=str)
    parser.add_argument('--prob', dest='prob', help='probability of sending the order request', default=0.5, type=float)

    # Parse the arguments and store them in a variable called 'args'
    args = parser.parse_args()

    # Call the main function with the 'args' parameter
    main(args)