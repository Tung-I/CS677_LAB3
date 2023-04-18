import requests
import argparse
import http.client
import sys
import json
import random
import time

# Test set: excessive trading, invalid order quantity, and invalid stock names
TEST_SET = [
    # Initial remaining quantity of each stock: 100
    # SELL 10 STOCKS OF AAPL, BUY 10 STOCKS OF MSFT -> AAPL: 110, MSFT: 90, GOOG: 100
    "/lookup?stock=AAPL",
    "/lookup?stock=MSFT",
    "/lookup?stock=GOOG",
    "/order?stock=AAPL&amp;quantity=10&amp;type=sell",
    "/order?stock=MSFT&amp;quantity=10&amp;type=buy",
    "/lookup?stock=AAPL",
    "/lookup?stock=MSFT",
    "/lookup?stock=GOOG",
    # BUY MORE THAN AVAILABLE QUANTITY WITHIN AN ORDER-> error 400, excessive trading
    "/order?stock=AAPL&amp;quantity=120&amp;type=buy",
    "/order?stock=MSFT&amp;quantity=100&amp;type=buy",
    # BUY MORE THAN AVAILABLE QUANTITY VIA SEQUENTIAL ORDERS -> error 400, excessive trading
    "/order?stock=GOOG&amp;quantity=10&amp;type=buy",
    "/order?stock=GOOG&amp;quantity=20&amp;type=buy",
    "/order?stock=GOOG&amp;quantity=30&amp;type=buy",
    "/order?stock=GOOG&amp;quantity=40&amp;type=buy",
    "/order?stock=GOOG&amp;quantity=10&amp;type=buy",
    # Query Existing Order
    "/order?order_number=1",
    "/order?order_number=5",
    "/order?order_number=1000000"
]

# Define the main function that takes an 'args' parameter
def main(args):
    # Extract the parameters from 'args'
    host = args.host
    port = args.port
    base_url = f'http://{host}:{port}'

    # Make several requests to the same host and reuse the underlying TCP connection
    s = requests.Session()
    
    # Run the test cases
    for _case in TEST_SET:
        url = f'{base_url}{_case}'

        if _case.startswith("/lookup") or _case.startswith("/order?order_number"):
            # Send the GET request and print the response content
            response = None
            while response is None:
                try:
                    response = s.get(url)
                    print(response.content.decode())
                except:
                    pass
        else:
            # Send the POST request and print the response content
            response = None
            while response is None:
                try:
                    response = s.post(url, data=url)
                    print(response.content.decode())
                except:
                    pass


if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser(description='Client.')
    # Add arguments for the port, host, and probability
    parser.add_argument('--port', dest='port', help='Port', default=8080, type=int)
    parser.add_argument('--host', dest='host', help='Host', default='0.0.0.0', type=str)

    # Parse the arguments and store them in a variable called 'args'
    args = parser.parse_args()

    # Call the main function with the 'args' parameter
    main(args)