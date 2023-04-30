import requests
import argparse
import http.client
import sys
import json
import random
import time
import itertools


# 1) Sell 1 stocks of AAPL, 2) Buy 1 stocks of MSFT, 3) Sell 1 stocks of MSFT, 4) Buy 1 stocks of MSFT
# Do this 100 times
# All the requests should be processed successfully
operation = [
    "/order?stock=AAPL&amp;quantity=1&amp;type=sell",
    "/order?stock=AAPL&amp;quantity=1&amp;type=buy",
    "/order?stock=MSFT&amp;quantity=1&amp;type=sell",
    "/order?stock=MSFT&amp;quantity=1&amp;type=buy"
]
ORDER_TEST_SET1 = [operation for i in range(100)]
ORDER_TEST_SET1 = list(itertools.chain.from_iterable(ORDER_TEST_SET1))

# Lookup existing orders
LOOKUP_TEST_SET= [
    "/order?order_number=1",
    "/order?order_number=2",
    "/order?order_number=3",
    "/order?order_number=4",
    "/order?order_number=5",
    # Order number not found
    "/order?order_number=9999999",
    "/order?order_number=-1"
]

# Buy more than available quantity 
# Buy 1 quantity each stock 
# Do it 101 times
# 10 excessive trading warnings are expected
operation = [
    "/order?stock=AAPL&amp;quantity=1&amp;type=buy",
    "/order?stock=GOOG&amp;quantity=1&amp;type=buy",
    "/order?stock=MSFT&amp;quantity=1&amp;type=buy",
    "/order?stock=SPX&amp;quantity=1&amp;type=buy",
    "/order?stock=OEX&amp;quantity=1&amp;type=buy",
    "/order?stock=DJX&amp;quantity=1&amp;type=buy",
    "/order?stock=NDX&amp;quantity=1&amp;type=buy",
    "/order?stock=CPQ&amp;quantity=1&amp;type=buy",
    "/order?stock=INTC&amp;quantity=1&amp;type=buy",
    "/order?stock=IBM&amp;quantity=1&amp;type=buy",
]
ORDER_TEST_SET2 = [operation for i in range(101)]
ORDER_TEST_SET2 = list(itertools.chain.from_iterable(ORDER_TEST_SET2))


# Invalid URL
# Place an order with negative quantity -> error 400, invalid quantity
# Place an order with 0 quantity -> error 400, invalid quantity
# Place an order with non-existent stock name -> error 404, stock not found
ORDER_TEST_SET3 = [
    "/order?stock=AAPL&amp;quantity=0&amp;type=buy",
    "/order?stock=AAPL&amp;quantity=0&amp;type=sell",
    "/order?stock=AAPLLLLLLLL&amp;quantity=100&amp;type=sell"
]


# Define the main function that takes an 'args' parameter
def main(args):
    # Make several requests to the same host and reuse the underlying TCP connection
    s = requests.Session()
    
    # Run the test cases
    # for test_set in [ORDER_TEST_SET1, LOOKUP_TEST_SET, ORDER_TEST_SET2, ORDER_TEST_SET3]:
    for test_set in [ORDER_TEST_SET1, LOOKUP_TEST_SET]:
        for request in test_set:
            url = f'http://{args.host}:{args.port}{request}'
            if request.startswith("/order?order_number"):
                response = requests.get(url)
            else:
                request_body = url.split('?')[-1].split("&amp;")
                stock_name = request_body[0].split("=")[-1]
                quantity = request_body[1].split("=")[-1]
                order_type = request_body[2].split("=")[-1]
                response = s.post(url, data={"stock": stock_name, "quantity": quantity, "type": order_type})
  
            print(response.content.decode())

        print('------------------------------------------------')
        time.sleep(3)  


if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser(description='Client.')
    # Add arguments for the port, host, and probability
    parser.add_argument('--port', dest='port', help='Port', default=16008, type=int)
    parser.add_argument('--host', dest='host', help='Host', default='127.0.0.1', type=str)

    # Parse the arguments and store them in a variable called 'args'
    args = parser.parse_args()

    # Call the main function with the 'args' parameter
    main(args)