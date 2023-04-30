import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import json
import argparse

# [client1, client2, client3, client4, client5]
lookup_latency_prob00_cache = [0.002794173717498779]
order_latency_prob00_cache = [0.030508742187962387]

def list_avg(_list):
    return sum(_list) / len(_list)

def main(args):
    # Maximun number of clients
    max_n_client = 5

    if args.exp_type == 'cache_or_not':
        lookup_latency = lookup_latency_prob05_nocache
        order_latency = order_latency_prob05_nocache 

    # Plot the figure
    fig, ax = plt.subplots()
    plt.xlabel('Number of clients')
    plt.ylabel('Average response time (s) per request')
    plt.xticks(list(range(1, max_n_client+1)), list(range(1, max_n_client+1)))
    ax.plot(list(range(1, max_n_client+1)), lookup_latency, label="Lookup()")
    ax.plot(list(range(1, max_n_client+1)), order_latency, label="Trade()")
    ax.set_title('Performance Evaluation')
    ax.xaxis.set_minor_locator(AutoMinorLocator(2))
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.grid(which='minor', linewidth=0.6)
    ax.legend()
    
    # plt.show()
    plt.savefig('performance_evaluation.png')
    plt.close(fig) 


if __name__ == '__main__':
   # Create an argument parser
    parser = argparse.ArgumentParser(description='Client.')
    # Add arguments 
    parser.add_argument('--exp_type', dest='exp_type', help='Type of experiment', type=str)
    # Parse the arguments and store them in a variable called 'args'
    args = parser.parse_args()

    # Call the main function with the 'args' parameter
    main(args)
    

