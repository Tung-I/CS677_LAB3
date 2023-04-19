import csv

with open('/home/tungi/CS677_LAB3/order/data/order.csv', newline='') as f:
    csv_reader = csv.reader(f)
    for row in csv_reader:
        print(row)