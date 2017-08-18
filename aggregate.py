from slowdaq.pb2 import *
from time import sleep

# Note that an address of 'localhost' will block remote connections
agg = Aggregator('localhost',3141,'data.log')

i = 0
while True:

    agg.serve(timeout=5)
    sleep(1)

    # displays all slow data scripts or Publishers currently connected
    if not i % 5:
        print str(agg.snapshot)

    # write all data to recieved to a file, and then delete it from memory
    agg.dump()

    i += 1
