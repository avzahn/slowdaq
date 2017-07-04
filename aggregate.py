from slowdaq.pb2 import *
from time import sleep

# Note that an address of 'localhost' will block remote connections
agg = Aggregator('localhost',3141,'data.log')

while True:
    
    agg.serve(timeout=5)
    time.sleep(5)
    
    agg.update_snapshot()
    
    # displays all slow data scripts or Publishers currently connected
    print agg.snapshot
    
    # write all data to recieved to a file, and then delete it from memory
    agg.dump()
