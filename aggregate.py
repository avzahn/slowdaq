from slowdaq.pb2 import *
from time import sleep

# Note that an address of 'localhost' will block remote connections
agg = Aggregator('localhost',3141,'data.log')

while True:
    
    agg.select(timeout=5)
    
    # TODO: shouldn't need this with a nonzero timeout
    time.sleep(5)
    
    print agg.snapshot
    
    agg.dump()
