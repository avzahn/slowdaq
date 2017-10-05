from slowdaq.pb2 import *
from time import sleep

# Note that an address of 'localhost' will block remote connections
agg = Aggregator('localhost',3141,logdir='agg')

i = 0
while True:

    agg.serve(timeout=5)
    sleep(1)

    # displays all slow data scripts or Publishers currently connected
    if not i % 5:
        print agg.snapshot.tty_out()
    	agg.log()



    i += 1
