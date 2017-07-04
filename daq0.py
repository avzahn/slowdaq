"""
A simple example slow data generating script that publishes the value of a loop
counter. Essentially identical to daq1.py.
"""

from slowdaq.pb2 import *
from time import sleep

# While pub will listen on a dynamically allocated TCP port, the aggregator
# location must be known in advance.
pub = Publisher('daq0','localhost',3141)

i = 0

while True:    
    
    # nothing is actually sent or recieved until we call this
    pub.serve()
    
    # all messages addressed to us are now in pub.inbox. It's the user's
    # responsibility to empty it to avoid memory problems    
    pub.queue(pub.pack({'i':i}))
    
    i += 1
    
    if not i % 10:
        print 'daq0:',i
        
    time.sleep(1)

