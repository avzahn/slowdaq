from slowdaq.pb2 import *
from time import sleep

pub = Publisher('daq1','localhost',3141)


j = 0

while True:    
    
    # nothing is actually sent or recieved until we call this
    pub.select()
    
    
    # all messages addressed to us are now in pub.inbox
    
    pub.publish_data({'j':j})
    
    
    if not j % 10:
        pub.pulse()
        
    
    j += 1
    print j
    time.sleep(1)

