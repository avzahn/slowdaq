from slowdaq.pb2 import *
from time import sleep

pub = Publisher('daq0','localhost',3141)


i = 0

while True:    
    
    # nothing is actually sent or recieved until we call this
    pub.select()
    
    
    # all messages addressed to us are now in pub.inbox
    
    pub.publish_data({'i':i})
    
    
    if not i % 10:
        pub.pulse()
        
    
    i += 1
    print i
    time.sleep(1)

