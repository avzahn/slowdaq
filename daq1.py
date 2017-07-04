from slowdaq.pb2 import *
from time import sleep

pub = Publisher('daq1','localhost',3141)


j = 0
while True:    
    
    # nothing is actually sent or recieved until we call this
    pub.serve()
    
    
    # all messages addressed to us are now in pub.inbox
    
    pub.queue(pub.pack({'j':j}))
    
    
    j += 1
    
    
    if not j % 10:
        print 'daq1:',j
        
    time.sleep(1)

