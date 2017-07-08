"""
An example subscriber client that demonstrates how livepb2 might access data
published by slow data scripts.
"""
from slowdaq.pb2 import *
from time import sleep

# The address and port supplied are that of the aggregator
sub = Subscriber('localhost',3141)

i = 0
while True:

    # Send and queued data and recieve any data available.
    sub.serve()

    # Queue a string to send to a slow daq script by the unique name it declares
    # for itself. Dictionaries are converted to json before queuing.
    sub.message('daq0','Hello from Subscriber')
    sub.message('daq1','Hello from Subscriber')

    if not i % 5:
        print 'publishers detected=',len(sub.index)


    # Strings recieved from publishers end up in sub.data
    while len(sub.data) > 0:
        # what the subscriber does with these strings is entirely not slowdaq's
        # concern
        print sub.data.pop()

    i += 1
    sleep(1)
