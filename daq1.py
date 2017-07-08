"""
A simple example slow data generating script that publishes the value of a loop
counter. Essentially identical to daq0.py.
"""

from slowdaq.pb2 import *
from time import sleep

# While pub will listen on a dynamically allocated TCP port, the aggregator
# location must be known in advance. Each publisher needs to have a unique name.
pub = Publisher('daq1','localhost',3141)

j = 0

while True:

    # nothing is actually sent or recieved until we call this
    pub.serve()

    # format data so that the aggregator can interpret it
    data = pub.pack({'j':j})

    # Recall this is only queued for sending, and won't be sent until the
    # next serve().
    pub.queue(data)

    j += 1

    if not j % 10:

        while len(pub.inbox) > 0:
            # any messages we recieve end up in the list pub.inbox. It's the
            # user's responsibility to empty it to avoid memory issues
            print pub.inbox.pop()

        print 'daq1: ',j

    sleep(1)
