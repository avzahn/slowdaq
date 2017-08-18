"""
A simple example slow data generating script that publishes the value of a loop
counter. Essentially identical to daq1.py.
"""

from slowdaq.pb2 import *
from time import sleep

# While pub will listen on a dynamically allocated TCP port, the aggregator
# location must be known in advance. Each publisher needs to have a unique name.
# Make sure to not use 'localhost' as an IP address in the field--this will cause
# the OS to ignore nonlocal connections. Instead, type the machine's IP address.
pub = Publisher('daq0','localhost',3141)

i = 0

while True:

    # nothing is actually sent or recieved until we call this
    pub.serve()

    # format data so that the aggregator can interpret it
    data = pub.pack({'i':i})

    # Recall this is only queued for sending, and won't be sent until the
    # next serve().
    pub.queue(data)

    i += 1

    if not i % 10:

        while len(pub.inbox) > 0:
            # any messages we recieve end up in the list pub.inbox. It's the
            # user's responsibility to empty it to avoid memory issues
            print pub.inbox.pop()

        print 'daq0: ',i

    sleep(1)
