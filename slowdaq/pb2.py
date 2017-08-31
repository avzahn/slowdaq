from . netstring import *
from . netarray import *
from . stream import *
from . snapshot import *

import os
import json
import datetime

class Publisher(Server):
    """
    Sends data to an Aggregator. Also capable of recieving messages from
    Aggregators and Subscribers.
    """
    def __init__(self,name, agg_addr, agg_port,debug=False):

        Server.__init__(self,debug)

        self.name = name
        self.agg_addr = agg_addr
        self.agg_port = agg_port
        self.pid = os.getpid()

        # for display purposes only
        self.status = 'unset'
        self.status_color = 'white'

        self.add_connection(agg_addr,agg_port)

        # listen on any open port
        self.add_listener()

        # all messages sent to this instance end up here
        self.inbox = deque([],maxlen=128)

        self.pulse()

    def pack(self,d):
        """
        Add key-value pairs to a dict d so as to be intelligible to an
        aggregator recieving it over the network, and return it as sendable JSON
        """
        d['source'] = [self.name, self.pid]
        d['event'] = 'data'
        apply_timestamp(d)
        return json.dumps(d)

    def pulse(self):
        """
        queue a heartbeat signal to send to all clients
        """
        addr,port = None,None
        for stream in self.streams:
            if stream.status == 'listening':
                # there should only ever be one listening stream in an instance
                addr,port = stream.host_location

        e = Entry()
        e.name = self.name
        e.pid = str(self.pid)

        #TODO: are the addr and port actually correct?
        e.addr = addr
        e.port = port
        e.status = self.status
        e.status_color = self.status_color
        e.systime = utcnow()

        self.queue(e.serialize())
        self.t_last_pulse = now()

    def handle_recv(self,stream,msgs):
        self.inbox += msgs

    def serve(self,timeout=0):
        # decide whether or not to create and publish a heartbeat
        t = now()
        if t - self.t_last_pulse > datetime.timedelta(seconds=15):
            self.pulse()
        Server.serve(self,timeout)

class Aggregator(Server):
    """
    Broker for a basic slowdaq system. Collects data from Publishers, and
    informs Subscribers of how to connect to any active Publishers by itself
    publishing a snapshot of the slowdaq system state.
    """
    def __init__(self,addr,port,fname,debug=False):

        Server.__init__(self,debug)

        self.add_listener(addr,port)

        self.fname = fname
        self.data = []

        # setup an initial snapshot
        self.snapshot = Snapshot()

    def dump(self):
        """
        Append everything in self.data to a file.

        TODO: Support new files as old ones become too large
        TODO: Decide when it is appropriate to a dump a snapshot
        """
        with File(self.fname,'a') as f:
            while len(self.data) > 0:
                msg = self.data.pop()
                f.write(msg)
            f.flush()

    def handle_recv(self,stream,msgs):

        do_new_snapshot = False

        for msg in msgs:
            try:
                m = json.loads(msg)
            except:
                continue

            if m['event'] == 'data':
                self.data.append(msg)

            if m['event'] == 'request_snapshot':
                # defer working on this at all until after sifting through all
                # the messages we have for new pulse events
                do_new_snapshot = True

            if m['event'] == 'pulse':
                # older pulses from the same source are deleted
                e = Entry(m)
                e.remote_addr,e.remote_port = stream.remote_location

                self.snapshot.add_entry(e)

        # snapshot logging period needs to be faster than three minutes since
        # we purge anything older than that from it
        self.snapshot.remove_oldest(datetime.timedelta(minutes=3))

        if do_new_snapshot:
            stream.queue(self.snapshot.serialize())

    def handle_accept(self,stream):
        stream.queue(self.snapshot.serialize())

class Subscriber(Server):
    """
    Handles the automatic retrieval of system snapshots from an Aggregator, and
    subscribes to any data published by the Publishers connected to that
    Aggregator.

    It is not recommended that a Subscriber be used as a data archiver.
    Subscribers are more prone to data loss than Aggregators and Archivers
    because they are one level further removed from the Publishers. When a
    Publisher's connection to a Subscriber fails, the Publisher has no
    responsibility to buffer for the Subscriber any data generated until the
    connection is reestablished. Further, the Subscriber must reestablish that
    connection itself by taking potentially seconds to get a new snapshot from
    the Aggregator.
    """

    def __init__(self, agg_addr, agg_port,debug=False):

        Server.__init__(self,debug)

        self.agg_addr = agg_addr
        self.agg_port = agg_port

        self.aggregator = self.add_connection(agg_addr,agg_port)

        self.snapshots = deque([Snapshot()],maxlen=128)
        self.new_snapshot = False

        # Will be filled with all data frames recieved. Up to the user to empty.
        self.data = deque([],maxlen=512)

        self.index = {}

        self.current_snapshot = None

        self.initial_connect = False

    def request_snapshot(self):
        d = {'event':'request_snapshot'}
        self.aggregator.queue(json.dumps(d))

    def handle_recv(self,stream,msgs):

        for msg in msgs:

            try:
                m = json.loads(msg)
            except:
                continue

            if m['event'] == 'snapshot':
                self.snapshots.append(Snapshot(m))
                self.new_snapshot = True

            if m['event'] == 'data':
                self.data.append(m)


    def build_index(self):
        """
        Associate every entry in the latest snapshot with a preexisting stream
        object, or None. Entry objects are hashable, so the index is just an
        {entry:stream} dict.
        """
        self.index = {}

        for e in self.snapshots[-1].entries:
            stream = None
            remote_location = (e.addr,e.port)

            self.index[e] = None

            for s in self.streams:
                if s.remote_location == remote_location:
                    self.index[e] = s
                    break


    def handle_close(self,stream):
        # if we lose a connection, it's safest to ask the aggregator if it's
        # still there before attempting to reconnect
        self.request_snapshot()
        Server.handle_close(self,stream)

    def serve(self,timeout=0):

        # any snapshots found so far will already be in self.snapshots

        if len(self.snapshots) == 1:
            # Connect everything and set current snapshot
            s = self.snapshots[-1]
            for e in s.entries:
                self.add_connection(e.addr,e.port)
            self.current_snapshot = s

        if len(self.snapshots) > 1:
            # diff currently connected snapshot with the latest snapshot then
            # establish connections not in current snapshot and remove
            # connections not in latest snapshot
            self.apply_diff(self.current_snapshot - self.snapshots[-1])
            # latest snapshot is now current
            self.current_snapshot = self.snapshots[-1]

        self.build_index()
        self.request_snapshot()

        Server.serve(self,timeout)

    def apply_diff(self,diff):

        for e in diff.add:
            # silently does nothing if address already exists in this instance
            self.add_connection(e.addr,e.port)

        for e in diff.remove:
            # silently does nothing if a stream with that address doesn't exist
            self.remove_connection(e.addr,e.port)


    def message(self,name,msg):
        if name in self.index:
            if isinstance(msg,dict):
                msg = json.loads(msg)
            self.index[name].queue(msg)
            return True
        else:
            return False


class Archiver(Server):
    pass
