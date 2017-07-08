from . netstring import *
from . netarray import *
from . stream import *

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

        self.add_connection(agg_addr,agg_port)

        # listen on any open port
        self.add_listener()

        # all messages sent to this instance end up here
        self.inbox = []

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

        d = {'event':'pulse',
             'name':self.name,
             'pid':self.pid,
             'addr':addr,
             'port':port}

        apply_timestamp(d)
        self.queue(json.dumps(d))
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
        self.pulses = []

        # setup an initial snapshot
        self.snapshot = {'event':'snapshot','log':fname,'entries':{}}
        apply_timestamp(self.snapshot)

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
                do_new_snapshot = True

            if m['event'] == 'pulse':
                self.pulses.append(m)

        if do_new_snapshot:
            snapshot = self.update_snapshot()
            stream.queue(snapshot)

    def handle_accept(self,stream):
        stream.queue(json.dumps(self.snapshot))

    def update_snapshot(self):
        """
        Modify self.snapshot to reflect any new heartbeat signals found. Also
        remove anything older than three minutes from self.snapshot
        """

        entries = self.snapshot['entries']

        while len(self.pulses) > 0:

            p = self.pulses.pop()

            entry = {'systime':p['systime'],
                     'pid':p['pid'],
                     'addr':p['addr'],
                     'port':p['port']}

            name = p['name']

            entries[name] = entry

        # purge anything older than three minutes from the snapshot

        t_cutoff = datetime.datetime.utcnow() - datetime.timedelta(minutes=3)
        remove = []

        for name in entries:
            t_snap = from_timestamp(entries[name]['systime'])
            if t_snap < t_cutoff:
                remove.append(name)

        for name in remove:
            del entries[name]

        self.snapshot['event'] = 'snapshot'
        self.snapshot['log'] = self.fname
        apply_timestamp(self.snapshot)

        return json.dumps(self.snapshot)

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

        self.snapshots = deque([],maxlen=128)
        self.new_snapshot = False

        # Will be filled with all data frames recieved. Up to the user to empty.
        self.data = []

        self.index = {}

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
                self.snapshots.append(m)
                self.new_snapshot = True

            if m['event'] == 'data':
                self.data.append(m)

    def build_index(self):
        self.index = {}
        entries = self.snapshots[-1]['entries']

        for name in entries:
            entry = entries[name]
            stream = None
            remote_location = (entry['addr'],entry['port'])

            for s in self.streams:
                if s.remote_location == remote_location:
                    self.index[name] = s

    def snapshotdiff(self,s0,s1):
        """
        Return lists of addresses to add to and remove from snapshot s0 to form
        snapshot s1.

        TODO: consider consolidating all the snapshot related work in this
        module into a snapshot class
        """
        addr0 = []

        for name in s0['entries']:
            entry = s0['entries'][name]
            addr0.append( (entry['addr'],entry['port']) )

        addr1 = []

        for name in s1['entries']:
            entry = s1['entries'][name]
            addr1.append( (entry['addr'],entry['port']) )

        addr0,addr1 = set(addr0),set(addr1)

        add = list(addr1-addr0)
        remove = list(addr0-addr1)

        return add,remove

    def handle_close(self,stream):
        # if we lose a connection, it's safest to ask the aggregator if it's
        # still there before attempting to reconnect
        self.request_snapshot()
        Server.handle_close(self,stream)


    def serve(self,timeout=0):

        # any snapshots found so far will already be in self.snapshots

        if self.new_snapshot and not self.initial_connect:
            entries = self.snapshots[-1]['entries']
            for name in entries:
                entry = entries[name]
                self.add_connection(entry['addr'],entry['port'])
            self.initial_connect = True
            self.build_index()

        if self.new_snapshot and self.initial_connect:

            if len(self.snapshots) == 1:
                add = []
                remove = []
                entries = self.snapshots[-1]['entries']
                for name in entries:
                    entry = entries[name]
                    add.append((entry['addr'],entry['port']))

            else:
                s0 = self.snapshots[-2]
                s1 = self.snapshots[-1]
                add,remove = self.snapshotdiff(s0,s1)

            self.apply_diff(add,remove)
            self.new_snapshot = False
            self.build_index()

        self.request_snapshot()

        Server.serve(self,timeout)

    def apply_diff(self,add,remove):

        for address in add:
            # silently does nothing if address already exists in this instance
            self.add_connection(*address)

        for address in remove:
            # silently does nothing if a stream with that address doesn't exist
            self.remove_connection(*address)


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
