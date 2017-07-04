from . netstring import *
from . netarray import *
from . stream import *

import os
import json
import datetime

class Publisher(Server):
    """
    """
    def __init__(self,name, agg_addr, agg_port):
        
        Server.__init__(self)
        
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
        d = {'event':'pulse',
             'name':self.name,
             'pid':self.pid}
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
    """
    def __init__(self,addr,port,fname):
        
        Server.__init__(self)
        
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
                m['addr'], m['port'] = stream.remote_location
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
    
    It is not recommended that a Subscriber be used as a data archiver.
    Subscribers are more prone to data loss than Aggregators and Archivers
    because they are one level further removed from the Publishers. When a
    Publisher's connection to a Subscriber fails, the Publisher has no
    responsibility to buffer for the Subscriber any data generated until the
    connection is reestablished. Further, the Subscriber must reestablish that
    connection itself by taking potentially tens of seconds to get a new
    snapshot from the Aggregator.
    """
    
    def __init__(self, agg_addr, agg_port):
        
        self.agg_addr = agg_addr
        self.agg_port = agg_port
        
        self.aggregator = self.add_connection(agg_addr,agg_port)
        
        self.snapshots = []
        
        # Will be filled with all data frames recieved. Up to the user to empty.
        self.data = []
        
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
            
            if m['event'] == 'data':
                self.data.append(m)
                
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
            addr0.append( (entry['port'],entry['addr']) )
        
        addr1 = []
        
        for name in s1['entries']:
            entry = s1['entries'][name]
            addr1.append( (entry['port'],entry['addr']) )
            
        addr0,addr1 = set(addr0),set(addr1)
        
        add = list(addr1-addr0)
        remove = list(addr0-addr1)
        
        return add,remove
    
    def handle_close(self,stream):
        # if we lose a connection, it's safest to ask the aggregator if it's
        # still there before attempting to reconnect
        self.request_snapshot()
        Server.handle_close(self,stream)
        
        
    def apply_diff(self,add,remove):
        pass      
    
    
    
class Archiver(Server):
    pass
