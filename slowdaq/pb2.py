from . netstring import *
from . netarray import *
from . server import *

import os
import json
import datetime

class Publisher(Server):
    
    def __init__(self, name, agg_addr, agg_port):
        
        Server.__init__(self)       
        
        # need to store messages to send to the aggregator whether or not it's
        # connected every time we see a call to publish()
        self.agg_addr = agg_addr
        self.agg_port = agg_port

        try:
            # Important to make sure that all disconnects set this to None
            self.agg_sock = self.connectTCP(agg_addr, agg_port)
            # queue for messages to aggregator that can't be put through a
            # socket yet. Only used if there is no Socket for the aggregator
            # connection.
            self.aq = None
        except:
            # If None, self.select will attempt to create it once per call.
            self.agg_sock = None
            # Important to make sure that all aggregator disconnects make sure
            # this is an appropriate non None
            self.aq = deque()
        
        self.sockname = self.listenTCP(
            socket.gethostbyname(socket.gethostname()),0)        
        self.pid = os.getpid()
        
        # messages recieved from the aggregator end up here        
        self.inbox = deque([],maxlen=256)
        
        self.name = name
        
        # time of last heartbeat signal sent to aggregator. A new signal is sent
        # on self.select() if the last one was sent more than fifteen seconds
        # earlier
        self.t_last_pulse = now()
        
        
    def publish_data(self,d):
        
        d['event'] = 'data'
        
        # TODO: consider a more concise protocol for identifying the source on
        # the aggregator side
        d['source'] = [self.name,self.pid]
        
        apply_timestamp(d)
        
        self.publish(json.dumps(d))
        
    
    def publish(self,msg):
        """
        Store msg for output on all Socket objects in self.listening. If there
        is no Socket object avaiable for the aggregator, queue message in
        self.aq.
        """
        
        if self.aq != None:
            # None if and only if aggregator isn't connected yet
            self.aq.appendleft(msg)
        
        for sock in self.all_sockets(listening=False):
            sock.push(msg)

    def on_writable(self,sock):
        if sock in self.all_sockets(listening=False):
            sock.transmit()
                    
    def on_fetch_decode(self,sock,msgs):
        for msg in msgs:
            self.inbox.appendleft(msg)

    def on_disconnect(sock,msgs):
        
        if sock is self.agg_sock:            
            self.aq = self.agg_sock.queue
            self.agg_sock = None
            
    def pulse(self):
        """
        publish() a heartbeat signal
        """
        
        d = {'event':'pulse',
             'name':self.name,
             'pid':self.pid}
             
        apply_timestamp(d)
             
        self.publish(json.dumps(d))

    def select(self,timeout=0.0):
        
        # decide whether or not to create and publish a heartbeat
        t = now()
        if t - self.t_last_pulse > datetime.timedelta(seconds=15):
            self.pulse()
            self.last_pulse = t
        
        # try to reconnect to the aggregator if necessary
        if self.agg_sock == None:
            try:
                self.agg_sock = self.connectTCP(agg_addr, agg_port)
                self.agg_sock.queue = self.aq
                self.aq = None
                
            except:
                pass
                
        Server.select(self,timeout)
        
class Aggregator(Server):
        
    def __init__(self,addr,port,fname):
        
        Server.__init__(self)
        
        self.listenTCP(addr,port)
        # TODO: create a new file automatically as files get too large
        self.fname = fname
        self.data = []
        self.pulses = []
        self.snapshot = {'event':'snapshot','log':fname,'entries':{}}
        apply_timestamp(self.snapshot)
        
    def on_accept(self,sock):
        sock.push(json.dumps(self.snapshot))
        
    def on_fetch_decode(self,sock,msgs):
        
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
                m['addr'] = sock.getpeername()[0]
                m['port'] = sock.getpeername()[1]
                self.pulses.append(m)
                
        if do_new_snapshot:
            snapshot = self.update_snapshot()
            sock.push(snapshot)
        
    def dump(self):
        
        with File(self.fname,'a') as f:
            while len(self.data) > 0:
                msg = self.data.pop()
                f.write(msg)
                   
            # TODO: decide when it is appropriate to dump a snapshot
            #f.write(json.dumps(self.snapshot))
            f.flush()
            
        
            
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
        
        for name in entries:            
            t_snap = from_timestamp(entries[name]['systime'])
            if t_snap < t_cutoff:
                remove.append(name)
                
        for name in remove:
            del entries[name]
            
        snapshot['event'] = 'snapshot'
        snapshot['log'] = self.fname
        apply_timestamp(snapshot)
        
        return json.dumps(snapshot)   
            
    def publish(self,msg):        
        for sock in self.all_sockets(listening=False):
            sock.push(msg)

    def on_writable(self,sock):
        if sock in self.all_sockets(listening=False):
            sock.transmit()
    
class Subscriber(Server):
    
    def __init__(self,agg_addr,agg_port):
        
        Server.__init__(self)
        
        self.inbox = deque([],maxlen=512)
        
        self.agg_addr = agg_addr
        self.agg_port = agg_port
        self.agg_sock = None
        
        self.snapshot = None
        
        self.connect_to_aggregator()
        
    def request_snapshot(self):
        d = {'event':'request_snapshot'}
        if self.agg_sock != None:
            self.agg_sock.push(json.dumps(d))    
    
    def connect_to_aggregator(self):
        try:
            self.agg_sock=self.connectTCP(agg_addr,agg_port)
            self.request_snapshot()
        except:
            pass
    
    def subscribe(self):
        
        if self.snapshot == None:
            return
        
        entries = self.snapshot['entries']
        
        for name in entries:
            addr = entries[name]['addr']
            port = entries[name]['port']
            try:
                self.connectTCP(addr,port)
            except:
                # TODO: decide on appropriate response if cannot connect
                pass
    
    def on_disconnect(self,sock):
        
        if sock == self.agg_sock:
            self.connect_to_aggregator()
        
    def on_writable(self,sock):
        if sock in self.all_sockets(listening=False):
            sock.transmit()

    def on_fetch_decode(self,sock,msgs):
        for msg in msgs:
            
            m = json.loads(msg)
            if 'event' in m:
                if m['event'] == 'snapshot':
                    # TODO: possible issues if multiple snapshots waiting
                    self.snapshot = m
                    continue
            
            self.inbox.appendleft(m)

class Archiver(Publisher):
    
    def __init__(self,agg_addr,agg_port,fname):
        
        Publisher.__init__(self,agg_addr,agg_port)
        self.fname = fname
        self.snapshot = None
        
    def rsync(self):
        pass
        
    def dump(self):
        
        with File(self.fname,'a') as f:
            while len(self.inbox) > 0:
                msg = self.inbox.pop()
                f.write(msg)
            f.flush()
            
    def on_connect(self, sock):
        
        d= {'event':'request_snapshot'}
        apply_timestamp(d)
        msg = json.dumps(d)
        sock.push(msg)
        
        
