"""
Module for handling and displaying information on the status of connected
processes.

If functioning properly, a process's Publisher will periodically emit a JSON
heartbeat signal. This will inform anyone listening of that Publisher's name,
PID, network location, and a short status message. These signals are here
represented by Entry objects. Snapshot objects represent the state of all
visible Publishers, and are essentially collections of Entry objects.

The object hierarchy implemented here provides convenient serialization and
deserialization, with printable summary information designed to be easy to read.
"""

import json
import datetime
from datetime import timedelta
import netarray

# TODO: Consider using clint for tty output formatting
from  tty import *

utcnow = datetime.datetime.utcnow

class Pulse(object):
    """
    Publisher-side representation of the publisher state
    """

    def __init__(self,pub):

        if isinstance(pub,dict) or isinstance(pub,str):
            self.deserialize(pub)
            return


        addr,port = None,None
        for s in pub.streams:
            if s.status == 'listening':
                addr,port = s.host_location

        # Address and port that publisher is listening() on. Note that the
        # value of the address will probably not be useful, since the public
        # IP of a listening socket is not easy to get from within the process
        # that owns it.
        self.addr = addr
        self.port = port

        self.name = pub.name
        self.pid = str(pub.pid)

        self.status = pub.status
        self.status_color = pub.status_color

        self.systime = datetime.datetime.utcnow()


    def dict(self):
        d = {'systime':netarray.timestamp(self.systime),'status':self.status,
            'pid':self.pid, 'addr':self.addr,'port':self.port,'name':self.name,
            'status_color':self.status_color,'event':'pulse'}

        return d

    def serialize(self):
        return json.dumps(self.dict())

    def deserialize(self,d):

        if isinstance(d,str):
            d = json.loads(d)

        self.pid = d['pid']
        self.addr = d['addr']
        self.port = d['port']
        self.name = d['name']
        self.status = d['status']
        self.status_color = d['status_color']
        self.systime = netarray.from_timestamp(d['systime'])

class Entry(object):
    """
    Aggregator or Subscriber side representation of what is known about a
    Publisher.

    The most important method here is self.update(), which updates all the fields
    from the information in a Pulse object, and the network location of the
    Publisher if known.
    """

    def __init__(self,src=None,addr=None):

        self.pid = None
        self.name = None

        # Network location where the publisher is listening, as seen from the
        # process running this instance. The publisher will probably claim an
        # address of 127.0.1.1 or similar, because the public IP is semi opaque
        # to it.
        self.addr = addr
        self.port = None

        self.first_seen = utcnow()
        self.last_seen = utcnow()

        self.status = None
        self.status_color = None

        # optionally, a place to store a reference to the stream object backing
        # this entry. Not serialized.
        self.stream = None

        if isinstance(src, Pulse):
            self.update(src,addr=addr)

        if isinstance(src, dict):
            self.from_dict(d)

        if isinstance(src, str) or isinstance (src, unicode):
            self.deserialize(src)


    def from_dict(self,d):

        self.pid = int(d['pid'])
        self.name = d['name']
        self.first_seen = netarray.from_timestamp(d['first_seen'])
        self.last_seen = netarray.from_timestamp(d['last_seen'])
        self.status = d['status']
        self.status_color = d['status_color']

        # If we're a Subscriber deserializing this from an Aggregator, we
        # have to trust the Aggregator for addr and port
        if 'addr' in d:
            self.addr = d['addr']
            self.port = d['port']

    def deserialize(self,s):

        d = json.loads(s)
        self.from_dict(d)

    def dict(self):
        d = {'pid':self.pid, 'name': self.name, 'status':self.status,
            'status_color': self.status_color,
            'first_seen':netarray.timestamp(self.first_seen),
            'last_seen':netarray.timestamp(self.last_seen)}

        if self.addr != None:
            d['addr'] = self.addr
            d['port'] = self.port

        return d

    def serialize(self):
        return json.dumps(self.dict())

    def update(self,p,addr=None):

        self.last_seen = p.systime
        self.pid = p.pid
        self.status = p.status
        self.status_color = p.status_color
        self.name = p.name
        self.port = p.port

        # recall that addr (but not port) needs to be found from the
        # associated network socket, so a Pulse object doesn't have
        # all the information we want
        if addr != None:
            self.addr = addr
            
    def __str__(self):
        return self.serialize()

    def __repr__(self):
        #TODO: return a brief text summary instead
        return self.serialize()

    def tty_out(self):

        t = netarray.timestamp(self.last_seen,fmt='short')
        t = rpad(t,15)
        dt = utcnow() - self.last_seen

        if dt < timedelta(minutes=1):
            t = colorize(t,color='green')

        if timedelta(minutes=1) < dt < timedelta(minutes=5):
            t = colorize(t,color='yellow')

        if dt > timedelta(minutes=5):
            t = colorize(t,color='red')

        name = colorize(self.name,'cyan')
        pid = str(self.pid)

        if self.addr != None:
            _addr,_port = self.addr,self.port
        else:
            _addr,_port = 'unknown host', ''

        addr = colorize('%s:%s'%(_addr,_port),'magenta')

        status = '[%s]'% cpad(self.status,8)
        status = colorize(status,self.status_color)

        b0 = rpad('%s@%s(pid %s) ' % (name,addr,pid), 57)
        b1 = t
        b2 = status

        return ''.join((b0,b1,b2))

class Snapshot(object):

    def __init__(self,s=None):

        self.systime = None

        # Name and network location indexed dictionaries of Entry objects. 
        self.names = {}
        self.locations = {} # keys are (addr,port) tuples

        if s != None:
            self.deserialize(s)


    def remove_entry(self,e):

        if e.name in self.names:
            del self.names[e.name]

        if (e.addr,e.port) in self.locations:
            del self.locations[(e.addr,e.port)]

    def add_entry(self,e):

        # in the event of collision, evict the previously stored Entry
        self.names[e.name] = e
        self.locations[(e.addr,e.port)] = e

    def purge_stale(self,t):
        """
        Remove Entry objects with last_seen longer than timedelta t ago.
        """
        now = utcnow()

        entries = self.names.values()
        for e in entries:
            if now - e.last_seen > t:
                self.remove_entry(e)

    def update(self,p,addr=None):

        self.systime = utcnow()

        if p.name in self.names:
            # Note that this operation is on the same Entry object in
            # self.locations. An (intended) consequence of this is that
            # seeing a the same publisher name at a new location or pid
            # causes eviction of the old location and pid information from
            # the present snapshot.
            self.names[p.name].update(p,addr)
        else:
            self.add_entry(Entry(p,addr))

    def tty_out(self):

        t = netarray.timestamp(self.systime,fmt='short')
        out = colorize('snapshot %s\n' % t,'cyan')
        for e in self.names.values():
            out += indent(e.tty_out()) + '\n'

        return out


    def deserialize(self,d):

        if isinstance(d,str):
            d = json.loads(d)

        self.systime = netarray.from_timestamp(d['systime'])
        for entry in d['entries']:
            if entry != None:
                self.add_entry(Entry(entry))


    def serialize(self):

        # test for empty snapshot
        if self.systime == None:
            return json.dumps({'event':'null_snapshot'})


        t = netarray.timestamp(self.systime)
        entries = []
        if len(self.names) > 0:
            entries = [e.serialize() for e in self.names.values()]
        d = {'event':'snapshot','systime':t,'entries':entries}

        return json.dumps(d)
