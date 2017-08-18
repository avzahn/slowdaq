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

class Entry(object):

    def __init__(self,d=None):

        self.systime = None
        self.pid = None
        self.addr = None
        self.port = None
        self.status = None
        self.status_color = 'white'
        self.name = None

        if d != None:
            self.deserialize(d)

    def __eq__(self,other):
        return self.dict() == other.dict()

    def __ne__(self,other):
        return not self == other

    def __hash__(self):
        h = 0
        for i in self.dict().items():
            h += hash(i)
        return h

    def copy(self):
        e = Entry()
        e.deserialize(self.serialize)
        return e

    def dict(self):

        d = {'systime':netarray.timestamp(self.systime),'status':self.status,
            'pid':self.pid, 'addr':self.addr,'port':self.port,'name':self.name,
            'status_color':self.status_color,'event':'pulse'}

        return d

    def serialize(self):
        return json.dumps(self.dict())

    def deserialize(self,d):
        """
        Set self fields from a dict or JSON string
        """

        if isinstance(d,str):
            d = json.loads(str)

        self.systime =netarray.from_timestamp(d['systime'])
        self.pid = d['pid']
        self.addr = d['addr']
        self.port = d['port']
        self.status = d['status']
        self.status_color = d['status_color']
        self.name = d['name']

    def isupdate(self,other):
        """
        self updates other iff self has the same network location and pid while
        having a later systime
        """
        if self.addr != other.addr:
            return False
        if str(self.port) != str(other.port):
            return False
        if self.systime < other.systime:
            return False
        return True

    def __str__(self):

        t = netarray.timestamp(self.systime,fmt='short')
        t = rpad(t,15)
        dt = utcnow() - self.systime

        if dt < timedelta(minutes=1):
            t = colorize(t,color='green')

        if timedelta(minutes=1) < dt < timedelta(minutes=5):
            t = colorize(t,color='yellow')

        if dt > timedelta(minutes=5):
            t = colorize(t,color='red')

        name = colorize(self.name,'cyan')
        pid = str(self.pid)
        addr = colorize('%s:%s'%(self.addr,self.port),'magenta')

        status = '[%s]'% cpad(self.status,8)
        status = colorize(status,self.status_color)

        b0 = rpad('%s@%s(pid %s) ' % (name,addr,pid), 57)
        b1 = t
        b2 = status

        return ''.join((b0,b1,b2))

class SnapshotDiff(object):
    """
    Manage add and remove lists of Entry objects representing a diff between
    two Snapshot objects.
    """

    def __init__(self,add,remove):
        self.add = add
        self.remove = remove

    def __str__(self):

        lines = []
        lines.append('Add:\n')
        for entry in self.add:
            line = indent(colorize('+ ','green')+str(entry)+'\n',4)
            lines.append(line)
        lines.append('Remove:\n')
        for entry in self.remove:
            line = indent(colorize('- ','red')+str(entry)+'\n',4)
            lines.append(line)

        return ''.join(lines)

class Snapshot(object):

    def __init__(self,d=None):

        self.systime = None
        self.entries = []
        self.log = None

        if d != None:
            self.deserialize(d)

    def __str__(self):
        t = netarray.timestamp(self.systime,fmt='short')
        out = colorize('snapshot %s\n' % t,'cyan')
        for e in self.entries:
            out += indent(str(e)) + '\n'

        return out


    def add_entry(self, entry):
        """
        Add an Entry, dict, or JSON string to self.entries. Remove any entries
        that entry is an update of.
        """
        if isinstance(entry,dict) or isinstance(entry,str):
            entry = Entry(entry)

        for e in self.entries:
            if entry.isupdate(e):
                self.remove_entry(e)

        self.entries.append(entry)

    def remove_entry(self,entry):
        while entry in self.entries:
            self.entries.remove(entry)

    def by_age(self,t):
        """
        Return lists of references to entries older and newer than a
        timedelta t
        """
        older = []
        newer = []
        now = utcnow()
        for e in self.entries:
            if now-e.systime > t:
                older.append(e)
            else:
                newer.append(e)

        return older,newer

    def remove_oldest(self,t):
        """
        Remove any entries older than a timdelta t
        """
        older,newer = self.by_age(t)
        for e in older:
            self.remove_entry(e)

    def __sub__(self,other):
        """
        Diff self with other
        """

        self_addrs = set(self.entries)
        other_addrs = set(other.entries)

        add = list(other_addrs - self_addrs)
        remove = list(self_addrs - other_addrs)

        return SnapshotDiff(add,remove)

    def deserialize(self,d):

        if isinstance(d,str):
            d = json.loads(d)

        self.systime = netarray.from_timestamp(d['systime'])
        self.log = d['log']
        self.entries = []
        for entry in d['entries']:
            self.add_entry(Entry(entry))

    def serialize(self):
        t = netarray.timestamp(self.systime)
        entries = [entry.dict() for entry in self.entries]
        d = {'event':'snapshot','systime':t,'log':self.log,'entries':entries}
        return json.dumps(d)
