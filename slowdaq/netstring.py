"""
Netstring is a simple, human readable, standardized protocol for encoding
length prefixed data. Netstrings begin with an ascii decimal number containing
the length of the message. The message itself followed by a comma comes
after. For example, the netstrings 3:asd, and 0:, respectively encode the string
abc and the null string.

This module implements basic tools for working with netstring encoded data. The
primary export products here are the Socket and File classes. These are designed
to look like ordinary socket and file objects that do all their work in
netstring encoded format, while sheltering the user as much as possible from the
use of the netstring protocol.

Decoder and mutable_string are helper classes for Socket and File. Hopefully,
the user won't have to see them.
"""

import re
import numpy as np

class mutable_string(object):
    """
    A mutable string class for use by the Decoder class. Does its best to
    emulate the string and list interfaces where you would naturally expect it
    to.
    
    Usage:
    
    s = mutable_string('asdf')
    s.append('qwerty') # no memory copy of the 'asdf'
    
    print s[0:3]
        >> asdf
        
    del s[0:2] # involves some, usually small, copying (see implementation) 
    print s
        >> dfqwerty
    
    Python strings are immutable, so that concatenation is very expensive
    because each concatenation operation costs a copy of both strings involved.
    For the Decoder, which may have to sequentially build up large strings via
    many concatenations, this is a problem.
    
    Similarly, python strings can't directly support character deletion, and a
    naive attempt at building a new string with the desired characters removed
    also costs a potentially large copy. This is also a problem for the Decoder,
    which may need to delete message terminators and prefixes from arbitrary
    positions in a string. There's room for improvement in the deletion
    algorithm implemented here, but it should be reasonable most of the time.
    
    So we implement a class that manipulates lists of strings. Concatenation
    is free, and deletion is usually mostly ok.
    
    One alternative to research is the python3 bytearray.
    """
    
    def __init__(self,s=None):

        # the data buffer, implemented as a list of strings
        self.blocks = []

        # lengths of the strings, also called blocks
        self.lengths = [0]
        
        # indices into the mutable_string at the start of each block
        self.idx = [0]
        
        if s != None:
            self.append(s)
            
    def append(self,s):
        
        self.lengths.append(len(s))
        self.idx = np.cumsum(self.lengths)
        self.blocks.append(s)
        
    def __str__(self):
        
        return ''.join(self.blocks)
    
    def __repr__(self):
        return repr(self.blocks)
        
    def _get(self,string_idx,char_idx):
        
        return self.blocks[string_idx][char_idx]
        
        
    def _find(self,i):
        """
        Find the index into self.blocks that points at the block containing the
        index i into the mutable_string, and the index into that block pointing
        at the character that i points at.
        """
        
        if i < 0:
            i = len(self) + i
        
        if i > len(self)-1:
            raise IndexError()
        
        for n in range(len(self.idx)):
            
            l = self.idx[n]
            u = self.idx[n+1]
            
            #print n,l,u
            
            if l <= i < u:
                
                block = n
                char = i - l
                
                return block,char
            
    def _get_range(self,start,stop):
        """
        Slice out an index range into a regular python string
        """
        
        if start == len(self):
            return ''
        
        block0,char0 = self._find(start)
        
        if stop == len(self):
            
            block1,char1 = len(self.blocks)-1,len(self.blocks[-1])
            
        else:
            block1,char1 = self._find(stop)
        
        if block0 == block1:
            return self.blocks[block0][char0:char1]
        
        s0 = [self.blocks[block0][char0:]]
        s1 = [self.blocks[block1][:char1]]
        
        return ''.join( s0 + self.blocks[block0+1:block1] + s1 )
    
    
    def _reset_idx(self):
        """
        Clean up any empty blocks and recalculate self.idx
        """
        for i,block in enumerate(self.blocks):
            if len(block) == 0:
                del self.blocks[i]
        
        self.lengths = [0] + [len(block) for block in self.blocks]
        self.idx = np.cumsum(self.lengths)
    
    def _del_range(self,start,stop):
        
        b0 = self[:start]
        b1 = self[stop:]
        
        self.blocks = [b0,b1]
        self._reset_idx()
        
    def __len__(self):
        return self.idx[-1]
    
    def __getitem__(self,i):
        
        if isinstance(i,int):
            block,char = self._find(i)
            return self._get(block,char)
        
        start = i.start
        stop = i.stop
        if start == None: start = 0
        if stop == None: stop = self.idx[-1]
        
        return self._get_range(start, stop)
    
    
    def __delitem__(self,i):
        
        if isinstance(i,int):
            
            if i == len(self):
                raise IndexError()
            
            start = i
            stop = i
        else:
            start = i.start
            stop = i.stop
            
            if start == None: start = 0
            if stop == None: stop = len(self)
            
        self._del_range(start,stop)
    
    
    def __iter__(self):
        return _mutable_string_iter(self)
            
class _mutable_string_iter(object):
    def __init__(self,target):
        self.target = target
        self.cursor = 0
        
    def next(self):
        if self.cursor == len(self.target):
            raise StopIteration()
        char = self.target[self.cursor]
        self.cursor += 1
        return char


class Decoder(object):
    
    def __init__(self):
        
        # maximum number of digits a prefix can have. Sets the size in bytes of
        # the largest allowable message
        self.max_size_digits = 9
        
        self.buf = mutable_string()
        
        self.start = None
        self.end = None
        
        # Accumulated ordered netstring payloads found. Negative values are the
        # number of nonconforming bytes.
        self.msg = []
        
        # tracks the size in bytes of the prefix of the netstring payload
        # currently under construction. Kept just in case that netstring ends up
        # invalid so that we can correctly tally the number of bad bytes
        self.prefix_len = None
        
        self.start_pattern = re.compile('([0-9]{1,%i})(.*?):'\
        %(self.max_size_digits),flags=re.DOTALL)
        
    def _seek_start(self):
        """
        Set start and end indices to the start and end indices of the first
        netstring in the buffer, even if the buffer doesn't have all of that
        netstring yet.
        """
        
        end = 0
            
        while True:
            # regex search over a substring containing the buffer start for a
            # valid prefix. If not found, look at a slightly larger substring
            # on the next iteration.
            
            # potentially catastrophically slow if we end up scanning a lot of
            # the buffer this way, but unlikely that will happen.
            
            # TODO: figure out something with a better worst case that still has
            # a reasonable average case.
            
            end = min(len(self.buf), end + self.max_size_digits + 1)
                
            buf = self.buf[0:end]
            
            m = self.start_pattern.search(buf)
 
            if m != None:
                 
                self.start = m.start(1)+len(m.group(1)) + 1
                self.end = self.start + int(m.group(1))
                self.prefix_len = len(m.group(1)) + 1
                return -m.start(1)
            
            if end == len(self.buf):
                return None
 
    def _seek_end(self):
        """
        Find the index of the terminating comma in the buffer
        """
        if len(self.buf) <= self.end:
            return None
        
        if self.buf[self.end] == ',':
            ret = self.buf[self.start:self.end]
            del self.buf[:self.end+1]
            self.start, self.end, self.prefix_len = None,None,None
            return ret
        else:
            ret = -(self.end - self.start) - self.prefix_len
            del self.buf[:self.end+1]
            self.start, self.end, self.prefix_len = None,None,None
            return ret
        
    def _next_msg(self):
        
        msg = None
        
        if self.start == None:
            bad = self._seek_start()
            
            if bad != 0 and bad != None:
                self.msg.append(bad)
            
        if self.end != None:
            msg = self._seek_end()
            if msg != None:
                self.msg.append(msg)

        return msg
                
        
    def feed(self,s):
        
        self.buf.append(s)
        msgs = []
        
        while True:
            msg = self._next_msg()

            if msg == None:
                break
                
    def get(self):
        """
        Return all the message payloads decoded so far in order and forget them.
        Note that negative integers in the return value represent a number of
        bytes that don't conform to the netstring protocol.
        """
        ret = self.msg
        self.msg = []
        return ret
        
class File(object):
    """
    Encodes netstring messages to disk and decodes netstrings from disk. The
    interface is similar to python builtin file object in that it's iterable
    and a context manager. Example:
    
    with File('test.net','a') as f:
        f.write('asdf')
        f.write('qwerty')
    
    
    with File('test.net', 'r') as f:
        for msg in f:
            print msg
                >> asdf # first iteration
                >> qwerty # second iteration
    
    """
    def __init__(self,fname,mode):
        
        self.fname = fname
        self.mode = mode
        self.decoder = Decoder()
        self.blocksize = (1<<24)
        
        self.file = None
        
    def __enter__(self):
        self.file = open(self.fname,self.mode,self.blocksize)
        return self
    
    def __exit__(self,*args):
        self.file.close()
        
    def __iter__(self):
        
        fdata = None
        
        while fdata != '':
            
            fdata = self.file.read(self.blocksize)
            self.decoder.feed(fdata)
            
            for msg in self.decoder.get():
                if not isinstance(msg,int):
                    yield msg
                
    def flush(self):
        self.file.flush()
        
    def write(self,msg):
        
        f = self.file
        # technically, adding a newline isn't netstring, but the Decoder can
        # handle it, and it adds some human readability
        prefix = '\n%s:'%(str(len(msg)))
        
        if len(msg) > self.blocksize:
            
            f.write(prefix)
            f.write(msg)
            f.write(',')
                
        else:
            f.write('%s%s,'%(prefix,msg))
            
import time
import errno
import socket
from collections import deque

class Socket(object):
    """
    Wrapper class for socket.socket that forwards most (in the future,
    potentially all) of its interface while providing some higher level features
    for transparently exchanging data over standard sockets using the netstring
    protocol. So far, only TCP is working. It is generally assumed also that
    users will only want asynchronous sockets.
    
    Each Socket stores an output queue in case socket.socket calls fail for any
    reason. One nice side effect of this is that you can send before a connect
    completes and not worry about it.
    
    The methods intended for the outside world to use are send_encoded,
    fetch_decode, and get.
    
    Rather than raising exceptions in the event of errors, this class will set
    self.status.
    """
    def __init__(self,*args,**kwargs):
        """
        Wrap an existing socket or wrap a new one by passing all the initializer
        arguments straight through.
        """
        
        if 'from_socket' in kwargs:
            self.sock = kwargs['from_socket']
        else:
            self.sock = socket.socket(*args)
            
        self.decoder = Decoder()
        self.blocksize = 4096
        self.queue = deque()
        self.status = 'ok'
       
    def connect(self,*args,**kwargs):
        return self.sock.connect(*args,**kwargs)
       
    def fileno(self):
        return self.sock.fileno()
        
    def getpeername(self,*args,**kwargs):
        return self.sock.getpeername(*args,**kwargs)
        
    def push(self,msg):
        self.queue.appendleft(msg)
        
    def settimeout(self,t):
        return self.sock.settimeout(t)
    
    def listen(self,*args):
        return self.sock.listen(*args)
    
    def bind(self,*args):
        return self.sock.bind(*args)
        
    def getsockname(self,*args):
        return self.sock.getsockname(*args)
        
    def transmit(self):
        """
        Attempt to send everything in self.queue though self.send_encoded.
        Returns the number of elements remaining in the queue.
        """

        while len(self.queue) > 0:

            msg = self.queue[-1]

            sent,intended = self.send_encoded(msg)

            if sent == intended:
                self.queue.pop()
            else:
                break
            
        return len(self.queue)

    def close(self):
        self.sock.close()
        return self.queue

    def send_encoded(self,msg):
        """
        Send a netstring encoded version of msg. Return the number of bytes
        sent and the number of bytes intended to be sent.
        """

        bytes_sent = 0

        prefix = '\n%s:'%(str(len(msg)))
        
        intended = len(prefix)+len(msg)+1

        if len(msg) > self.blocksize:
            
            bytes_sent += self.send_all(prefix)
            bytes_sent += self.send_all(msg)
            bytes_sent += self.send_all(',')
                
        else:
            bytes_sent += self.send_all('%s%s,'%(prefix,msg))
        
        return bytes_sent, intended

    def send_all(self,msg):
        """
        Replacement for socket.sendall that returns the number of bytes sent
        """
        
        intended = len(msg)
        sent = 0
        
        while sent < intended:
            
            try:
                sent += self.sock.send(msg)
            except socket.error as e:
                self.status = e.errno
                break
            
        return sent

    def recv(self,nbytes):
        """
        Exactly like socket.recv, except also feeds all the data it retrieves
        into a netstring decoder.
        """
        
        sdata = self.sock.recv(nbytes)
        
        if sdata != '':
            self.decoder.feed(sdata)
            
        return sdata
      
    def fetch_decode(self):
        """
        Keep hitting self.recv() until no more data is available, then
        self.get() all the message payloads found. Requires socket to be
        asynchronous.
        """
        
        status = 'ok'
        
        while True:
            
            try:
                data = self.recv(self.blocksize)
                if data == '':
                    status = 'closed'
                    break
            except:
                break
        
        self.status = status
        
        return self.get()
    
    def get(self):
        """
        Return all the message payloads found so far and forget them
        """
        # Decoder could return negative integer values to indicate non netstring
        # bytes found (that is, bytes preceding a prefix that aren't valid
        # ascii 0-9, or bytes in excess of the the bytes expected based on a
        # prefix). For now, just ignore those.
        return [msg for msg in self.decoder.get() if not isinstance(msg,int)]
                            
    def accept(self):
        s,addr = self.sock.accept()
        return Socket(from_socket=s),addr
            

