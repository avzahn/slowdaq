"""
Module implementing a skeleton asynchronous TCP server. This is similar to, but
less general than, the concept of a "reactor" in some frameworks such as
(notably) Twisted. The user programs the server by subclassing the Server class,
and overriding a set of callback methods that run whenever important events
occur. The Server then does absolutely nothing until its select method gets
called, at which point it will dispatch all the appropriate callbacks. This
allows the program running the Server to manage many sockets at a time while
still managing other work without resorting to multithreading or multiprocess
parallelism.

In the future, the plan is to add support for waiting on arbitrary file
descriptors. This will allow the Server class to set timers elegantly on
unixlike platforms. 

For the uninitiated, asynchronous sockets are ordinary sockets with an option
set to not block on any operation. Any socket operation that would block returns
immediately with an exception. In order to do anything with them, the select /
poll family of system calls exists to ask the OS which sockets are ready for
reading or writing. The length of time these calls will wait for sockets to be
ready can be controlled by passing a timeout parameter. See netstring.select
for more information, or see the python documentation on the select module.

We use the netstring infrastructure in netstring.py in order to abstract away
the problem of determining where discrete user messages begin and end in a TCP
stream. In principle, there is no reason that this module can't be adapted to
use a different protocol. To do that, it is preferred to create a module
that exports a Socket class that emulates the same functionality as
netstring.Socket, and then tell this module somehow which protocol module to
import from.

On the choice of the netstring protocol:

Netstring is a length-prefixed, as opposed to terminator-separated,
protocol. The advantage of using a length-prefixed protocol is that we don't
ever have to worry about terminator bytes showing up inside our message
payloads. A disadvantage is that messages take slightly more space. Netstring
is not actually the most compact length-prefixed protocol, but it seems worth
the price for its human readability as long as we don't intend to send and
recieve a great many short messages.
"""

from . netstring import *
from select import select as _select

def select(rlist,wlist,xlist,timeout):
    """
    Forward the capabilities of the standard select() for use with the Socket
    class. In the future, might be worth adding poll/epoll/kqueue support.
    """
    rdict,wdict = {},{}
    
    # Python builtin sockets are valid hashable objects
    for r in rlist:
        rdict[r.sock] = r
    
    for w in wlist:
        wdict[w.sock] = w

    rsl = [r.sock for r in rlist]
    wsl = [w.sock for w in wlist]
    
    rsl,wsl,esl = _select(rsl,wsl,[],timeout)
    
    return [rdict[r] for r in rsl], [wdict[w] for w in wsl],[]

class Server(object):
    
    def __init__(self,debug=False):

        # sockets on which we have called connect()
        self.connected = []
        
        # sockets created from an accept() call
        self.accepted = []
        
        # sockets on which we have called listen() and bind()
        self.listening = []
        
        self.debug = debug

    def dprint(self,msg):
        if self.debug:
            print msg
               
    # override these
    def on_connect(self,sock):
        """
        Called after connect() has been called on the argument Socket
        """
        pass
    
    def on_accept(self,sock):
        """
        Called after the argument socket is created from another Socket's
        accept().
        """
        pass
    
    def on_disconnect(self,sock,msgs):
        """
        Called after the argument Socket has been determined to be defunct. The
        Socket will be destroyed once this callback completes. Any messages
        fetch_decode()'d from that Socket from the current select cycle are also
        passed as argument.
        """
        pass
    
    def on_fetch_decode(self,sock,msgs):
        """
        Called after a Socket completes a fetch_decode(). The Socket and list
        of messages found are passed as argument. This callback is not
        responsible for catching a null message.
        """
        pass
    
    def on_writable(self,sock):
        """
        Called whenever sock is writable
        """
        pass
    
    def connectTCP(self,addr,port):
        sock = Socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1.0)
        sock.connect((addr,port))
        sock.settimeout(0.0)
        self.connected.append(sock)
        self.on_connect(sock)
        return sock
    
    def listenTCP(self,addr,port):
        """
        Also bind()
        """
        sock = Socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.0)
        sock.bind((addr,port))
        sock.listen(5)
        self.listening.append(sock)
        
        return sock.getsockname()
    
    def all_sockets(self,listening=True):
        
        if listening:
            return self.accepted+self.listening+self.connected
        else:
            return self.accepted+self.connected
    
    def listenfd(self,fd):
        pass
    
    def on_fd_writable(self,fd):
        pass
    
    def on_fd_readable(self,fd):
        pass
    
    def remove(self,sock):
        
        if sock in self.connected:
            self.connected.remove(sock)
            sock.close()
        if sock in self.listening:
            self.listening.remove(sock)
            sock.close()
        if sock in self.accepted:
            self.accepted.remove(sock)
            sock.close()
            
    def push(self,msg):
        for sock in self.all_sockets(listening=False):
            sock.push(msg)
                
    def select(self,timeout=0.0):
        """
        The reactive core of this object. Waits in a select call on all the 
        Sockets we're aware of, and then dispatches the appropriate user defined
        callbacks based on what it finds.
        """
        
        sockets = self.accepted + self.connected + self.listening
        
        # wait for readable (r) or writable (w) Sockets
        r,w,e = select(sockets,sockets,[],timeout)
        
        self.dprint( 'server select' )

        for _r in r:
            
            if _r in self.listening:
                try:
                    sock, addr = _r.accept()
                    sock.settimeout(0.0)
                    self.accepted.append(sock)                
                    self.on_accept(sock)
                    self.dprint( 'server on_accept')
                except:
                    pass
            else:
                msgs = _r.fetch_decode()
                status = _r.status
                
                if msgs != []:
                    self.on_fetch_decode(_r,msgs)
                    self.dprint('server on_fetch_decode')
                    
                if status != 'ok':
                    self.dprint( 'server on_disconnect')
                    self.on_disconnect(_r,msgs)
                    self.remove(_r)
                
        for _w in w:
            self.on_writable(_w)
            self.dprint( 'server on_writable')
            
            if _w.status != 'ok':
                self.remove(_w)
                
