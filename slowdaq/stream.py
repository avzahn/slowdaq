from netstring import *
from collections import deque
from socket import *
import errno




import inspect
def caller():
    return inspect.currentframe().f_back.f_back.f_code.co_name

##### TODO: make stream status, errno , addresses into properties

class Stream(object):
    """
    Wrapper class to handle common nonblocking TCP socket operations, especially
    message framing and unframing using the netstring protocol.

    The most important methods facing the outside world are listen, connect,
    queue, sendall, and recvall.

    Basic usage is to queue() strings for sending. This leaves framed strings
    in the tcpbox queue. sendall() sends all the framed messages that it can,
    removing successes from tcpbox. Recieving messages is done by calling
    recvall() followed by get(), which will return a list of post-unframing
    messages. connect() and listen() create a socket to listen or connect to a
    tcp address.

    Each of these calls catches exceptions and stores the error number to
    self.errno. Caught exceptions that indicate connection problems will set
    self.status to closed.

    self.type stores the use of the stream's socket, whether to listen,
    connect to a remote address, or just passively send and recv.

    """

    def __init__(self, debug=False):

        # if true, dumps status changes to console
        self.debug = debug

        # retrieve and remove framed messages with inbox.get()
        self.inbox = Decoder()

        # messages not yet acknowledged by reciever, defaults to being synced
        # with tcpbox on every sendall()
        self.outbox = deque()

        # messages queued for the network stack
        self.tcpbox = deque()

        self._sock = None
        self._errno = None

        # 'listen', 'connect' or 'accept'
        self.type = None

        # 'listening', 'connected', 'accepted' or 'closed'
        self._status = None

        # TODO status should be property, and changing it should trigger
        # unified status change handling

        self._host_location = (None,None)
        self._remote_location = (None,None)

    @property
    def sock(self):
        return self._sock

    @sock.setter
    def sock(self,value):
        if self.debug:
            print caller(), 'SOCK SET',self

        self._sock = value

    @sock.deleter
    def sock(self):
        if self.debug:
            print caller(), 'SOCK DEL',self

        del self._sock

    @property
    def errno(self):
        return self._errno

    @errno.setter
    def errno(self,value):
        if self.debug:
            print caller(),'ERRNO SET', self
        self._errno = value

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self,value):
        if self.debug:
            print caller(),'STATUS SET', self
        self._status = value


    @property
    def remote_location(self):
        try:
            self._remote_location = self.sock.getpeername()
        except:
            pass
        return self._remote_location

    @remote_location.setter
    def remote_location(self,value):
        self._remote_location = value


    @property
    def host_location(self):
        try:
            self._host_location = self.sock.getsockname()
        except:
            pass
        return self._host_location

    @host_location.setter
    def host_location(self,value):
        self._host_location = value

    def __del__(self):
        if self.sock:
            self.sock.close()

    def listen(self,addr=None,port=None):
        """
        Create, bind(), and listen() a socket.

        If addr is None, automatically get a local ip and open port
        """

        self.type = 'listen'

        if addr == None:
            addr = gethostbyname(gethostname())
            self.host_location = (addr,0)
        else:
            self.host_location = (addr,port)

        s = socket(AF_INET,SOCK_STREAM)
        s.settimeout(0.0)
        s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        s.bind(self.host_location)
        s.listen(5)
        self.sock = s
        self.status = 'listening'

        return s

    def connect(self,addr=None,port=None):
        """
        If addr is None, try to connect to the last address and port to which a
        connection attempt was made.
        """

        self.type = 'connect'

        if addr != None:
            self.remote_location = (addr,port)
        try:
            s = socket(AF_INET,SOCK_STREAM)
            s.settimeout(1.0)
            s.connect(self.remote_location)
            self.status = 'connected'
            s.settimeout(0.0)
            self.sock = s
        except error as e:
            self.errno = e.errno
            self.status = 'closed'

    def queue(self,data):
        """
        Encode data as netstring and queue for sending
        """
        prefix = '%s:'%(str(len(data)))
        size = len(prefix) + len(data) + 1

        # large pieces of data incur significant overhead to copy
        # into a netstring
        if size > 4096:
            self.outbox.append(prefix)
            self.outbox.append(data)
            self.outbox.append(',')
            self.tcpbox.append(prefix)
            self.tcpbox.append(data)
            self.tcpbox.append(',')
        else:
            data = '%s%s,' % (prefix,data)
            self.outbox.append(data)
            self.tcpbox.append(data)

    def close(self):
        self.status = 'closed'
        self.sock.close()
        self.sock = None

    def __repr__(self):

        if self.errno == None:
            e = 'None'
        else:
            e = errno.errorcode[self.errno]

        s = 'Stream [%s]@(%s,%s)->(%s,%s):%s(err=%s)' % (self.type,\
            self.host_location[0],\
            str(self.host_location[1]),\
            self.remote_location[0],\
            str(self.remote_location[1]),\
            self.status,\
            e)

        return s


    def sendall(self, clear_outbox = True):
        """
        Send all messages in self.tcpbox. Assumes all message entries are valid
        netstring and have been queued by self.queue.
        """

        retries = 4
        total_bytes_sent = 0

        while len(self.tcpbox) > 0:

            if retries == 0:
                break

            data = self.tcpbox[-1]

            try:
                n = self.sock.send(data)
                total_bytes_sent += n

                if n < len(data):
                    data = data[0:n]
                    self.tcpbox[-1] = data
                    retries -= 1

                    if n == 0:
                        break

                else:
                    self.tcpbox.pop()

            except error as e:
                self.errno = e.errno
                if not self.errno in (errno.EWOULDBLOCK,errno.EAGAIN):
                    self.status = 'closed'
                break

        if clear_outbox:
            # Some applications may have an acknowledgement of receipt protocol,
            # so we store messages in outbox after they've been sent to the tcp
            # stack in case they need to be resent later. The default behavior
            # however is to sync outbox and tcpbox, so that receipt unaware
            # applications don't have to worry about losing memory to an
            # overgrown outbox.
            self.outbox = deque(self.tcpbox)

        return total_bytes_sent

    def recvall(self):
        """
        recv() all data available on the socket, check for errors and closed
        closed connections, and feed any data found into self.inbox for decoding
        or unframing.
        """

        while True:
            try:
                data = self.sock.recv(4096)

                if data == '':
                    self.status = 'closed'
                    break

            except error as e:
                # Not all errors are bad. Most of the time, we're
                # waiting for EWOULDBLOCK as the loop end condition
                self.errno = e.errno
                if not self.errno in (errno.EWOULDBLOCK,errno.EAGAIN):
                    self.status = 'closed'
                break

            self.inbox.feed(data)

    def accept(self):
        """
        The stream equivalent of socket.socket.accept(). If stream type is
        'listen', return a new stream object that manages self.sock.accept()
        """

        s, addr = self.sock.accept()
        s.settimeout(0)
        host,addr = s.getpeername()
        m = Stream()
        m.sock = s
        m.remote_location = (host,addr)
        m.type = 'accept'
        m.status = 'accepted'

        return m

    def clear_outbox(self,nbytes):

        _nbytes = nbytes

        while nbytes > 0:

            data = self.outbox[-1]
            n = len(data)

            if n <= nbytes:
                self.outbox.pop()

            else:
                data = data[0:nbytes]
                self.outbox[-1] = data

            nbytes -= n

    def get(self):
        # TODO remove the need to reverse this
        return self.inbox.get()[::-1]

from select import select as _select

def select(rlist,wlist,xlist,timeout):
    """
    Forward the capabilities of the standard select() for use with the Stream
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
    """
    Simple server implementation based on the Stream class meant for
    subclassing in any particular application.

    Special behavior for a particular application is by subclassing this and
    overriding the handle_* callback methods.
    """

    def __init__(self, debug=False):

        # if true, set all streams to debug output mode
        self.debug = debug

        self.streams = []
        self.outbox = deque([])
        self.connecting = []

    def queue(self,msg):
        for s in self.streams:
            if s.type != 'listen':
                s.queue(msg)

    def add_connection(self,addr,port):
        """
        Create new stream and connect() it to address. If connect() fails, queue
        that new stream to retry on the next serve() call.
        """

        # don't add an existing connection
        for stream in self.streams:
            if stream.remote_location == (addr,port):
                return stream

        s = Stream(debug=self.debug)
        s.connect(addr,port)

        if s.status == 'closed':
            # will retry on next call to serve()
            self.connecting.append(s)

        self.streams.append(s)

        return s

    def remove_connection(self, addr, port):
        """
        Destroy any streams connected to address, and make sure they aren't
        queued for reconnect. Does nothing if addr and port are not found.
        """
        location = (addr,port)
        remove = []
        for stream in self.streams:
            if stream.remote_location == location:
                remove.append(stream)

        for r in remove:
            self.streams.remove(r)
            try:
                r.close()
            except:
                pass
            if r in self.connecting:
                self.connecting.remove(r)

    def add_listener(self,addr=None,port=None):
        """
        If addr is None, automatically get a local ip and open port

        TODO: error handling similar to add_connection
        """
        s = Stream(debug=self.debug)
        s.listen(addr,port)
        self.streams.append(s)


    def handle_recv(self,stream,msgs):
        """
        Called whenever valid message frames are found. Arguments are the stream
        that generated the messages, and the list of post-decoded messages.

        TODO: create a callback slot for invalid message frame reception
        """
        pass

    def handle_accept(self,stream):
        pass

    def handle_close(self,stream):
        stream.close()
        if stream.type == 'connect':
            # if the stream was meant to connect(), retry the connection
            self.connecting.append(stream)
        else:
            self.streams.remove(stream)

    def serve(self,timeout=0):

        streams = self.streams[:]

        # Attempt to connect() for every stream in the connecting queue. For
        # some reason, nonblocking sockets don't always connect() even after
        # being polled writable, throwing EWOULDBLOCK. Instead, stream.connect
        # just sets a short blocking timeout on all the sockets, and we attempt
        # a synchronouss connect here
        for stream in self.connecting:
            stream.connect()
            if stream.status != 'closed':
                # A closed status indicates the connect() failed. On success,
                # remove the stream from the list of streams requiring
                # connecting
                self.connecting.remove(stream)
            else:
                # don't want to select() on a dead socket
                streams.remove(stream)

        r,w,e = select(streams,streams,[],timeout)

        for _r in r:

            if _r.type == 'listen':
                s = _r.accept()
                self.streams.append(s)
                self.handle_accept(s)

            else:
                _r.recvall()
                msgs = _r.get()
                if len(msgs) > 0:
                    self.handle_recv(_r,msgs)

            if _r.status == 'closed':
                self.handle_close(_r)
                if _r in w:
                    w.remove(_r)

        for _w in w:

            _w.sendall()

            if _w.status == 'closed':
                self.handle_close(_w)
