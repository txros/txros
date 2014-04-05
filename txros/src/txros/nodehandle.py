import os
import struct
import traceback
import StringIO

from twisted.web import server, xmlrpc
from twisted.internet import defer, protocol, reactor, endpoints, error
from twisted.protocols import basic

from roscpp.srv import GetLoggers, GetLoggersResponse, SetLoggerLevel, SetLoggerLevelResponse

from txros import util


class Server(xmlrpc.XMLRPC):
    '''
    An example object to be published.
    '''
    def __init__(self, node_handle):
        xmlrpc.XMLRPC.__init__(self)
        self._node_handle = node_handle
    
    def xmlrpc_publisherUpdate(self, caller_id, topic, publishers):
        #print locals()
        self._node_handle._server_handlers[topic](publishers)
        #raise xmlrpc.Fault(123, 'The fault procedure is faulty.')

def deserialize_list(s):
    pos = 0
    res = []
    while pos != len(s):
        length, = struct.unpack('<I', s[pos:pos+4])
        if pos+4+length > len(s):
            raise ValueError('early end')
        res.append(s[pos+4:pos+4+length])
        pos = pos+4+length
    return res
def serialize_list(lst):
    return ''.join(struct.pack('<I', len(x)) + x for x in lst)

def deserialize_dict(s):
    res = {}
    for item in deserialize_list(s):
        key, value = item.split('=', 1)
        res[key] = value
    return res
def serialize_dict(s):
    return serialize_list('%s=%s' % (k, v) for k, v in s.iteritems())

class TCPROSServer(basic.IntNStringReceiver):
    structFormat = '<I'
    prefixLength = struct.calcsize(structFormat)
    MAX_LENGTH = 2**32
    
    def __init__(self, node_handle):
        self._node_handle = node_handle
    
    def stringReceived(self, string):
        header = deserialize_dict(string)
        if 'service' in header:
            self.stringReceived = self._node_handle._tcpros_handlers['service', header['service']](header, self)
        else:
            assert False

class TCPROSClient(basic.IntNStringReceiver):
    structFormat = '<I'
    prefixLength = struct.calcsize(structFormat)
    MAX_LENGTH = 2**32
    
    def __init__(self):
        self.queue = util.DeferredQueue()
    
    def stringReceived(self, string):
        self.queue.add(string)
    
    def connectionLost(self, reason):
        self.queue.add(reason) # reason is a Failure

class NodeHandle(object):
    def __init__(self, name):
        self._ns = ''
        self._name = self._ns + '/' + name
        
        self._proxy = xmlrpc.Proxy(os.environ['ROS_MASTER_URI'])
        #self._proxy.callRemote('getParam', '/test', '/').addCallbacks(printValue, printError)
        self._addr = '127.0.0.1' # XXX
        
        self._server = reactor.listenTCP(0, server.Site(Server(self)))
        self._server_uri = 'http://%s:%i/' % (self._addr, self._server.getHost().port)
        self._server_handlers = {}
        
        self._tcpros_server = reactor.listenTCP(0, util.AutoServerFactory(TCPROSServer, self))
        self._tcpros_server_uri = 'rosrpc://%s:%i' % (self._addr, self._tcpros_server.getHost().port)
        self._tcpros_handlers = {}
        
        self.advertise_service('~get_loggers', GetLoggers, lambda req: GetLoggersResponse())
        self.advertise_service('~set_logger_level', SetLoggerLevel, lambda req: SetLoggerLevelResponse())
    
    def resolve(self, name):
        if name.startswith('/'):
            return name
        elif name.startswith('~'):
            return self._name + '/' + name[1:]
        else:
            return self._ns + '/' + name
    
    def advertise_service(self, *args, **kwargs):
        return Service(self, *args, **kwargs)
    
    def subscribe(self, *args, **kwargs):
        return Subscriber(self, *args, **kwargs)

class Service(object):
    def __init__(self, node_handle, name, service_type, callback):
        self._node_handle = node_handle
        self._name = node_handle.resolve(name)
        
        self._type = service_type
        self._callback = callback
        
        assert ('service', self._name) not in node_handle._tcpros_handlers
        node_handle._tcpros_handlers['service', self._name] = self._handle_tcpros_conn
        
        node_handle._proxy.callRemote('registerService',
            node_handle._name, self._name,
            node_handle._tcpros_server_uri, node_handle._server_uri) # XXX check result
    
    def _handle_tcpros_conn(self, headers, conn):
        conn.sendString(serialize_dict(dict(
            callerid=self._node_handle._name,
            type=self._type._type,
            md5sum=self._type._md5sum,
            request_type=self._type._request_class._type,
            response_type=self._type._response_class._type,
        )))
        @util.inlineCallbacks
        def more(string):
            req = self._type._request_class().deserialize(string)
            try:
                resp = yield self._callback(req)
            except Exception as e:
                traceback.print_exc()
                conn.transport.write(chr(0)) # ew
                conn.sendString(str(e))
            else:
                assert isinstance(resp, self._type._response_class)
                conn.transport.write(chr(1)) # ew
                x = StringIO.StringIO()
                resp.serialize(x)
                conn.sendString(x.getvalue())
        return more

class Subscriber(object):
    def __init__(self, node_handle, name, message_type, callback):
        self._node_handle = node_handle
        self._name = node_handle.resolve(name)
        
        self._type = message_type
        self._callback = callback
        
        self._publisher_threads = {}
        
        assert self._name not in node_handle._server_handlers
        node_handle._server_handlers[self._name] = self._handle_publisher_list
        
        @util.inlineCallbacks
        def reg():
            statusCode, statusMessage, value = yield node_handle._proxy.callRemote('registerSubscriber',
                node_handle._name, self._name,
                self._type._type, node_handle._server_uri)
            assert statusCode == 1
            
            self._handle_publisher_list(value)
        reg()
    
    @util.inlineCallbacks
    def _publisher_thread(self, url):
        while True:
            try:
                proxy = xmlrpc.Proxy(url)
                statusCode, statusMessage, value = yield proxy.callRemote('requestTopic', self._node_handle._name, self._name, [['TCPROS']])
                assert statusCode == 1
                
                protocol, host, port = value
                conn = yield endpoints.TCP4ClientEndpoint(reactor, host, port).connect(util.AutoServerFactory(TCPROSClient))
                try:
                    conn.sendString(serialize_dict(dict(
                        message_definition=self._type._full_text,
                        callerid=self._node_handle._name,
                        topic=self._name,
                        md5sum=self._type._md5sum,
                        type=self._type._type,
                    )))
                    header = deserialize_dict((yield conn.queue.get_next()))
                    # XXX do something with header
                    while True:
                        data = yield conn.queue.get_next()
                        msg = self._type().deserialize(data)
                        self._callback(msg)
                finally:
                    conn.transport.loseConnection()
            except (error.ConnectionDone, error.ConnectionLost):
                pass
            except Exception:
                traceback.print_exc()
            
            yield util.sleep(1)
    
    def _handle_publisher_list(self, publishers):
        new = dict((k, self._publisher_threads[k] if k in self._publisher_threads else self._publisher_thread(k)) for k in publishers)
        for k, v in self._publisher_threads.iteritems():
            v.cancel()
            v.addErrback(lambda fail: fail.trap(defer.CancelledError))
        self._publisher_threads = new

if __name__ == '__main__':
    nh = NodeHandle('testnode')
    
    from geometry_msgs.msg import PointStamped
    def cb(msg):
        print msg
    nh.subscribe('point', PointStamped, cb)
    
    reactor.run()
