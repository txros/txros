from __future__ import division

import StringIO

from twisted.internet import defer, reactor, endpoints

from txros import util, tcpros


class ServiceError(Exception):
    def __init__(self, message):
        self._message = message
    def __str__(self):
        return 'ServiceError(%r)' % (self._message,)

class ServiceClient(object):
    def __init__(self, node_handle, name, service_type):
        self._node_handle = node_handle
        self._name = self._node_handle.resolve_name(name)
        self._type = service_type
    
    @util.cancellableInlineCallbacks
    def __call__(self, req):
        serviceUrl = yield self._node_handle._proxy.lookupService(self._name)
        
        protocol, rest = serviceUrl.split('://', 1)
        host, port_str = rest.rsplit(':', 1)
        port = int(port_str)
        
        assert protocol == 'rosrpc'
        
        conn = yield endpoints.TCP4ClientEndpoint(reactor, host, port).connect(util.AutoServerFactory(lambda addr: tcpros.Protocol()))
        try:
            conn.sendString(tcpros.serialize_dict(dict(
                callerid=self._node_handle._name,
                service=self._name,
                md5sum=self._type._md5sum,
                type=self._type._type,
            )))
            
            header = tcpros.deserialize_dict((yield conn.receiveString()))
            # XXX do something with header
            
            # request could be sent before header is received to reduce latency...
            x = StringIO.StringIO()
            self._type._request_class.serialize(req, x)
            data = x.getvalue()
            conn.sendString(data)
            
            result = ord((yield conn.receiveByte()))
            data = yield conn.receiveString()
            if result: # success
                defer.returnValue(self._type._response_class().deserialize(data))
            else:
                raise ServiceError(data)
        finally:
            conn.transport.loseConnection()
