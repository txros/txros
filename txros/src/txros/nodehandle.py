from __future__ import division

import os
import traceback

from twisted.web import server, xmlrpc
from twisted.internet import reactor

from roscpp.srv import GetLoggers, GetLoggersResponse, SetLoggerLevel, SetLoggerLevelResponse

from txros import util, tcpros, publisher, rosxmlrpc, service, subscriber


class XMLRPCSlave(xmlrpc.XMLRPC):
    def __init__(self, handlers):
        xmlrpc.XMLRPC.__init__(self)
        self._handlers = handlers
    
    def xmlrpc_publisherUpdate(self, caller_id, topic, publishers):
        return self._handlers['publisherUpdate', topic](publishers)
    
    def xmlrpc_requestTopic(self, caller_id, topic, protocols):
        return self._handlers['requestTopic', topic](protocols)

class NodeHandle(object):
    def __init__(self, name):
        self._ns = ''
        self._name = self._ns + '/' + name
        
        self._shutdown_callbacks = []
        reactor.addSystemEventTrigger('before', 'shutdown', self.shutdown)
        
        self._proxy = rosxmlrpc.Proxy(xmlrpc.Proxy(os.environ['ROS_MASTER_URI']), self._name)
        self._addr = '127.0.0.1' # XXX
        
        self._xmlrpc_handlers = {}
        self._xmlrpc_server = reactor.listenTCP(0, server.Site(XMLRPCSlave(self._xmlrpc_handlers)))
        self._xmlrpc_server_uri = 'http://%s:%i/' % (self._addr, self._xmlrpc_server.getHost().port)
        
        self._tcpros_handlers = {}
        self._tcpros_server = reactor.listenTCP(0, util.AutoServerFactory(tcpros.Server, self._tcpros_handlers))
        self._tcpros_server_uri = 'rosrpc://%s:%i' % (self._addr, self._tcpros_server.getHost().port)
        self._tcpros_server_addr = self._addr, self._tcpros_server.getHost().port
        
        self.advertise_service('~get_loggers', GetLoggers, lambda req: GetLoggersResponse())
        self.advertise_service('~set_logger_level', SetLoggerLevel, lambda req: SetLoggerLevelResponse())
    
    @util.inlineCallbacks
    def shutdown(self):
        while self._shutdown_callbacks:
            self._shutdown_callbacks, old = [], self._shutdown_callbacks
            old_dfs = [func() for func in old]
            for df in old_dfs:
                try:
                    yield df
                except:
                    traceback.print_exc()
    
    def resolve_name(self, name):
        if name.startswith('/'):
            return name
        elif name.startswith('~'):
            return self._name + '/' + name[1:]
        else:
            return self._ns + '/' + name
    
    
    def advertise_service(self, *args, **kwargs):
        return service.Service(self, *args, **kwargs)
    
    def subscribe(self, *args, **kwargs):
        return subscriber.Subscriber(self, *args, **kwargs)
    
    def advertise(self, *args, **kwargs):
        return publisher.Publisher(self, *args, **kwargs)
    
    
    def get_param(self, key):
        return self._proxy.getParam(key)
    
    def has_param(self, key):
        return self._proxy.hasParam(key)
    
    def delete_param(self, key):
        return self._proxy.deleteParam(key)
    
    def set_param(self, key, value):
        return self._proxy.setParam(key, value)
    
    def search_param(self, key):
        return self._proxy.searchParam(key)
    
    def get_param_names(self):
        return self._proxy.getParamNames()
