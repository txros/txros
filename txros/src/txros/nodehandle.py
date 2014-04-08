from __future__ import division

import os
import random
import socket
import sys
import traceback

from twisted.web import server, xmlrpc
from twisted.internet import reactor

from roscpp.srv import GetLoggers, GetLoggersResponse, SetLoggerLevel, SetLoggerLevelResponse

from txros import util, tcpros, publisher, rosxmlrpc, service, serviceclient, subscriber


class XMLRPCSlave(xmlrpc.XMLRPC):
    def __init__(self, handlers):
        xmlrpc.XMLRPC.__init__(self)
        self._handlers = handlers
    
    def xmlrpc_publisherUpdate(self, caller_id, topic, publishers):
        return self._handlers['publisherUpdate', topic](publishers)
    
    def xmlrpc_requestTopic(self, caller_id, topic, protocols):
        return self._handlers['requestTopic', topic](protocols)

class NodeHandle(object):
    @classmethod
    def from_argv(cls, default_name, argv=sys.argv, anonymous=False):
        res = [argv[0]]
        remappings = {}
        for arg in argv[1:]:
            if ':=' in arg:
                before, after = arg.split(':=')
                assert before not in remappings
                remappings[before] = after
            else:
                res.append(arg)
        
        name = default_name
        if anonymous:
            name = name + '_' + '%016x' % (random.randrange(2**64),)
        if '__name' in remappings:
            name = remappings['__name']
        
        log = None # what does this default to?
        if '__log' in remappings:
            log = remappings['__log']
        
        addr = socket.gethostname() # could be a bit smarter
        if 'ROS_IP' in os.environ:
            addr = os.environ['ROS_IP']
        if 'ROS_HOSTNAME' in os.environ:
            addr = os.environ['ROS_HOSTNAME']
        if '__ip' in remappings:
            addr = remappings['__ip']
        if '__hostname' in remappings:
            addr = remappings['__hostname']
        
        master_uri = None
        if 'ROS_MASTER_URI' in os.environ:
            master_uri = os.environ['ROS_MASTER_URI']
        if '__master' in remappings:
            master_uri = remappings['__master']
        if master_uri is None:
            raise ValueError('either ROS_MASTER_URI variable or __master argument has to be provided')
        
        ns = ''
        if 'ROS_NAMESPACE' in os.environ:
            ns = os.environ['ROS_NAMESPACE']
        if '__ns' in remappings:
            ns = remappings['__ns']
        
        return cls(ns=ns, name=name, addr=addr, master_uri=master_uri, remappings=remappings)
    
    def __init__(self, ns, name, addr, master_uri, remappings):
        if ns: assert ns[0] == '/'
        assert not ns.endswith('/')
        self._ns = ns # valid values: '', '/a', '/a/b'
        
        assert '/' not in name
        self._name = self._ns + '/' + name
        
        self._shutdown_callbacks = set()
        reactor.addSystemEventTrigger('before', 'shutdown', self.shutdown)
        
        self._addr = addr
        self._proxy = rosxmlrpc.Proxy(xmlrpc.Proxy(master_uri), self._name)
        self._remappings = remappings
        self._is_running = True
        
        self._xmlrpc_handlers = {}
        self._xmlrpc_server = reactor.listenTCP(0, server.Site(XMLRPCSlave(self._xmlrpc_handlers)))
        self._xmlrpc_server_uri = 'http://%s:%i/' % (self._addr, self._xmlrpc_server.getHost().port)
        
        self._tcpros_handlers = {}
        @util.cancellableInlineCallbacks
        def _handle_tcpros_conn(conn):
            try:
                header = tcpros.deserialize_dict((yield conn.receiveString()))
                if 'service' in header:
                    self._tcpros_handlers['service', header['service']](header, conn)
                elif 'topic' in header:
                    self._tcpros_handlers['topic', header['topic']](header, conn)
                else:
                    assert False
            except:
                conn.transport.loseConnection()
                raise
        def _make_tcpros_protocol(addr):
            conn = tcpros.Protocol()
            _handle_tcpros_conn(conn)
            return conn
        self._tcpros_server = reactor.listenTCP(0, util.AutoServerFactory(_make_tcpros_protocol))
        self._tcpros_server_uri = 'rosrpc://%s:%i' % (self._addr, self._tcpros_server.getHost().port)
        self._tcpros_server_addr = self._addr, self._tcpros_server.getHost().port
        
        self.advertise_service('~get_loggers', GetLoggers, lambda req: GetLoggersResponse())
        self.advertise_service('~set_logger_level', SetLoggerLevel, lambda req: SetLoggerLevelResponse())
    
    def shutdown(self):
        if not hasattr(self, '_shutdown_thread'):
            self._shutdown_thread = self._real_shutdown()
        return util.branch_deferred(self._shutdown_thread)
    @util.cancellableInlineCallbacks
    def _real_shutdown(self):
        self._is_running = False
        while self._shutdown_callbacks:
            self._shutdown_callbacks, old = set(), self._shutdown_callbacks
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
    
    def is_running(self):
        return self._is_running
    def is_shutdown(self):
        return not self._is_running
    
    def advertise_service(self, *args, **kwargs):
        return service.Service(self, *args, **kwargs)
    
    def get_service_client(self, *args, **kwargs):
        return serviceclient.ServiceClient(self, *args, **kwargs)
    
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
