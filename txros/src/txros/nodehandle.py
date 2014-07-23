from __future__ import division

import os
import random
import socket
import sys
import traceback

from twisted.web import server, xmlrpc
from twisted.internet import defer, error, reactor

import genpy
from roscpp.srv import GetLoggers, GetLoggersResponse, SetLoggerLevel, SetLoggerLevelResponse
from rosgraph_msgs.msg import Clock

from txros import util, tcpros, publisher, rosxmlrpc, service, serviceclient, subscriber


class XMLRPCSlave(xmlrpc.XMLRPC):
    def __init__(self, node_handle):
        xmlrpc.XMLRPC.__init__(self)
        self._node_handle = node_handle
    
    def __getattr__(self, attr):
        if attr.startswith('xmlrpc_'):
            print attr, 'not implemented'
        raise AttributeError(attr)
    
    def xmlrpc_getBusStats(self, caller_id):
        return 1, 'success', [[], [], []] # XXX
    
    def xmlrpc_getBusInfo(self, caller_id):
        # list of (connectionId, destinationId, direction, transport, topic, connected)
        return 1, 'success', [] # XXX
    
    def xmlrpc_getMasterUri(self, caller_id):
        return 1, 'success', self._node_handle._master_uri
    
    @util.cancellableInlineCallbacks
    def xmlrpc_shutdown(self, caller_id, msg=''):
        print 'Shutdown requested. Reason:', repr(msg)
        yield self._node_handle.shutdown()
        # XXX needs to terminate process somehow
        defer.returnValue((1, 'success', False))
    
    def xmlrpc_getPid(self, caller_id):
        return 1, 'success', os.getpid()
    
    def xmlrpc_getSubscriptions(self, caller_id):
        return 1, 'success', [] # XXX
    
    def xmlrpc_getPublications(self, caller_id):
        return 1, 'success', [] # XXX
    
    def xmlrpc_paramUpdate(self, parameter_key, parameter_value):
        return 1, 'success', False # XXX
    
    def xmlrpc_publisherUpdate(self, caller_id, topic, publishers):
        return self._node_handle._xmlrpc_handlers.get(('publisherUpdate', topic),
            lambda _: (1, 'success', True))(publishers)
    
    def xmlrpc_requestTopic(self, caller_id, topic, protocols):
        return self._node_handle._xmlrpc_handlers.get(('requestTopic', topic),
            lambda _: (-1, "Not a publisher of [%s]" % topic, []))(protocols)

class NodeHandle(object):
    @classmethod
    def from_argv_with_remaining(cls, default_name, argv=sys.argv, anonymous=False):
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
        
        return cls(ns=ns, name=name, addr=addr, master_uri=master_uri, remappings=remappings), res
    
    @classmethod
    def from_argv(cls, *args, **kwargs):
        return cls.from_argv_with_remaining(*args, **kwargs)[0]
    
    def __init__(self, ns, name, addr, master_uri, remappings):
        if ns: assert ns[0] == '/'
        assert not ns.endswith('/')
        self._ns = ns # valid values: '', '/a', '/a/b'
        
        assert '/' not in name
        self._name = self._ns + '/' + name
        
        self._shutdown_callbacks = set()
        reactor.addSystemEventTrigger('before', 'shutdown', self.shutdown)
        
        self._addr = addr
        self._master_uri = master_uri
        self._remappings = remappings
        
        self._master_proxy = rosxmlrpc.Proxy(xmlrpc.Proxy(self._master_uri), self._name)
        self._is_running = True
        
        self._xmlrpc_handlers = {}
        self._xmlrpc_server = reactor.listenTCP(0, server.Site(XMLRPCSlave(self)))
        self._xmlrpc_server_uri = 'http://%s:%i/' % (self._addr, self._xmlrpc_server.getHost().port)
        
        self._tcpros_handlers = {}
        @util.cancellableInlineCallbacks
        def _handle_tcpros_conn(conn):
            try:
                header = tcpros.deserialize_dict((yield conn.receiveString()))
                def default(header, conn):
                    conn.sendString(tcpros.serialize_dict(dict(error='unhandled connection')))
                    conn.transport.loseConnection()
                if 'service' in header:
                    self._tcpros_handlers.get(('service', header['service']), default)(header, conn)
                elif 'topic' in header:
                    self._tcpros_handlers.get(('topic', header['topic']), default)(header, conn)
                else:
                    conn.sendString(tcpros.serialize_dict(dict(error='no topic or service name detected')))
                    conn.transport.loseConnection()
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
        
        self._ready_flag = defer.Deferred()
        self._node_think()
        
        self.advertise_service('~get_loggers', GetLoggers, lambda req: GetLoggersResponse())
        self.advertise_service('~set_logger_level', SetLoggerLevel, lambda req: SetLoggerLevelResponse())
    
    def _get_ready(self):
        return util.branch_deferred(self._ready_flag)
    
    @util.cancellableInlineCallbacks
    def _node_think(self):
        while True:
            try:
                other_node_uri = yield self._master_proxy.lookupNode(self._name)
            except rosxmlrpc.Error:
                break # assume that error means unknown node
            except Exception:
                traceback.print_exc()
                yield util.sleep(1)
            else:
                other_node_proxy = rosxmlrpc.Proxy(xmlrpc.Proxy(other_node_uri), self._name)
                try:
                    yield util.wrap_timeout(other_node_proxy.shutdown('new node registered with same name'), 3)
                except error.ConnectionRefusedError:
                    pass
                except Exception:
                    traceback.print_exc()
                break
        
        try:
            self._use_sim_time = yield self.get_param('/use_sim_time')
        except rosxmlrpc.Error:
            self._use_sim_time = False
        if self._use_sim_time:
            self.subscribe('/clock', Clock, self._got_clock)
        
        for k, v in self._remappings.iteritems():
            if k.startswith('_') and not k.startswith('__'):
                # XXX should attempt to parse v as at least int
                yield self.set_param(self.resolve_name('~' + k[1:]), v)
        
        self._ready_flag.callback(None)
    
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
    
    def _got_clock(self, msg):
        self._sim_time = msg.clock
    
    def get_time(self):
        if hasattr(self, '_use_sim_time') and self._use_sim_time: # XXX may not be set yet
            return self._sim_time # XXX may not be set yet
        else:
            return genpy.Time(reactor.seconds())
    
    def resolve_name_without_remapping(self, name):
        if name.startswith('/'):
            return name
        elif name.startswith('~'):
            return self._name + '/' + name[1:]
        else:
            return self._ns + '/' + name
    def resolve_name(self, name):
        name = self.resolve_name_without_remapping(name)
        for before_unresolved, after_unresolved in self._remappings.iteritems():
            before = self.resolve_name_without_remapping(before_unresolved)
            after = self.resolve_name_without_remapping(after_unresolved)
            if name == before or name.startswith(before + '/'):
                return after + name[len(before):]
        return name
    
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
        return self._master_proxy.getParam(key)
    
    def has_param(self, key):
        return self._master_proxy.hasParam(key)
    
    def delete_param(self, key):
        return self._master_proxy.deleteParam(key)
    
    def set_param(self, key, value):
        return self._master_proxy.setParam(key, value)
    
    def search_param(self, key):
        return self._master_proxy.searchParam(key)
    
    def get_param_names(self):
        return self._master_proxy.getParamNames()
