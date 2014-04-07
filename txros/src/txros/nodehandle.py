from __future__ import division

import os
import traceback
import StringIO

from twisted.web import server, xmlrpc
from twisted.internet import defer, reactor, endpoints, error

from roscpp.srv import GetLoggers, GetLoggersResponse, SetLoggerLevel, SetLoggerLevelResponse

from txros import util, tcpros


class XMLRPCSlave(xmlrpc.XMLRPC):
    def __init__(self, handlers):
        xmlrpc.XMLRPC.__init__(self)
        self._handlers = handlers
    
    def xmlrpc_publisherUpdate(self, caller_id, topic, publishers):
        return self._handlers['publisherUpdate', topic](publishers)
    
    def xmlrpc_requestTopic(self, caller_id, topic, protocols):
        return self._handlers['requestTopic', topic](protocols)

class ROSXMLRPCError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
    def __str__(self):
        return 'ROSXMLRPCError' + repr((self.code, self.message))
class ROSXMLRPCProxy(object):
    def __init__(self, proxy, caller_id):
        self._proxy = proxy
        self._caller_id = caller_id
    
    def __getattr__(self, name):
        @util.inlineCallbacks
        def _(*args):
            statusCode, statusMessage, value = yield self._proxy.callRemote(name, self._caller_id, *args)
            if statusCode == 1: # SUCCESS
                defer.returnValue(value)
            else:
                raise ROSXMLRPCError(statusCode, statusMessage)
        return _

class NodeHandle(object):
    def __init__(self, name):
        self._ns = ''
        self._name = self._ns + '/' + name
        
        self._shutdown_callbacks = []
        reactor.addSystemEventTrigger('before', 'shutdown', self.shutdown)
        
        self._proxy = ROSXMLRPCProxy(xmlrpc.Proxy(os.environ['ROS_MASTER_URI']), self._name)
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
        return Service(self, *args, **kwargs)
    
    def subscribe(self, *args, **kwargs):
        return Subscriber(self, *args, **kwargs)
    
    def advertise(self, *args, **kwargs):
        return Publisher(self, *args, **kwargs)
    
    
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

class Service(object):
    def __init__(self, node_handle, name, service_type, callback):
        self._node_handle = node_handle
        self._name = node_handle.resolve_name(name)
        
        self._type = service_type
        self._callback = callback
        
        assert ('service', self._name) not in node_handle._tcpros_handlers
        node_handle._tcpros_handlers['service', self._name] = self._handle_tcpros_conn
        self._think_thread = self._think()
        self._node_handle._shutdown_callbacks.append(self.shutdown)
    
    @util.inlineCallbacks
    def _think(self):
        while True:
            try:
                yield self._node_handle._proxy.registerService(self._name, self._node_handle._tcpros_server_uri, self._node_handle._xmlrpc_server_uri)
            except:
                traceback.print_exc()
            else:
                break
    
    @util.inlineCallbacks
    def shutdown(self):
        self._think_thread.cancel()
        yield self._node_handle._proxy.unregisterService(self._name, self._node_handle._tcpros_server_uri)
        del self._node_handle._tcpros_handlers['service', self._name]
    
    @util.inlineCallbacks
    def _handle_tcpros_conn(self, headers, conn):
        try:
            # check headers
            
            conn.sendString(tcpros.serialize_dict(dict(
                callerid=self._node_handle._name,
                type=self._type._type,
                md5sum=self._type._md5sum,
                request_type=self._type._request_class._type,
                response_type=self._type._response_class._type,
            )))
            
            while True:
                string = yield conn.queue.get_next()
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
        except (error.ConnectionDone, error.ConnectionLost):
            pass
        finally:
            conn.transport.loseConnection()

class Subscriber(object):
    def __init__(self, node_handle, name, message_type, callback=lambda message: None):
        self._node_handle = node_handle
        self._name = node_handle.resolve_name(name)
        
        self._type = message_type
        self._callback = callback
        
        self._publisher_threads = {}
        
        self._last_message = None
        self._message_dfs = []
        
        assert ('publisherUpdate', self._name) not in node_handle._xmlrpc_handlers
        node_handle._xmlrpc_handlers['publisherUpdate', self._name] = self._handle_publisher_list
        self._think_thread = self._think()
        self._node_handle._shutdown_callbacks.append(self.shutdown)
    
    @util.inlineCallbacks
    def _think(self):
        while True:
            try:
                publishers = yield self._node_handle._proxy.registerSubscriber(self._name, self._type._type, self._node_handle._xmlrpc_server_uri)
            except:
                traceback.print_exc()
            else:
                break
        self._handle_publisher_list(publishers)
    
    @util.inlineCallbacks
    def shutdown(self):
        self._think_thread.cancel()
        yield self._node_handle._proxy.unregisterSubscriber(self._name, self._node_handle._xmlrpc_server_uri)
        del self._node_handle._xmlrpc_handlers['publisherUpdate', self._name]
    
    def get_last_message(self):
        return self._last_message
    
    def get_next_message(self):
        res = defer.Deferred()
        self._message_dfs.append(res)
        return res
    
    @util.inlineCallbacks
    def _publisher_thread(self, url):
        while True:
            try:
                proxy = ROSXMLRPCProxy(xmlrpc.Proxy(url), self._node_handle._name)
                value = yield proxy.requestTopic(self._name, [['TCPROS']])
                
                protocol, host, port = value
                conn = yield endpoints.TCP4ClientEndpoint(reactor, host, port).connect(util.AutoServerFactory(tcpros.Client))
                try:
                    conn.sendString(tcpros.serialize_dict(dict(
                        message_definition=self._type._full_text,
                        callerid=self._node_handle._name,
                        topic=self._name,
                        md5sum=self._type._md5sum,
                        type=self._type._type,
                    )))
                    header = tcpros.deserialize_dict((yield conn.queue.get_next()))
                    # XXX do something with header
                    while True:
                        data = yield conn.queue.get_next()
                        msg = self._type().deserialize(data)
                        self._last_message = msg
                        self._callback(msg)
                        old, self._message_dfs = self._message_dfs, []
                        for df in old:
                            df.callback(msg)
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

class Publisher(object):
    def __init__(self, node_handle, name, message_type, latching=False):
        self._node_handle = node_handle
        self._name = node_handle.resolve_name(name)
        
        self._type = message_type
        self._latching = latching
        
        self._last_message_data = None
        self._connections = set()
        
        assert ('topic', self._name) not in node_handle._tcpros_handlers
        node_handle._tcpros_handlers['topic', self._name] = self._handle_tcpros_conn
        assert ('requestTopic', self._name) not in node_handle._xmlrpc_handlers
        node_handle._xmlrpc_handlers['requestTopic', self._name] = self._handle_requestTopic
        self._think_thread = self._think()
        self._node_handle._shutdown_callbacks.append(self.shutdown)
    
    @util.inlineCallbacks
    def _think(self):
        while True:
            try:
                yield self._node_handle._proxy.registerPublisher(self._name, self._type._type, self._node_handle._xmlrpc_server_uri)
            except:
                traceback.print_exc()
            else:
                break
    
    @util.inlineCallbacks
    def shutdown(self):
        self._think_thread.cancel()
        yield self._node_handle._proxy.unregisterPublisher(self._name, self._node_handle._xmlrpc_server_uri)
    
    def _handle_requestTopic(self, protocols):
        return 1, 'ready on ' + self._node_handle._tcpros_server_uri, ['TCPROS', self._node_handle._tcpros_server_addr[0], self._node_handle._tcpros_server_addr[1]]
    
    @util.inlineCallbacks
    def _handle_tcpros_conn(self, headers, conn):
        try:
            # XXX handle headers
            
            conn.sendString(tcpros.serialize_dict(dict(
                callerid=self._node_handle._name,
                type=self._type._type,
                md5sum=self._type._md5sum,
                latching='1' if self._latching else '0',
            )))
            
            if self._latching and self._last_message_data is not None:
                conn.sendString(self._last_message_data)
            
            self._connections.add(conn)
            try:
                while True:
                    x = yield conn.queue.get_next()
                    print repr(x)
            finally:
                self._connections.remove(conn)
        except (error.ConnectionDone, error.ConnectionLost):
            pass
        finally:
            conn.transport.loseConnection()
    
    def publish(self, msg):
        x = StringIO.StringIO()
        msg.serialize(x)
        data = x.getvalue()
        
        for conn in self._connections:
            conn.sendString(data)
        
        if self._latching:
            self._last_message_data = data
