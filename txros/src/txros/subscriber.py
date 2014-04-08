from __future__ import division

import traceback

from twisted.web import xmlrpc
from twisted.internet import defer, reactor, endpoints, error

from txros import rosxmlrpc, tcpros, util


class Subscriber(object):
    def __init__(self, node_handle, name, message_type, callback=lambda message: None):
        self._node_handle = node_handle
        self._name = self._node_handle.resolve_name(name)
        self._type = message_type
        self._callback = callback
        
        self._publisher_threads = {}
        self._last_message = None
        self._message_dfs = []
        
        self._shutdown_finished = defer.Deferred()
        self._think_thread = self._think()
        self._node_handle._shutdown_callbacks.add(self.shutdown)
    
    @util.cancellableInlineCallbacks
    def _think(self):
        try:
            yield self._node_handle._get_ready()
            assert ('publisherUpdate', self._name) not in self._node_handle._xmlrpc_handlers
            self._node_handle._xmlrpc_handlers['publisherUpdate', self._name] = self._handle_publisher_list
            try:
                while True:
                    try:
                        publishers = yield self._node_handle._master_proxy.registerSubscriber(self._name, self._type._type, self._node_handle._xmlrpc_server_uri)
                    except Exception:
                        traceback.print_exc()
                    else:
                        break
                self._handle_publisher_list(publishers)
                yield defer.Deferred() # wait for cancellation
            finally:
                try:
                    yield self._node_handle._master_proxy.unregisterSubscriber(self._name, self._node_handle._xmlrpc_server_uri)
                except Exception:
                    traceback.print_exc()
                del self._node_handle._xmlrpc_handlers['publisherUpdate', self._name]
        finally:
            self._shutdown_finished.callback(None)
    
    def shutdown(self):
        self._node_handle._shutdown_callbacks.discard(self.shutdown)
        self._think_thread.cancel()
        self._think_thread.addErrback(lambda fail: fail.trap(defer.CancelledError))
        return util.branch_deferred(self._shutdown_finished)
    
    def get_last_message(self):
        return self._last_message
    
    def get_next_message(self):
        res = defer.Deferred()
        self._message_dfs.append(res)
        return res
    
    @util.cancellableInlineCallbacks
    def _publisher_thread(self, url):
        while True:
            try:
                proxy = rosxmlrpc.Proxy(xmlrpc.Proxy(url), self._node_handle._name)
                value = yield proxy.requestTopic(self._name, [['TCPROS']])
                
                protocol, host, port = value
                conn = yield endpoints.TCP4ClientEndpoint(reactor, host, port).connect(util.AutoServerFactory(lambda addr: tcpros.Protocol()))
                try:
                    conn.sendString(tcpros.serialize_dict(dict(
                        message_definition=self._type._full_text,
                        callerid=self._node_handle._name,
                        topic=self._name,
                        md5sum=self._type._md5sum,
                        type=self._type._type,
                    )))
                    header = tcpros.deserialize_dict((yield conn.receiveString()))
                    # XXX do something with header
                    while True:
                        data = yield conn.receiveString()
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
        
        return 1, 'success', False
