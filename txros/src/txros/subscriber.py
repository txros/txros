from __future__ import division

import traceback

from twisted.web import xmlrpc
from twisted.internet import defer, reactor, endpoints, error

from txros import rosxmlrpc, tcpros, util


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
                proxy = rosxmlrpc.Proxy(xmlrpc.Proxy(url), self._node_handle._name)
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
