from __future__ import division

import traceback
import StringIO

from twisted.internet import error

from txros import util, tcpros


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
