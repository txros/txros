from __future__ import division

import traceback
import StringIO

from twisted.internet import error

from txros import util, tcpros


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
