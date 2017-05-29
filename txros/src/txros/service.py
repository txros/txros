from __future__ import division

import traceback
import StringIO

from twisted.internet import defer, error

from txros import util, tcpros


class Service(object):

    def __init__(self, node_handle, name, service_type, callback):
        self._node_handle = node_handle
        self._name = self._node_handle.resolve_name(name)
        self._type = service_type
        self._callback = callback

        self._shutdown_finished = defer.Deferred()
        self._think_thread = self._think()
        self._node_handle._shutdown_callbacks.add(self.shutdown)

    @util.cancellableInlineCallbacks
    def _think(self):
        try:
            assert ('service', self._name) not in self._node_handle._tcpros_handlers
            self._node_handle._tcpros_handlers['service', self._name] = self._handle_tcpros_conn
            try:
                while True:
                    try:
                        yield self._node_handle._master_proxy.registerService(
                            self._name, self._node_handle._tcpros_server_uri, self._node_handle._xmlrpc_server_uri)
                    except Exception:
                        traceback.print_exc()
                    else:
                        break
                yield defer.Deferred()  # wait for cancellation
            finally:
                try:
                    yield self._node_handle._master_proxy.unregisterService(
                        self._name, self._node_handle._tcpros_server_uri)
                except Exception:
                    traceback.print_exc()
                del self._node_handle._tcpros_handlers['service', self._name]
        finally:
            self._shutdown_finished.callback(None)

    def shutdown(self):
        self._node_handle._shutdown_callbacks.discard(self.shutdown)
        self._think_thread.cancel()
        self._think_thread.addErrback(lambda fail: fail.trap(defer.CancelledError))
        return util.branch_deferred(self._shutdown_finished)

    @util.cancellableInlineCallbacks
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
                string = yield conn.receiveString()
                req = self._type._request_class().deserialize(string)
                try:
                    resp = yield self._callback(req)
                except Exception as e:
                    traceback.print_exc()
                    conn.sendByte(chr(0))
                    conn.sendString(str(e))
                else:
                    conn.sendByte(chr(1))
                    x = StringIO.StringIO()
                    self._type._response_class.serialize(resp, x)
                    conn.sendString(x.getvalue())
        except (error.ConnectionDone, error.ConnectionLost):
            pass
        finally:
            conn.transport.loseConnection()
