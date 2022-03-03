from __future__ import division

import traceback
import StringIO

from twisted.internet import defer, error

from txros import util, tcpros


class Publisher:
    def __init__(self, node_handle, name, message_type, latching=False):
        self._node_handle = node_handle
        self._name = self._node_handle.resolve_name(name)
        self._type = message_type
        self._latching = latching

        self._last_message_data = None
        self._connections = {}

        self._shutdown_finished = defer.Deferred()
        self._think_thread = self._think()
        self._node_handle._shutdown_callbacks.add(self.shutdown)

    @util.cancellableInlineCallbacks
    def _think(self):
        try:
            assert ("topic", self._name) not in self._node_handle._tcpros_handlers
            self._node_handle._tcpros_handlers[
                "topic", self._name
            ] = self._handle_tcpros_conn
            assert (
                "requestTopic",
                self._name,
            ) not in self._node_handle._xmlrpc_handlers
            self._node_handle._xmlrpc_handlers[
                "requestTopic", self._name
            ] = self._handle_requestTopic
            try:
                while True:
                    try:
                        yield self._node_handle._master_proxy.registerPublisher(
                            self._name,
                            self._type._type,
                            self._node_handle._xmlrpc_server_uri,
                        )
                    except Exception:
                        traceback.print_exc()
                    else:
                        break
                yield defer.Deferred()  # wait for cancellation
            finally:
                try:
                    yield self._node_handle._master_proxy.unregisterPublisher(
                        self._name, self._node_handle._xmlrpc_server_uri
                    )
                except Exception:
                    traceback.print_exc()
                del self._node_handle._tcpros_handlers["topic", self._name]
                del self._node_handle._xmlrpc_handlers["requestTopic", self._name]
        finally:
            self._shutdown_finished.callback(None)

    def shutdown(self):
        self._node_handle._shutdown_callbacks.discard(self.shutdown)
        self._think_thread.cancel()
        self._think_thread.addErrback(lambda fail: fail.trap(defer.CancelledError))
        return util.branch_deferred(self._shutdown_finished)

    def _handle_requestTopic(self, protocols):
        return (
            1,
            "ready on " + self._node_handle._tcpros_server_uri,
            [
                "TCPROS",
                self._node_handle._tcpros_server_addr[0],
                self._node_handle._tcpros_server_addr[1],
            ],
        )

    @util.cancellableInlineCallbacks
    def _handle_tcpros_conn(self, headers, conn):
        try:
            # XXX handle headers

            conn.sendString(
                tcpros.serialize_dict(
                    dict(
                        callerid=self._node_handle._name,
                        type=self._type._type,
                        md5sum=self._type._md5sum,
                        latching="1" if self._latching else "0",
                    )
                )
            )

            if self._latching and self._last_message_data is not None:
                conn.sendString(self._last_message_data)

            self._connections[conn] = headers["callerid"]
            try:
                while True:
                    x = yield conn.receiveString()
                    print(repr(x))
            finally:
                del self._connections[conn]
        except (error.ConnectionDone, error.ConnectionLost):
            pass
        finally:
            conn.transport.loseConnection()

    def publish(self, msg):
        x = StringIO.StringIO()
        self._type.serialize(msg, x)
        data = x.getvalue()

        for conn in self._connections:
            conn.sendString(data)

        if self._latching:
            self._last_message_data = data

    def get_connections(self):
        return list(self._connections.values())
