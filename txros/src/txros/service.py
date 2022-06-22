from __future__ import annotations

import traceback
from io import StringIO
from typing import TYPE_CHECKING, Callable
import genpy

from twisted.internet import defer, error

from . import util, tcpros

if TYPE_CHECKING:
    from .nodehandle import NodeHandle
    from .serviceclient import ServiceType


class Service:
    """
    A service in the txROS suite. Handles incoming requests through a user-supplied
    callback function, which is expected to return a response message.
    """
    def __init__(
        self,
        node_handle: NodeHandle,
        name: str,
        service_type: ServiceType,
        callback: Callable[[genpy.Message], defer.Deferred],
    ):
        """
        Args:
            node_handle (NodeHandle): The node handle to use in conjunction with
                the service.
            name (str): The name to use for the service.
            service_type (ServiceType): A ROS service class to use with the service.
                The callback method used by the class will receive the request
                class associated with the service, and is expected to return the
                response class associated with this class.
            callback (Callable[[genpy.Message], defer.Deferred]): The callback
                to process all incoming service requests. The callback should return
                the service response class through a Deferred object.
        """
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
            assert ("service", self._name) not in self._node_handle._tcpros_handlers
            self._node_handle._tcpros_handlers[
                "service", self._name
            ] = self._handle_tcpros_conn
            try:
                while True:
                    try:
                        yield self._node_handle._master_proxy.registerService(
                            self._name,
                            self._node_handle._tcpros_server_uri,
                            self._node_handle._xmlrpc_server_uri,
                        )
                    except Exception:
                        traceback.print_exc()
                    else:
                        break
                yield defer.Deferred()  # wait for cancellation
            finally:
                try:
                    yield self._node_handle._master_proxy.unregisterService(
                        self._name, self._node_handle._tcpros_server_uri
                    )
                except Exception:
                    traceback.print_exc()
                del self._node_handle._tcpros_handlers["service", self._name]
        finally:
            self._shutdown_finished.callback(None)

    def shutdown(self) -> None:
        """
        Shuts the service down. Cancels all operations currently scheduled to be
        completed by the service.
        """
        self._node_handle._shutdown_callbacks.discard(self.shutdown)
        self._think_thread.cancel()
        self._think_thread.addErrback(lambda fail: fail.trap(defer.CancelledError))
        return util.branch_deferred(self._shutdown_finished)

    @util.cancellableInlineCallbacks
    def _handle_tcpros_conn(self, headers, conn):
        try:
            # check headers

            conn.sendString(
                tcpros.serialize_dict(
                    dict(
                        callerid=self._node_handle._name,
                        type=self._type._type,
                        md5sum=self._type._md5sum,
                        request_type=self._type._request_class._type,
                        response_type=self._type._response_class._type,
                    )
                )
            )

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
