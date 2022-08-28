from __future__ import annotations

import asyncio
import traceback
from io import BytesIO
from typing import TYPE_CHECKING, Callable, Awaitable, TypeVar, Generic
import genpy

from . import util, tcpros, types

if TYPE_CHECKING:
    from .nodehandle import NodeHandle
    from .serviceclient import ServiceType
    from .tcpros import Protocol

Request = TypeVar('Request', bound = types.Message)
Reply = TypeVar('Reply', bound = types.Message)


class Service(Generic[Request, Reply]):
    """
    A service in the txROS suite. Handles incoming requests through a user-supplied
    callback function, which is expected to return a response message.
    """
    def __init__(
        self,
        node_handle: NodeHandle,
        name: str,
        service_type: types.ServiceMessage[Request, Reply],
        callback: Callable[[Request], Awaitable[Reply]],
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

        self._node_handle.shutdown_callbacks.add(self.shutdown)

    async def setup(self) -> None:
        assert ("service", self._name) not in self._node_handle.tcpros_handlers
        self._node_handle.tcpros_handlers[
            "service", self._name
        ] = self._handle_tcpros_conn
        await self._node_handle.master_proxy.register_service(
            self._name,
            self._node_handle._tcpros_server_uri,
            self._node_handle.xmlrpc_server_uri,
        )
        print(f"Service {self._name} is now accepting requests...")

    async def shutdown(self) -> None:
        """
        Shuts the service down. Cancels all operations currently scheduled to be
        completed by the service.
        """
        try:
            await self._node_handle.master_proxy.unregister_service(
                self._name, self._node_handle._tcpros_server_uri
            )
        except Exception:
            traceback.print_exc()
        del self._node_handle.tcpros_handlers["service", self._name]

        self._node_handle.shutdown_callbacks.discard(self.shutdown)

    async def _handle_tcpros_conn(self, _, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            # check headers

            tcpros.send_string(
                tcpros.serialize_dict(
                    dict(
                        callerid=self._node_handle._name,
                        type=self._type._type,
                        md5sum=self._type._md5sum,
                        request_type=self._type._request_class._type,
                        response_type=self._type._response_class._type,
                    )
                ),
                writer
            )

            while True:
                string = await tcpros.receive_string(reader)
                req = self._type._request_class().deserialize(string)
                try:
                    resp = await self._callback(req)
                except Exception as e:
                    traceback.print_exc()
                    tcpros.send_byte(chr(0).encode(), writer)
                    tcpros.send_string(str(e).encode(), writer)
                else:
                    tcpros.send_byte(chr(1).encode(), writer)
                    x = BytesIO()
                    self._type._response_class.serialize(resp, x)
                    tcpros.send_string(x.getvalue(), writer)
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            # Usually means that the client has just disconnected
            pass
        finally:
            writer.close()
