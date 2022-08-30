from __future__ import annotations

import warnings
import asyncio
import traceback
from io import BytesIO
from typing import TYPE_CHECKING, Any, Generic, Type, TypeVar

import genpy

from txros import tcpros, types, util

if TYPE_CHECKING:
    from .nodehandle import NodeHandle

M = TypeVar("M", bound=types.Message)


class Publisher(Generic[M]):
    """
    A Publisher in the txROS suite. Managed through a node handle.

    Args:
        node_handle (NodeHandle): The node handle to associate with this publisher.
            Used to communicate with the ROS master server.
        name (str): The name of the publisher topic.
        message_type (Type[genpy.Message]): The message type that will be published
            on the topic.
        latching (bool): Enables latching on the publisher. This ensures that all
            new connections are immediately sent the most recently sent message
            when they connect to the publisher.
    """

    _connections: dict[asyncio.StreamWriter, str]
    _name: str
    _node_handle: NodeHandle
    _is_running: bool

    def __init__(
        self,
        node_handle: NodeHandle,
        name: str,
        message_type: type[M],
        latching: bool = False,
    ):
        self._node_handle = node_handle
        self._name = self._node_handle.resolve_name(name)
        self.message_type = message_type
        self._latching = latching

        self._last_message_data = None
        self._connections = {}

        self._node_handle.shutdown_callbacks.add(self.shutdown)
        self._is_running = False

    async def setup(self) -> None:
        """
        Sets up the publisher by registering the name of the publisher with ROS
        and registering callbacks associated with the publisher with the parent
        node handle.

        This should always be called before the publisher is ever used.
        """
        assert ("topic", self._name) not in self._node_handle.tcpros_handlers
        self._node_handle.tcpros_handlers[
            "topic", self._name
        ] = self._handle_tcpros_conn
        assert (
            "requestTopic",
            self._name,
        ) not in self._node_handle.xmlrpc_handlers
        self._node_handle.xmlrpc_handlers[
            "requestTopic", self._name
        ] = self._handle_requestTopic

        # Register the publisher with the master ROS node
        await self._node_handle.master_proxy.register_publisher(
            self._name,
            self.message_type._type,
            self._node_handle.xmlrpc_server_uri,
        )
        print(f"Publisher {self._name} is now registered.")
        self._is_running = True

    def __del__(self):
        if self._is_running:
            warnings.simplefilter("always", ResourceWarning)
            warnings.warn(
                f"The '{self._name}' publisher was never shutdown(). This may cause issues with this instance of ROS - please fix the errors and completely restart ROS.",
                ResourceWarning,
            )
            warnings.simplefilter("default", ResourceWarning)

    async def shutdown(self):
        """
        Shuts the publisher down. All operations scheduled by the publisher are cancelled.
        """
        if not self._is_running:
            raise RuntimeError(f"The {self._name} publisher is not running. It may have been shutdown previously or never started.")

        try:
            await self._node_handle.master_proxy.unregister_publisher(
                self._name, self._node_handle.xmlrpc_server_uri
            )
        except Exception:
            traceback.print_exc()
        del self._node_handle.tcpros_handlers["topic", self._name]
        del self._node_handle.xmlrpc_handlers["requestTopic", self._name]

        self._node_handle.shutdown_callbacks.discard(self.shutdown)
        self._is_running = False

    def _handle_requestTopic(self, protocols) -> tuple[int, str, list[str | int]]:
        del protocols  # This method is protocol-agnostic
        return (
            1,
            "ready on " + self._node_handle._tcpros_server_uri,
            [
                "TCPROS",
                self._node_handle._tcpros_server_addr[0],
                self._node_handle._tcpros_server_addr[1],
            ],
        )

    async def _handle_tcpros_conn(
        self,
        headers: dict[str, str],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            tcpros.send_string(
                tcpros.serialize_dict(
                    dict(
                        callerid=self._node_handle._name,
                        type=self.message_type._type,
                        md5sum=self.message_type._md5sum,
                        latching="1" if self._latching else "0",
                    )
                ),
                writer,
            )
            print("I just sent some data back!")

            if self._latching and self._last_message_data is not None:
                tcpros.send_string(self._last_message_data, writer)

            self._connections[writer] = headers["callerid"]
            try:
                while True:
                    print(await tcpros.receive_string(reader))
            except (asyncio.IncompleteReadError, BrokenPipeError, ConnectionResetError):
                # These exceptions are likely related to the client exiting, so we
                # can ignore them
                pass
            finally:
                del self._connections[writer]
        except (asyncio.IncompleteReadError, BrokenPipeError, ConnectionResetError):
            # Exceptions related to client exiting - ignore
            pass
        finally:
            writer.close()

    def publish(self, msg: M) -> None:
        """
        Publishes a message onto the topic. The message is serialized and sent to
        all connections connected to the publisher.

        Args:
            msg (genpy.Message): The ROS message to send to all connected clients.
        """
        x = BytesIO()
        self.message_type.serialize(msg, x)
        data = x.getvalue()

        for conn in self._connections:
            tcpros.send_string(data, conn)

        if self._latching:
            self._last_message_data = data

    def get_connections(self):
        """
        Gets connections to the publisher.
        """
        return list(self._connections.values())
