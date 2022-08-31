from __future__ import annotations

import warnings
import asyncio
import traceback
from typing import TYPE_CHECKING, Callable, Generic, TypeVar
from types import TracebackType

import genpy

from . import rosxmlrpc, tcpros, util, types

if TYPE_CHECKING:
    from .nodehandle import NodeHandle

M = TypeVar("M", bound = types.Message)

class Subscriber(Generic[M]):
    """
    A subscriber in the txROS suite. This class should usually be made through
    :meth:`txros.NodeHandle.subscribe`.

    .. container:: operations

        .. describe:: async with x:

            On entering the block, the subscriber is :meth:`~.setup`; upon leaving the block,
            the subscriber is :meth:`~.shutdown`.
    """
    _message_futs: list[asyncio.Future]
    _publisher_threads: dict[str, asyncio.Task]
    _is_running: bool

    def __init__(
        self,
        node_handle: NodeHandle,
        name: str,
        message_type: type[M],
        callback: Callable[[M], M | None] = lambda message: None,
    ):
        """
        Args:
            node_handle (NodeHandle): The node handle used to communicate with the
                ROS master server.
            name (str): The name of the topic to subscribe to.
            message_type (Type[genpy.Message]): The message class shared by the topic.
            callback (Callable[[genpy.Message], genpy.Message | None]): The callback to use with
                the subscriber. The callback should receive an instance of the message
                shared by the topic and return None.
        """
        self._node_handle = node_handle
        self._name = self._node_handle.resolve_name(name)
        self.message_type = message_type
        self._callback = callback

        self._publisher_threads = {}
        self._last_message = None
        self._last_message_time = None
        self._message_futs = []
        self._connections = {}

        self._node_handle.shutdown_callbacks.add(self.shutdown)
        self._is_running = False

    async def setup(self) -> None:
        """
        Sets up the subscriber by registering the subscriber with ROS and listing
        handlers supported by the topic.

        The subscriber must be setup before use.
        """
        assert (
            "publisherUpdate",
            self._name,
        ) not in self._node_handle.xmlrpc_handlers
        self._node_handle.xmlrpc_handlers[
            "publisherUpdate", self._name
        ] = self._handle_publisher_list
        publishers = (
            await self._node_handle.master_proxy.register_subscriber(
                self._name,
                self.message_type._type,
                self._node_handle.xmlrpc_server_uri,
            )
        )
        self._handle_publisher_list(publishers)
        self._is_running = True

    def __del__(self):
        if self._is_running:
            warnings.simplefilter("always", ResourceWarning)
            warnings.warn(
                f"The '{self._name}' subscriber was never shutdown(). This may cause issues with this instance of ROS - please fix the errors and completely restart ROS.",
                ResourceWarning
            )
            warnings.simplefilter("default", ResourceWarning)

    async def __aenter__(self) -> Subscriber:
        await self.setup()
        return self

    async def __aexit__(
        self, exc_type: type[Exception], exc_value: Exception, traceback: TracebackType
    ):
        await self.shutdown()

    def is_running(self) -> bool:
        """
        Returns:
            bool: Whether the subscriber is running.
        """
        return self._is_running

    async def shutdown(self):
        """
        Shuts the subscriber down. All operations scheduled by the subscriber
        are immediately cancelled.

        Raises:
            ResourceWarning: The subscriber is already not running.
        """
        if not self._is_running:
            warnings.simplefilter("always", ResourceWarning)
            warnings.warn(
                f"The {self._name} subscriber is not currently running. It may have been shutdown previously or never started.",
                ResourceWarning
            )
            warnings.simplefilter("default", ResourceWarning)
            return

        try:
            await self._node_handle.master_proxy.unregisterSubscriber(
                self._name, self._node_handle.xmlrpc_server_uri
            )
        except Exception:
            traceback.print_exc()
        del self._node_handle.xmlrpc_handlers["publisherUpdate", self._name]

        self._node_handle.shutdown_callbacks.discard(self.shutdown)
        for _, task in self._publisher_threads.items():
            task.cancel()
        self._handle_publisher_list([])
        self._is_running = False

    def get_last_message(self) -> M | None:
        """
        Gets the last received message. This may be ``None`` if no message has been
        received.

        Returns:
            Optional[genpy.Message]: The last sent message, or ``None``.
        """
        return self._last_message

    def get_last_message_time(self) -> genpy.Time | None:
        """
        Gets the time associated with the last received message. May be ``None`` if
        no message has been received yet.

        Returns:
            Optional[genpy.Time]: The time of the last received message, or ``None``.
        """
        return self._last_message_time

    def get_next_message(self) -> asyncio.Future[M]:
        """
        Returns a deferred which will contain the next message.

        Returns:
            asyncio.Future[genpy.Message]: A future which will eventually contain
            the next message published on the topic.
        """
        res = asyncio.Future()
        self._message_futs.append(res)
        return res

    def get_connections(self) -> list[str]:
        """
        Returns the ``callerid`` of each connected client. If a connection does
        not provide an ID, then the value may be None.

        Returns:
            List[str]: A list of the caller IDs of all nodes connected to the
            subscriber.
        """
        return list(self._connections.values())

    async def _publisher_thread(self, url: str) -> None:
        while True:
            try:
                proxy = rosxmlrpc.ROSMasterProxy(
                    rosxmlrpc.AsyncServerProxy(url, self._node_handle),
                    self._node_handle._name,
                )
                value = await proxy.request_topic(self._name, [["TCPROS"]])

                _, host, port = value
                assert isinstance(host, str)
                assert isinstance(port, int)
                reader, writer = await asyncio.open_connection(
                    host, port
                )
                try:
                    tcpros.send_string(
                        tcpros.serialize_dict(
                            dict(
                                message_definition=self.message_type._full_text,
                                callerid=self._node_handle._name,
                                topic=self._name,
                                md5sum=self.message_type._md5sum,
                                type=self.message_type._type,
                            )
                        ),
                        writer
                    )
                    header = tcpros.deserialize_dict((await tcpros.receive_string(reader)))
                    self._connections[writer] = header.get("callerid", None)
                    try:
                        while True:
                            data = await tcpros.receive_string(reader)
                            msg = self.message_type().deserialize(data)
                            try:
                                self._callback(msg)
                            except:
                                traceback.print_exc()

                            self._last_message = msg
                            self._last_message_time = self._node_handle.get_time()

                            old, self._message_futs = self._message_futs, []
                            for fut in old:
                                fut.set_result(msg)
                    except (ConnectionRefusedError, BrokenPipeError, ConnectionResetError, asyncio.IncompleteReadError):
                        pass
                    finally:
                        del self._connections[writer]
                except (ConnectionRefusedError, BrokenPipeError, ConnectionResetError, asyncio.IncompleteReadError):
                    pass
                finally:
                    writer.close()
            except (ConnectionRefusedError, BrokenPipeError, ConnectionResetError, asyncio.IncompleteReadError):
                pass
            except Exception:
                traceback.print_exc()

            await util.wall_sleep(
                1
            )  # pause so that we don't repeatedly reconnect immediately on failure

    def _handle_publisher_list(self, publishers: list[str]) -> tuple[int, str, bool]:
        new = {
            k: self._publisher_threads.pop(k)
            if k in self._publisher_threads
            else asyncio.create_task(self._publisher_thread(k))
            for k in publishers
        }
        for k, v in self._publisher_threads.items():
            v.cancel()
        self._publisher_threads = new

        return 1, "success", False
