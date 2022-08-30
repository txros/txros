from __future__ import annotations

import asyncio
import warnings
import functools
import os
import random
import socket
import sys
import time
import traceback
from types import TracebackType
from typing import Any, Awaitable, Callable, Coroutine, TypeVar

import aiohttp
import genpy
import yaml
from aiohttp import web
from aiohttp_xmlrpc import handler as xmlrpc_handler
from roscpp.srv import (
    GetLoggers,
    GetLoggersRequest,
    GetLoggersResponse,
    SetLoggerLevel,
    SetLoggerLevelRequest,
    SetLoggerLevelResponse,
)
from rosgraph_msgs.msg import Clock

from . import (
    publisher,
    rosxmlrpc,
    service,
    serviceclient,
    subscriber,
    tcpros,
    types,
    util,
)

M = TypeVar("M", bound=types.Message)
Request = TypeVar("Request", bound=types.Message)
Reply = TypeVar("Reply", bound=types.Message)
S = TypeVar("S", bound = types.ServiceMessage)


class _XMLRPCSlave(xmlrpc_handler.XMLRPCView):

    node_handle: NodeHandle

    def __init__(self, request: web.Request, *, node_handle: NodeHandle):
        self.node_handle = node_handle
        super().__init__(request)

    def __getattr__(self, attr: str):
        if attr.startswith("xmlrpc_") or attr.startswith("rpc_"):
            print(attr, "not implemented")
        raise AttributeError(attr)

    def rpc_getBusStats(self, _) -> tuple[int, str, list[list[Any]]]:
        return 1, "success", [[], [], []]  # XXX

    def rpc_getBusInfo(self, _) -> tuple[int, str, list[Any]]:
        # list of (connectionId, destinationId, direction, transport, topic, connected)
        return 1, "success", []  # XXX

    def rpc_getMasterUri(self, _) -> tuple[int, str, str]:
        return 1, "success", self.node_handle.master_uri

    async def rpc_shutdown(self, _, msg: str = ""):
        print("Shutdown requested. Reason:", repr(msg))
        # XXX should somehow tell/wait for user code to cleanly exit here, e.g.
        # by .cancel()ing main coroutine
        await self.node_handle.shutdown()

        async def _kill_soon():
            await util.wall_sleep(0)
            sys.exit(0)

        await _kill_soon()
        return (1, "success", False)

    def rpc_getPid(self, _) -> tuple[int, str, int]:
        return 1, "success", os.getpid()

    def rpc_getSubscriptions(self, _) -> tuple[int, str, list[Any]]:
        return 1, "success", []  # XXX

    def rpc_getPublications(self, _) -> tuple[int, str, list[Any]]:
        return 1, "success", []  # XXX

    def rpc_paramUpdate(self, parameter_key, parameter_value) -> tuple[int, str, bool]:
        del parameter_key, parameter_value
        return 1, "success", False  # XXX

    def rpc_publisherUpdate(self, _, topic: str, publishers):
        return self.node_handle.xmlrpc_handlers.get(
            ("publisherUpdate", topic), lambda _: (1, "success", True)
        )(publishers)

    def rpc_requestTopic(self, _, topic: str, protocols):
        return self.node_handle.xmlrpc_handlers.get(
            ("requestTopic", topic),
            lambda _: (-1, f"Not a publisher of [{topic}]", []),
        )(protocols)


class NodeHandle:
    """
    Class representing a Pythonic handle for a ROS node, similar to the C++
    concept of a node handle. This class provides asynchronous communication with
    several ROS features, including publishers, subscribers, and services through
    helper methods.

    This node must be constructeed using the `await` keyword because it spins up
    resources upon initialization. As a result, you cannot setup this node using a specialized
    method.

    You must shutdown the node using :meth:`~.shutdown`. Otherwise, you can use an
    asynchronous context manager.

    .. warning::

        You cannot spawn two nodes with the same name. Attempts to do so will result
        in the node which already exists being killed to make way for the new node
        you are initiailizing. If the other node is shutdown, it will be shutdown
        safely.

    The node handle can be used for general purposes, such as parameters and sleeping:

    .. code-block:: python

        >>> import os
        >>> import txros
        >>> nh = await NodeHandle("/test", "special_node", "localhost", os.environ["ROS_MASTER_URI"], {})
        >>> nh.get_name()
        '/test/special_node'
        >>> await nh.set_param("special_param", True)
        >>> await nh.get_param("special_param")
        True
        >>> await nh.delete_param("special_param")
        >>> try:
        ...     await nh.get_param("special_param")
        ... except txros.ROSMasterException:
        ...     print("This parameter does not exist!")
        This parameter does not exist!
        >>> await nh.sleep(2) # Sleeps for 2 seconds

    The node handle can also be used for publishing and subscribing to topics. Note
    that all publishers and subscribers must be setup.

    .. code-block:: python

        >>> import os
        >>> import txros
        >>> nh = await txros.NodeHandle("/test", "special_node", "localhost", os.environ["ROS_MASTER_URI"], {})
        >>> from std_msgs.msg import Int32
        >>> pub = nh.advertise("running_time", Int32)
        >>> await pub.setup()
        >>> async def publish():
        ...     try:
        ...         count = 0
        ...         while True:
        ...             pub.publish(Int32(count))
        ...             count += 1
        ...             await asyncio.sleep(1)
        ...     except asyncio.CancelledError as _:
        ...         # When task gets cancelled, stop publishing
        ...         pass
        >>> task = asyncio.create_task(publish()) # Start publishing!
        >>> sub = nh.subscribe("running_time", Int32)
        >>> await sub.setup()
        >>> while True:
        ...     print(await sub.get_next_message())
        4
        5
        6

    .. container:: operations

        .. describe:: async with x:

            On enter, the node handle is setup through :meth:`~.setup`. Upon leaving
            the context block, the nodehandle is :meth:`~.shutdown`.

    Attributes:
        master_uri (str): The master URI used by the handle to communicate with ROS.
    """

    master_uri: str
    _ns: str
    _name: str
    _addr: str
    shutdown_callbacks: set[Callable[[], Coroutine[Any, Any, Any | None]]]
    _aiohttp_session: aiohttp.ClientSession
    _tcpros_server_uri: str
    _is_running: bool
    master_proxy: rosxmlrpc.ROSMasterProxy
    xmlrpc_server_uri: str
    tcpros_handlers: dict[tuple[str, str], types.TCPROSProtocol]
    xmlrpc_handlers: dict[tuple[str, str], Callable[[Any], tuple[int, str, Any]]]
    _tcpros_server: asyncio.Server
    _tcpros_server_addr: tuple[str, int]
    _use_sim_time: bool
    _clock_sub: subscriber.Subscriber
    _remappings: dict[str, str]
    _sim_time: genpy.Time
    _xmlrpc_runner: web.AppRunner
    _xmlrpc_site: web.TCPSite
    get_log_serv: service.Service
    set_log_serv: service.Service

    @classmethod
    def from_argv_with_remaining(
        cls, default_name: str, argv=None, anonymous: bool = False
    ) -> tuple[NodeHandle, list[str]]:
        """
        Constructs a handle from an argument vector.

        Args:
            default_name (str): The default name of the node.
            argv (???): The argument vector to use.
            anonymous (bool): Whether to make the node anonymous. Appends a random
                number to the end of the node name.
        """
        if not argv:
            argv = sys.argv

        res = [argv[0]]
        remappings = {}
        for arg in argv[1:]:
            if ":=" in arg:
                before, after = arg.split(":=")
                assert before not in remappings
                remappings[before] = after
            else:
                res.append(arg)

        name = default_name
        if anonymous:
            name = name + "_" + "{:016x}".format(random.randrange(2**64))
        if "__name" in remappings:
            name = remappings["__name"]

        addr = socket.gethostname()  # could be a bit smarter
        if "ROS_IP" in os.environ:
            addr = os.environ["ROS_IP"]
        if "ROS_HOSTNAME" in os.environ:
            addr = os.environ["ROS_HOSTNAME"]
        if "__ip" in remappings:
            addr = remappings["__ip"]
        if "__hostname" in remappings:
            addr = remappings["__hostname"]

        master_uri = None
        if "ROS_MASTER_URI" in os.environ:
            master_uri = os.environ["ROS_MASTER_URI"]
        if "__master" in remappings:
            master_uri = remappings["__master"]
        if master_uri is None:
            raise ValueError(
                "either ROS_MASTER_URI variable or __master argument has to be provided"
            )

        ns = ""
        if "ROS_NAMESPACE" in os.environ:
            ns = os.environ["ROS_NAMESPACE"]
        if "__ns" in remappings:
            ns = remappings["__ns"]

        return (
            cls(
                ns=ns,
                name=name,
                addr=addr,
                master_uri=master_uri,
                remappings=remappings,
            )
        ), res

    @classmethod
    def from_argv(cls, *args, **kwargs) -> NodeHandle:
        """
        Constructs a handle using the asynchronous generator format. All *args
        and **kwargs are passed to :meth:`.from_argv_with_remaining`.
        """
        return (cls.from_argv_with_remaining(*args, **kwargs))[0]

    def __init__(self, ns: str, name: str, addr: str, master_uri: str, remappings: dict[str, str]):
        self._ns = ns
        self._name = name
        self._addr = addr
        self.master_uri = master_uri
        self._remappings = remappings
        self._is_running = False
        self._is_setting_up = False

    async def setup(self):
        """
        Args:
            ns (str): The namespace to put the node under. The namespace should
                not end with a `/`. For example, `nsexample/test`.
            name (str): The name of the node itself. The name will be appended behind
                the namespace.
            addr (str): The address of the ROS master server, typically specified
                as an IPv4 address, and often, ``localhost``.
            master_uri (str): The URI linking to the ROS master server.
        """
        # constraints: anything blocking here should print something if it's
        # taking a long time in order to avoid confusion

        loop = asyncio.get_event_loop()

        if self._ns:
            assert self._ns[0] == "/"
        assert not self._ns.endswith("/")
        self._ns = self._ns  # valid values: '', '/a', '/a/b'

        assert "/" not in self._name
        self._name = self._ns + "/" + self._name

        self.shutdown_callbacks = set()

        self._addr = self._addr
        self._is_setting_up = True

        self._aiohttp_session = aiohttp.ClientSession()
        self.master_proxy = rosxmlrpc.ROSMasterProxy(
            rosxmlrpc.AsyncServerProxy(self.master_uri, self),
            self._name,
        )

        self.xmlrpc_handlers = {}

        # Start up an HTTP server to handle incoming XMLRPC requests - below is
        # an alternative to the blocking web.run_app method that is sometimes
        # used with aiohttp.
        app = web.Application()

        async def handle_xmlrpc(request: web.Request):
            slave = _XMLRPCSlave(request, node_handle=self)
            return await slave.post()

        app.router.add_route("*", "/", handle_xmlrpc)

        self._xmlrpc_runner = web.AppRunner(app)
        await self._xmlrpc_runner.setup()
        self._xmlrpc_site = web.TCPSite(self._xmlrpc_runner, host="127.0.1.1", port=0)
        await self._xmlrpc_site.start()

        self.shutdown_callbacks.add(self._xmlrpc_runner.cleanup)
        self.xmlrpc_server_uri = "http://%s:%i/" % (
            self._addr,
            self._xmlrpc_site._server.sockets[0].getsockname()[1],
        )

        self.tcpros_handlers = {}
        self._tcpros_server = await asyncio.start_server(
            functools.partial(tcpros.callback, self.tcpros_handlers),
            family=socket.AF_INET,
            port=0,
        )  # Port 0 lets the host assign the port for us
        tcpros_server_port = self._tcpros_server.sockets[0].getsockname()[1]
        self._tcpros_server_addr = self._addr, tcpros_server_port
        self._tcpros_server_uri = f"rosrpc://{self._addr}:{tcpros_server_port}"
        print(f"XMLRPC URI: {self.xmlrpc_server_uri}")

        while True:
            try:
                other_node_uri = await self.master_proxy.lookupNode(self._name)
            except rosxmlrpc.ROSMasterException:
                break  # assume that error means unknown node
            except Exception as _:
                traceback.print_exc()
                await util.wall_sleep(1)  # pause so we don't retry immediately
            else:
                other_node_proxy = rosxmlrpc.AsyncServerProxy(
                    other_node_uri, self
                )
                try:
                    await util.wrap_timeout(
                        other_node_proxy.shutdown("new node registered with same name"),
                        3,
                    )
                except ConnectionRefusedError:
                    pass
                except Exception:
                    traceback.print_exc()
                break

        try:
            self._use_sim_time = await self.get_param("/use_sim_time")
        except rosxmlrpc.ROSMasterException:  # assume that error means not found
            self._use_sim_time = False

        if self._use_sim_time:

            def got_clock(msg: Clock):
                self._sim_time = msg.clock

            self._clock_sub = self.subscribe("/clock", Clock, got_clock)
            await self._clock_sub.setup()
            # make sure self._sim_time gets set before we continue
            await util.wrap_time_notice(
                self._clock_sub.get_next_message(),
                1,
                "getting simulated time from /clock",
            )

        for k, v in self._remappings.items():
            if k.startswith("_") and not k.startswith("__"):
                await self.set_param(self.resolve_name("~" + k[1:]), yaml.safe_load(v))

        async def get_loggers_response(_: GetLoggersRequest) -> GetLoggersResponse:
            return GetLoggersResponse()

        async def set_loggers_response(
            _: SetLoggerLevelRequest,
        ) -> SetLoggerLevelResponse:
            return SetLoggerLevelResponse()

        self.get_log_serv = self.advertise_service(
            "~get_loggers", GetLoggers, get_loggers_response
        )
        self.set_log_serv = self.advertise_service(
            "~set_logger_level", SetLoggerLevel, set_loggers_response
        )
        await asyncio.gather(self.get_log_serv.setup(), self.set_log_serv.setup())
        self.shutdown_callbacks.update([self.get_log_serv.shutdown, self.set_log_serv.shutdown])

        self._is_running = True
        self._is_setting_up = False
        return self

    async def __aenter__(self) -> NodeHandle:
        await self.setup()
        return self

    async def __aexit__(
        self, exc_type: type[Exception], exc_value: Exception, traceback: TracebackType
    ):
        await self.shutdown()

    def __del__(self):
        if self._is_running:
            warnings.simplefilter("always", ResourceWarning)
            warnings.warn(
                f"shutdown() was never called for node '{self._name}'",
                ResourceWarning,
            )
            warnings.simplefilter("default", ResourceWarning)

    async def shutdown(self) -> None:
        """
        Shuts down all active connections.
        """
        if not hasattr(self, "_shutdown_thread"):
            self._shutdown_thread = self._real_shutdown()
        return await self._shutdown_thread

    async def _real_shutdown(self):
        self._is_running = False

        while self.shutdown_callbacks:
            self.shutdown_callbacks, old = set(), self.shutdown_callbacks
            old_coros = [func() for func in old]
            for coro in old_coros:
                print(f"Awaiting shutdown callback {coro.__name__}")
                try:
                    await coro
                except:
                    traceback.print_exc()

        # Close the open aiohttp connection used for XMLRPC
        # Clean this up LAST - other services/publishers/subscribers may still
        # be using this session
        if not self._aiohttp_session.closed:
            # print("it's not closed lol")
            self._tcpros_server.close()
            await self._tcpros_server.wait_closed()
            await self._xmlrpc_runner.cleanup()
            await self._aiohttp_session.close()

    def get_name(self) -> str:
        """
        Returns:
            str: The name of the node handle.
        """
        return self._name

    def get_time(self) -> genpy.Time:
        """
        Returns the time used by the node handle. If simulated time is requested
        to be used, then the current simulated time is returned. Otherwise, the current
        real time is returned.

        Returns:
            genpy.Time: The current time used by the node handle.
        """
        if self._use_sim_time:
            return self._sim_time
        else:
            return genpy.Time(time.time())

    async def sleep_until(self, future_time: float | int | genpy.Time):
        """
        Sleeps for a specified amount of time.

        Args:
            time (Union[float, int, genpy.Time]): The amount of time to sleep until.
                If a float or integer is used, then the generated time is constructed
                through the ``from_sec`` method of ``genpy.Time``.
        """
        if isinstance(future_time, (float, int)):
            future_time = genpy.Time.from_sec(future_time)
        elif not isinstance(future_time, genpy.Time):
            raise TypeError("expected float or genpy.Time")

        if self._use_sim_time:
            while self._sim_time < future_time:
                await self._clock_sub.get_next_message()
        else:
            while True:
                current_time = self.get_time()
                if current_time >= future_time:
                    return
                await util.wall_sleep((future_time - current_time).to_sec())

    async def sleep(self, duration: float | int | genpy.Duration):
        """
        Sleeps for a specified duration of time.

        Args:
            duration (Union[float, int, genpy.Duration]): The amount of time to sleep
                for.
        """
        if isinstance(duration, (float, int)):
            duration = genpy.Duration.from_sec(duration)
        elif not isinstance(duration, genpy.Duration):
            raise TypeError("expected float or genpy.Duration")

        return await self.sleep_until(self.get_time() + duration)

    def resolve_name_without_remapping(self, name: str) -> str:
        """
        Resolves the true name of a given node path.

        Args:
            name (str): The path to resolve.

        Returns:
            str: The resolved path.
        """
        if name.startswith("/"):
            return name
        if name.startswith("~"):
            return self._name + "/" + name[1:]
        return self._ns + "/" + name

    def resolve_name(self, name: str) -> str:
        """
        Resolves the name of the node handle and updates the name attribute with this
        resolved name.
        """
        name = self.resolve_name_without_remapping(name)
        for before_unresolved, after_unresolved in self._remappings.items():
            before = self.resolve_name_without_remapping(before_unresolved)
            after = self.resolve_name_without_remapping(after_unresolved)
            if name == before or name.startswith(before + "/"):
                return after + name[len(before) :]
        return name

    def is_running(self) -> bool:
        """
        Returns:
            bool: Whether the node handle is running.
        """
        return self._is_running

    def is_shutdown(self) -> bool:
        """
        Returns:
            bool: Whether the node handle is not running.
        """
        return not self._is_running

    def advertise_service(
        self,
        name: str,
        service_message: types.ServiceMessage[Request, Reply],
        callback: Callable[[Request], Awaitable[Reply]],
    ) -> service.Service[Request, Reply]:
        """
        Creates a service using this node handle. The arguments and keyword
        arguments passed to this method are passed into the constructor of the service;
        check there for information on what arguments can be passed in.

        Returns:
            txros.Service: The given service.
        """
        if not self._is_running and not self._is_setting_up:
            raise RuntimeError("The node is not currently running. It may never have been setup() or may have already been shutdown().")

        return service.Service(self, name, service_message, callback)

    def get_service_client(
        self, name: str, service_type: S
    ) -> serviceclient.ServiceClient[S]:
        """
        Creates a service client using this node handle. The arguments and keyword
        arguments passed to this method are passed into the constructor of the client;
        check there for information on what arguments can be passed in.

        Returns:
            txros.ServiceClient: The given service client.
        """
        if not self._is_running and not self._is_setting_up:
            raise RuntimeError("The node is not currently running. It may never have been setup() or may have already been shutdown().")

        return serviceclient.ServiceClient(self, name, service_type)

    def subscribe(
        self,
        name: str,
        message_type: type[M],
        callback: Callable[[M], M | None] = lambda _: None,
    ) -> subscriber.Subscriber[M]:
        """
        Creates a subscriber using this node handle. The arguments and keyword
        arguments passed to this method are passed into the constructor of the subscriber;
        check there for information on what arguments can be passed in.

        Note that the subscriber can only be used when it is setup through :meth:`Subscriber.setup`.

        Returns:
            txros.Subscriber: The given subscriber.
        """
        if not self._is_running and not self._is_setting_up:
            raise RuntimeError("The node is not currently running. It may never have been setup() or may have already been shutdown().")

        return subscriber.Subscriber(self, name, message_type, callback)

    def advertise(
        self, name: str, message_type: type[M], *, latching: bool = False
    ) -> publisher.Publisher[M]:
        """
        Creates a publisher using this node handle. The arguments and keyword
        arguments passed to this method are passed into the constructor of the publisher;
        check there for information on what arguments should be passed in.

        Note that the publisher can only be used when it is setup through :meth:`Publisher.setup`.

        Returns:
            txros.Publisher: The given publisher.
        """
        if not self._is_running and not self._is_setting_up:
            raise RuntimeError("The node is not currently running. It may never have been setup() or may have already been shutdown().")

        return publisher.Publisher(self, name, message_type, latching)

    async def get_param(self, key: str) -> rosxmlrpc.XMLRPCLegalType:
        """
        Gets a parameter value from the ROS parameter server. If the key requested
        is a namespace, then a dictionary representing all keys below the given
        namespace is returned.

        Args:
            key (str): The name of the parameter to retrieve from the server.

        Raises:
            :class:`ROSMasterException`: The parameter is not set.

        Returns:
            :class:`txros.XMLRPCLegalType`: The value of the parameter with the given
            name.
        """
        if not self._is_running and not self._is_setting_up:
            raise RuntimeError("The node is not currently running. It may never have been setup() or may have already been shutdown().")

        return await self.master_proxy.getParam(key)

    async def has_param(self, key: str) -> bool:
        """
        Checks whether the ROS parameter server has a parameter with the specified
        name.

        Args:
            key (str): The name of the parameter to check the status of.

        Returns:
            bool: Whether the parameter server has the specified key.
        """
        if not self._is_running and not self._is_setting_up:
            raise RuntimeError("The node is not currently running. It may never have been setup() or may have already been shutdown().")

        return await self.master_proxy.hasParam(key)

    async def delete_param(self, key: str) -> int:
        """
        Deletes the specified parameter from the parameter server.

        Args:
            key (str): The parameter to delete from the parameter server.

        Returns:
            int: The result of the delete operation. According to ROS documentation,
            this value can be ignored.
        """
        if not self._is_running and not self._is_setting_up:
            raise RuntimeError("The node is not currently running. It may never have been setup() or may have already been shutdown().")

        return await self.master_proxy.deleteParam(key)

    async def set_param(self, key: str, value: Any) -> int:
        """
        Sets a parameter and value in the ROS parameter server.

        Args:
            key (str): The parameter to set in the parameter server.
            value (:class:`txros.XMLRPCLegalType`): The value to set for the given parameter.

        Returns:
            int: The result of setting the parameter. According to the ROS documentation,
            this value can be ignored.
        """
        if not self._is_running and not self._is_setting_up:
            raise RuntimeError("The node is not currently running. It may never have been setup() or may have already been shutdown().")

        return await self.master_proxy.setParam(key, value)

    async def search_param(self, key: str) -> str:
        """
        Searches for a parameter on the ROS parameter server, and returns the first
        key found that matches the search term.

        For more information on how searching is done to find a matching key, view
        the `ROS wiki documentation on this method <http://wiki.ros.org/ROS/Parameter%20Server%20API>`_.

        Args:
            key (str): The search key to use to find parameters.

        Returns:
            str: The name of the first key found.
        """
        if not self._is_running and not self._is_setting_up:
            raise RuntimeError("The node is not currently running. It may never have been setup() or may have already been shutdown().")

        return await self.master_proxy.searchParam(key)

    async def get_param_names(self) -> list[str]:
        """
        Gets the names of all parameters in the ROS parameter server.

        Returns:
            List[str]: The names of all parameters in the server.
        """
        if not self._is_running and not self._is_setting_up:
            raise RuntimeError("The node is not currently running. It may never have been setup() or may have already been shutdown().")

        return await self.master_proxy.getParamNames()
