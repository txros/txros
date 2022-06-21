from __future__ import annotations
import os
import random
import socket
import sys
import traceback
import yaml
import time

from twisted.web import server, xmlrpc
from twisted.internet import defer, error, reactor
from twisted.internet.tcp import Port
from typing import Tuple, List, Any, Union

import genpy
from roscpp.srv import (
    GetLoggers,
    GetLoggersResponse,
    SetLoggerLevel,
    SetLoggerLevelResponse,
)
from rosgraph_msgs.msg import Clock

from . import util, tcpros, publisher, rosxmlrpc, service, serviceclient, subscriber


class _XMLRPCSlave(xmlrpc.XMLRPC):
    def __init__(self, node_handle: NodeHandle):
        xmlrpc.XMLRPC.__init__(self)
        self._node_handle = node_handle

    def __getattr__(self, attr):
        if attr.startswith("xmlrpc_"):
            print(attr, "not implemented")
        raise AttributeError(attr)

    def xmlrpc_getBusStats(self, _) -> Tuple[int, str, List[List[Any]]]:
        return 1, "success", [[], [], []]  # XXX

    def xmlrpc_getBusInfo(self, _) -> Tuple[int, str, List[Any]]:
        # list of (connectionId, destinationId, direction, transport, topic, connected)
        return 1, "success", []  # XXX

    def xmlrpc_getMasterUri(self, _):
        return 1, "success", self._node_handle._master_uri

    @util.cancellableInlineCallbacks
    def xmlrpc_shutdown(self, _, msg: str = ""):
        print("Shutdown requested. Reason:", repr(msg))
        # XXX should somehow tell/wait for user code to cleanly exit here, e.g. by .cancel()ing main coroutine
        yield self._node_handle.shutdown()

        @util.cancellableInlineCallbacks
        def _kill_soon():
            yield util.wall_sleep(0)
            os._exit(0)

        _kill_soon()
        defer.returnValue((1, "success", False))

    def xmlrpc_getPid(self, _) -> Tuple[int, str, int]:
        return 1, "success", os.getpid()

    def xmlrpc_getSubscriptions(self, _) -> Tuple[int, str, List[Any]]:
        return 1, "success", []  # XXX

    def xmlrpc_getPublications(self, _) -> Tuple[int, str, List[Any]]:
        return 1, "success", []  # XXX

    def xmlrpc_paramUpdate(
        self, parameter_key, parameter_value
    ) -> Tuple[int, str, bool]:
        del parameter_key, parameter_value
        return 1, "success", False  # XXX

    def xmlrpc_publisherUpdate(self, _, topic, publishers):
        return self._node_handle._xmlrpc_handlers.get(
            ("publisherUpdate", topic), lambda _: (1, "success", True)
        )(publishers)

    def xmlrpc_requestTopic(self, _, topic, protocols):
        return self._node_handle._xmlrpc_handlers.get(
            ("requestTopic", topic),
            lambda _: (-1, "Not a publisher of [%s]" % topic, []),
        )(protocols)


class NodeHandle:
    @classmethod
    @util.cancellableInlineCallbacks
    def from_argv_with_remaining(cls, default_name: str, argv = None, anonymous: bool = False):
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
            name = name + "_" + "%016x" % (random.randrange(2**64),)
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

        defer.returnValue(
            (
                (
                    yield cls(
                        ns=ns,
                        name=name,
                        addr=addr,
                        master_uri=master_uri,
                        remappings=remappings,
                    )
                ),
                res,
            )
        )

    @classmethod
    @util.cancellableInlineCallbacks
    def from_argv(cls, *args, **kwargs):
        """
        Constructs a handle using the asynchronous generator format. All *args
        and **kwargs are passed to :meth:`.from_argv_with_remaining`.
        """
        defer.returnValue((yield cls.from_argv_with_remaining(*args, **kwargs))[0])

    @util.cancellableInlineCallbacks
    def __new__(cls, ns: str, name: str, addr: str, master_uri: str, remappings):
        # constraints: anything blocking here should print something if it's
        # taking a long time in order to avoid confusion

        self = object.__new__(cls)

        if ns:
            assert ns[0] == "/"
        assert not ns.endswith("/")
        self._ns = ns  # valid values: '', '/a', '/a/b'

        assert "/" not in name
        self._name = self._ns + "/" + name

        self._shutdown_callbacks = set()
        reactor.addSystemEventTrigger("before", "shutdown", self.shutdown)

        self._addr = addr
        self._master_uri = master_uri
        self._remappings = remappings

        self._master_proxy = rosxmlrpc.Proxy(
            xmlrpc.Proxy(self._master_uri.encode()), self._name
        )
        self._is_running = True

        self._xmlrpc_handlers = {}
        self._xmlrpc_server: Port = reactor.listenTCP(
            0, server.Site(_XMLRPCSlave(self))
        )
        self._shutdown_callbacks.add(self._xmlrpc_server.loseConnection)
        self._xmlrpc_server_uri = "http://%s:%i/" % (
            self._addr,
            self._xmlrpc_server.getHost().port,
        )

        self._tcpros_handlers = {}

        @util.cancellableInlineCallbacks
        def _handle_tcpros_conn(conn):
            try:
                header = tcpros.deserialize_dict((yield conn.receiveString()))

                def default(header, conn):
                    conn.sendString(
                        tcpros.serialize_dict(dict(error="unhandled connection"))
                    )
                    conn.transport.loseConnection()

                if "service" in header:
                    self._tcpros_handlers.get(("service", header["service"]), default)(
                        header, conn
                    )
                elif "topic" in header:
                    self._tcpros_handlers.get(("topic", header["topic"]), default)(
                        header, conn
                    )
                else:
                    conn.sendString(
                        tcpros.serialize_dict(
                            dict(error="no topic or service name detected")
                        )
                    )
                    conn.transport.loseConnection()
            except:
                conn.transport.loseConnection()
                raise

        def _make_tcpros_protocol(addr):
            conn = tcpros.Protocol()
            _handle_tcpros_conn(conn)
            return conn

        self._tcpros_server = reactor.listenTCP(
            0, util.AutoServerFactory(_make_tcpros_protocol)
        )
        self._shutdown_callbacks.add(self._tcpros_server.loseConnection)
        self._tcpros_server_uri = "rosrpc://%s:%i" % (
            self._addr,
            self._tcpros_server.getHost().port,
        )
        self._tcpros_server_addr = self._addr, self._tcpros_server.getHost().port

        while True:
            try:
                other_node_uri = yield self._master_proxy.lookupNode(self._name)
            except rosxmlrpc.Error:
                break  # assume that error means unknown node
            except Exception:
                traceback.print_exc()
                yield util.wall_sleep(1)  # pause so we don't retry immediately
            else:
                other_node_proxy = rosxmlrpc.Proxy(
                    xmlrpc.Proxy(other_node_uri), self._name
                )
                try:
                    yield util.wrap_timeout(
                        other_node_proxy.shutdown("new node registered with same name"),
                        3,
                    )
                except error.ConnectionRefusedError:
                    pass
                except Exception:
                    traceback.print_exc()
                break

        try:
            self._use_sim_time = yield self.get_param("/use_sim_time")
        except rosxmlrpc.Error:  # assume that error means not found
            self._use_sim_time = False
        if self._use_sim_time:

            def got_clock(msg):
                self._sim_time = msg.clock

            self._clock_sub = self.subscribe("/clock", Clock, got_clock)
            # make sure self._sim_time gets set before we continue
            yield util.wrap_time_notice(
                self._clock_sub.get_next_message(),
                1,
                "getting simulated time from /clock",
            )

        for k, v in self._remappings.items():
            if k.startswith("_") and not k.startswith("__"):
                yield self.set_param(self.resolve_name("~" + k[1:]), yaml.load(v))

        self.advertise_service(
            "~get_loggers", GetLoggers, lambda req: GetLoggersResponse()
        )
        self.advertise_service(
            "~set_logger_level", SetLoggerLevel, lambda req: SetLoggerLevelResponse()
        )

        defer.returnValue(self)

    def shutdown(self) -> defer.Deferred:
        """
        Called before the twisted reactor is shutdown. Calls the shutdown thread
        if one is specified - otherwise, the normal shutdown for the node is called.
        """
        if not hasattr(self, "_shutdown_thread"):
            self._shutdown_thread = self._real_shutdown()
        return util.branch_deferred(self._shutdown_thread)

    @util.cancellableInlineCallbacks
    def _real_shutdown(self):

        # Ensure that we are called directly from reactor rather than from a callback
        # This avoids strange case where a cancellableInlineCallbacks is cancelled while it's running
        yield util.wall_sleep(0)

        self._is_running = False
        while self._shutdown_callbacks:
            self._shutdown_callbacks, old = set(), self._shutdown_callbacks
            old_dfs = [func() for func in old]
            for df in old_dfs:
                try:
                    yield df
                except:
                    traceback.print_exc()

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

    @util.cancellableInlineCallbacks
    def sleep_until(self, time: Union[float, int, genpy.Time]):
        """
        Sleeps for a specified amount of time.

        Args:
            time (Union[float, int, genpy.Time]): The amount of time to sleep until.
                If a float or integer is used, then the generated time is constructed
                through the ``from_sec`` method of ``genpy.Time``.
        """
        if isinstance(time, (float, int)):
            time = genpy.Time.from_sec(time)
        elif not isinstance(time, genpy.Time):
            raise TypeError("expected float or genpy.Time")

        if self._use_sim_time:
            while self._sim_time < time:
                yield self._clock_sub.get_next_message()
        else:
            while True:
                current_time = self.get_time()
                if current_time >= time:
                    return
                yield util.wall_sleep((time - current_time).to_sec())

    def sleep(self, duration: Union[float, int, genpy.Duration]):
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

        return self.sleep_until(self.get_time() + duration)

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
        elif name.startswith("~"):
            return self._name + "/" + name[1:]
        else:
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

    def advertise_service(self, *args, **kwargs) -> service.Service:
        """
        Creates a service using this node handle. The arguments and keyword
        arguments passed to this method are passed into the constructor of the service;
        check there for information on what arguments can be passed in.

        Returns:
            txros.Service: The given service.
        """
        return service.Service(self, *args, **kwargs)

    def get_service_client(self, *args, **kwargs) -> serviceclient.ServiceClient:
        """
        Creates a service client using this node handle. The arguments and keyword
        arguments passed to this method are passed into the constructor of the client;
        check there for information on what arguments can be passed in.

        Returns:
            txros.ServiceClient: The given service client.
        """
        return serviceclient.ServiceClient(self, *args, **kwargs)

    def subscribe(self, *args, **kwargs) -> subscriber.Subscriber:
        """
        Creates a subscriber using this node handle. The arguments and keyword
        arguments passed to this method are passed into the constructor of the subscriber;
        check there for information on what arguments can be passed in.

        Returns:
            txros.Subscriber: The given subscriber.
        """
        return subscriber.Subscriber(self, *args, **kwargs)

    def advertise(self, *args, **kwargs) -> publisher.Publisher:
        """
        Creates a publisher using this node handle. The arguments and keyword
        arguments passed to this method are passed into the constructor of the publisher;
        check there for information on what arguments should be passed in.

        Returns:
            txros.Publisher: The given publisher.
        """
        return publisher.Publisher(self, *args, **kwargs)

    def get_param(self, key: str):
        """
        Gets a parameter value from the ROS parameter server. If the key requested
        is a namespace, then a dictionary representing all keys below the given
        namespace is returned.

        Args:
            key (str): The name of the parameter to retrieve from the server.
        """
        return self._master_proxy.getParam(key)

    def has_param(self, key: str) -> bool:
        """
        Checks whether the ROS parameter server has a parameter with the specified
        name.

        Args:
            key (str): The name of the parameter to check the status of.

        Returns:
            bool: Whether the parameter server has the specified key.
        """
        return self._master_proxy.hasParam(key)

    def delete_param(self, key: str) -> int:
        """
        Deletes the specified parameter from the parameter server.

        Args:
            key (str): The parameter to delete from the parameter server.

        Returns:
            int: The result of the delete operation. According to ROS documentation,
            this value can be ignored.
        """
        return self._master_proxy.deleteParam(key)

    def set_param(self, key: str, value: Any) -> int:
        """
        Sets a parameter and value in the ROS parameter server.

        Args:
            key (str): The parameter to set in the parameter server.
            value (Any): The value to set for the given parameter.

        Returns:
            int: The result of setting the parameter. According to the ROS documentation,
            this value can be ignored.
        """
        return self._master_proxy.setParam(key, value)

    def search_param(self, key: str) -> str:
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
        return self._master_proxy.searchParam(key)

    def get_param_names(self) -> List[str]:
        """
        Gets the names of all parameters in the ROS parameter server.

        Returns:
            List[str]: The names of all parameters in the server.
        """
        return self._master_proxy.getParamNames()
