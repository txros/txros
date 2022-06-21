from __future__ import division

from twisted.internet import defer
from twisted.web import xmlrpc

from . import util


class Error(Exception):
    """
    Represents an error that occurred in the XMLRPC communication.

    .. container:: operations

        .. describe:: str(x)

            Pretty-prints the error status code and message.

        .. describe:: repr(x)

            Pretty-prints the error status code and message. Equivalent to
            ``str(x)``.

    Attributes:
        code (int): The status code returned by the ROS server.
        message (str): The status message returned by the ROS server.
    """
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return "rosxmlrpc.Error" + repr((self.code, self.message))

    __repr__ = __str__


class Proxy:
    """
    The txROS proxy to the ROS master server.

    .. container:: operations

        .. describe:: x.attr

            Calls the master server with the attribute name representing the name
            of the method to call. The value is returned in a Deferred object.
            If the status code is not 1, then :class:`txros.Error` is raised.
    """
    def __init__(self, proxy: xmlrpc.Proxy, caller_id: str):
        """
        Args:
            proxy (xmlrpc.Proxy): The proxy representing an XMLRPC connection to the ROS
                master server.
            caller_id (str): The ID of the caller in the proxy.
        """
        self._master_proxy = proxy
        self._caller_id = caller_id

    def __getattr__(self, name: str):
        @util.cancellableInlineCallbacks
        def remote_caller(*args):
            statusCode, statusMessage, value = yield self._master_proxy.callRemote(
                name, self._caller_id, *args
            )
            if statusCode == 1:  # SUCCESS
                defer.returnValue(value)
            else:
                raise Error(statusCode, statusMessage)

        return remote_caller
