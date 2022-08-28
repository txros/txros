from __future__ import annotations

import asyncio
import bisect
import itertools
import math
from typing import TYPE_CHECKING, List

import genpy
import numpy
from geometry_msgs.msg import Pose
from geometry_msgs.msg import Transform as TransformMsg
from geometry_msgs.msg import TransformStamped
from tf import transformations  # XXX
from tf2_msgs.msg import TFMessage
from twisted.internet import defer

from . import util

if TYPE_CHECKING:
    from .nodehandle import NodeHandle
    from .subscriber import Subscriber


def _sinc(x):
    return numpy.sinc(x / numpy.pi)


def _quat_from_rotvec(r):
    r = numpy.array(r)
    angle = numpy.linalg.norm(r)
    return numpy.concatenate(
        [
            _sinc(angle / 2)
            / 2
            * r,  # = sin(angle/2) * normalized(r), without the singularity
            [math.cos(angle / 2)],
        ]
    )


def _rotvec_from_quat(q):
    q = transformations.unit_vector(q)
    if q[3] < 0:
        q = -q
    return 2 / _sinc(math.acos(min(1, q[3]))) * q[:3]


class Transform:
    """
    Represents a tf transform in the txROS suite.

    .. container:: operations

        .. describe:: x + y

            Adds the components of each transform together.

        .. describe:: x - y

            Subtracts the components between each transform.

        .. describe:: x * y

            Multiplies the components of each transform together.

        .. describe:: str(x)

            Prints the constructor of the class in a string-formatted fashion.

        .. describe:: repr(x)

            Prints the constructor of the class in a string-formatted fashion.
            Equivalent to ``str(x)``.
    """

    @classmethod
    def identity(cls):
        """
        Creates and identity transform. The x, y, and z coordinates are all set to zero.
        The quaternion is set to ``[0, 0, 0, 1]``.
        """
        return cls([0, 0, 0], [0, 0, 0, 1])

    @classmethod
    def from_Transform_message(cls, msg: TransformMsg):
        """
        Constructs a transform from a :class:`geometry_msgs.msg.Transform` message.
        """
        return cls(
            [msg.translation.x, msg.translation.y, msg.translation.z],
            [msg.rotation.x, msg.rotation.y, msg.rotation.z, msg.rotation.w],
        )

    @classmethod
    def from_Pose_message(cls, msg: Pose):
        """
        Constructs a transform from a :class:`geometry_msgs.msg.Pose` message.
        """
        return cls(
            [msg.position.x, msg.position.y, msg.position.z],
            [
                msg.orientation.x,
                msg.orientation.y,
                msg.orientation.z,
                msg.orientation.w,
            ],
        )

    def __init__(self, p: List[float], q: List[float]):
        self._p = numpy.array(p)
        self._q = numpy.array(q)
        self._q_mat = transformations.quaternion_matrix(self._q)[:3, :3]

    def __sub__(self, other: Transform):
        return numpy.concatenate(
            [
                self._p - other._p,
                _rotvec_from_quat(
                    transformations.quaternion_multiply(
                        self._q,
                        transformations.quaternion_inverse(other._q),
                    )
                ),
            ]
        )

    def __add__(self, other: Transform):
        return Transform(
            self._p + other[:3],
            transformations.quaternion_multiply(
                _quat_from_rotvec(other[3:]),
                self._q,
            ),
        )

    def __mul__(self, other: Transform):
        return Transform(
            self._p + self._q_mat.dot(other._p),
            transformations.quaternion_multiply(self._q, other._q),
        )

    def inverse(self) -> Transform:
        """
        Constructs an inverse transform.

        Returns:
            Transform: The inverse transform.
        """
        return Transform(
            -self._q_mat.T.dot(self._p),
            transformations.quaternion_inverse(self._q),
        )

    def as_matrix(self):
        return transformations.translation_matrix(self._p).dot(
            transformations.quaternion_matrix(self._q)
        )

    def __str__(self):
        return "Transform(%r, %r)" % (self._p, self._q)

    __repr__ = __str__

    def transform_point(self, point):
        return self._p + self._q_mat.dot(point)

    def transform_vector(self, vector):
        return self._q_mat.dot(vector)

    def transform_quaternion(self, quaternion):
        return transformations.quaternion_multiply(self._q, quaternion)


def _make_absolute(frame_id: str) -> str:
    if not frame_id.startswith("/"):
        return "/" + frame_id
    return frame_id


class TransformListener:

    _node_handle: NodeHandle
    _history_length: genpy.Duration
    _id_count: itertools.count
    _tfs: dict[str, list[genpy.Time | str | Transform]]
    _futs: dict[str, dict[int, asyncio.Future]]
    _tf_subscriber: Subscriber

    def __init__(
        self,
        node_handle: NodeHandle,
        history_length: genpy.Duration = genpy.Duration(10),
    ):
        self._node_handle = node_handle
        self._history_length = history_length

        self._id_counter = itertools.count()

        self._tfs = {}  # child_frame_id -> sorted list of (time, frame_id, Transform)
        self._futs: dict[
            str, dict[int, asyncio.Future]
        ] = {}  # child_frame_id -> dict of futures to call when new tf is received

        self._tf_subscriber = self._node_handle.subscribe(
            "/tf", TFMessage, self._got_tfs
        )

    async def setup(self):
        await self._tf_subscriber.setup()

    async def shutdown(self) -> None:
        """
        Shuts the transform listener down.
        """
        await self._tf_subscriber.shutdown()

    def _got_tfs(self, msg: TFMessage):
        for transform in msg.transforms:
            frame_id = _make_absolute(transform.header.frame_id)
            child_frame_id = _make_absolute(transform.child_frame_id)

            l = self._tfs.setdefault(child_frame_id, [])

            if l and transform.header.stamp < l[-1][0]:
                del l[:]

            l.append(
                (
                    transform.header.stamp,
                    frame_id,
                    Transform.from_Transform_message(transform.transform),
                )
            )

            if l[0][0] + self._history_length * 2 <= transform.header.stamp:
                pos = 0
                while l[pos][0] + self._history_length <= transform.header.stamp:
                    pos += 1
                del l[:pos]

        for transform in msg.transforms:
            frame_id = _make_absolute(transform.header.frame_id)

            futs = self._futs.pop(frame_id, {})
            for fut in futs.values():
                fut.set_result(None)

    def _wait_for_new(self, child_frame_id) -> tuple[asyncio.Future, int]:
        id_ = next(self._id_counter)

        fut = asyncio.Future()
        self._futs.setdefault(child_frame_id, {})[id_] = fut
        return fut, id_

    async def get_transform(
        self, to_frame: str, from_frame: str, time: genpy.Time | None = None
    ):
        to_frame = _make_absolute(to_frame)
        from_frame = _make_absolute(from_frame)
        assert time is None or isinstance(time, genpy.Time)

        to_tfs = {to_frame: Transform.identity()}  # x -> Transform from to_frame to x
        to_pos = to_frame
        from_tfs = {
            from_frame: Transform.identity()
        }  # x -> Transform from from_frame to x
        from_pos = from_frame

        while True:
            while True:
                try:
                    new_to_pos, t = self._interpolate(self._tfs.get(to_pos, []), time)
                except _TooFutureError:
                    break
                except TooPastError:
                    raise
                else:
                    assert new_to_pos not in to_tfs
                    to_tfs[new_to_pos] = t * to_tfs[to_pos]

                    if new_to_pos in from_tfs:
                        return to_tfs[new_to_pos].inverse() * from_tfs[new_to_pos]

                    to_pos = new_to_pos

            while True:
                try:
                    new_from_pos, t = self._interpolate(
                        self._tfs.get(from_pos, []), time
                    )
                except _TooFutureError:
                    break
                except TooPastError:
                    raise
                else:
                    assert new_from_pos not in from_tfs
                    from_tfs[new_from_pos] = t * from_tfs[from_pos]

                    if new_from_pos in to_tfs:
                        return to_tfs[new_from_pos].inverse() * from_tfs[new_from_pos]

                    from_pos = new_from_pos

            to_pos_fut, to_pos_id = self._wait_for_new(to_pos)
            from_pos_fut, from_pos_id = self._wait_for_new(from_pos)
            lst = [to_pos_fut, from_pos_fut]
            try:
                await asyncio.wait(lst, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for fut in lst:
                    fut.cancel()
                if to_pos in self._futs:
                    self._futs[to_pos].pop(to_pos_id)
                    if not self._futs[to_pos]:
                        self._futs.pop(to_pos)
                if from_pos in self._futs:
                    self._futs[from_pos].pop(from_pos_id)
                    if not self._futs[from_pos]:
                        self._futs.pop(from_pos)

    def _interpolate(
        self,
        sorted_list: list[tuple[genpy.Time, str, Transform]],
        time: genpy.Time | None,
    ) -> tuple[str, genpy.Time]:
        if time is None:
            if sorted_list:
                return sorted_list[-1][1], sorted_list[-1][2]
            else:
                raise _TooFutureError()

        if not sorted_list or time > sorted_list[-1][0]:
            raise _TooFutureError()
        if time < sorted_list[0][0]:
            raise TooPastError()

        pos = bisect.bisect_left(sorted_list, (time,))

        left = sorted_list[0 if pos == 0 else pos - 1]
        right = sorted_list[pos]

        assert left[0] <= time
        assert right[0] >= time

        x = (time - left[0]).to_sec() / (right[0] - left[0]).to_sec()

        return left[1], left[2] + x * (right[2] - left[2])


class _TooFutureError(Exception):

    """This is an internal exception; it should never escape to the user"""


class TooPastError(Exception):
    """
    User asked for a transform that will never known because it's from before
    the start of the history buffer.

    Inherits from :class:`Exception`.
    """


class TransformBroadcaster:
    """
    Broadasts transforms onto a topic.
    """

    def __init__(self, node_handle: NodeHandle):
        """
        Args:
            node_handle (NodeHandle): The node handle used to communicate with the
                ROS master server.
        """
        self._node_handle = node_handle
        self._tf_publisher = self._node_handle.advertise("/tf", TFMessage)

    async def setup(self) -> None:
        await self._tf_publisher.setup()

    def send_transform(self, transform: TransformStamped):
        """
        Sends a stamped Transform message onto the publisher.

        Args:
            transform (TransformStamped): The transform to publish onto the topic.

        Raises:
            TypeError: The transform class was not of the TransformStamped class.
        """
        if not isinstance(transform, TransformStamped):
            raise TypeError("expected TransformStamped")
        self._tf_publisher.publish(
            TFMessage(
                transforms=[transform],
            )
        )
