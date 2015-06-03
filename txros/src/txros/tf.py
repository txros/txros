from __future__ import absolute_import
from __future__ import division

import math
import numpy
import bisect
import itertools

from twisted.internet import defer

import genpy
from tf2_msgs.msg import TFMessage
from tf import transformations # XXX

from txros import util

def _sinc(x):
    return numpy.sinc(x/numpy.pi)

def _quat_from_rotvec(r):
    r = numpy.array(r)
    angle = numpy.linalg.norm(r)
    return numpy.concatenate([
        _sinc(angle/2)/2 * r, # = sin(angle/2) * normalized(r), without the singularity
        [math.cos(angle/2)],
    ])

def _rotvec_from_quat(q):
    q = transformations.unit_vector(q)
    if q[3] < 0: q = -q
    return 2/_sinc(math.acos(min(1, q[3]))) * q[:3]


class Transform(object):
    @classmethod
    def identity(cls):
        return cls([0, 0, 0], [0, 0, 0, 1])
    
    @classmethod
    def from_Transform_message(cls, msg):
        return cls([msg.translation.x, msg.translation.y, msg.translation.z],
            [msg.rotation.x, msg.rotation.y, msg.rotation.z, msg.rotation.w])
    
    def __init__(self, p, q):
        self._p = numpy.array(p)
        self._q = numpy.array(q)
    
    def __sub__(self, other):
        return numpy.concatenate([
            self._p - other._p,
            _rotvec_from_quat(transformations.quaternion_multiply(
                self._q,
                transformations.quaternion_inverse(other._q),
            )),
        ])
    def __add__(self, other):
        return Transform(
            self._p + other[:3],
            transformations.quaternion_multiply(
                _quat_from_rotvec(other[3:]),
                self._q,
            ),
        )
    
    def __mul__(self, other):
        return Transform(
            self._p + transformations.quaternion_matrix(self._q)[:3, :3].dot(other._p),
            transformations.quaternion_multiply(self._q, other._q),
        )
    
    def inverse(self):
        return Transform(
            -transformations.quaternion_matrix(self._q)[:3, :3].T.dot(self._p),
            transformations.quaternion_inverse(self._q),
        )
    
    def as_matrix(self):
        return transformations.translation_matrix(self._p).dot(transformations.quaternion_matrix(self._q))
    
    def __str__(self):
        return 'Transform(%r, %r)' % (self._p, self._q)
    __repr__ = __str__
    
    def transform_point(self, point):
        return self._p + transformations.quaternion_matrix(self._q)[:3, :3].dot(point)
    
    def transform_vector(self, vector):
        return transformations.quaternion_matrix(self._q)[:3, :3].dot(vector)
    
    def transform_quaternion(self, quaternion):
        return transformations.quaternion_multiply(self._q, quaternion)

class TransformListener(object):
    def __init__(self, node_handle, history_length=genpy.Duration(10)):
        self._node_handle = node_handle
        self._history_length = history_length
        
        self._id_counter = itertools.count()
        
        self._tfs = {} # child_frame_id -> sorted list of (time, frame_id, Transform)
        self._dfs = {} # child_frame_id -> dict of deferreds to call when new tf is received
        
        self._tf_subscriber = self._node_handle.subscribe('/tf', TFMessage, self._got_tfs)
    
    def shutdown(self):
        self._tf_subscriber.shutdown()
    
    def _got_tfs(self, msg):
        def make_absolute(frame_id):
            if not frame_id.startswith('/'):
                return '/' + frame_id
            return frame_id
        
        for transform in msg.transforms:
            frame_id = make_absolute(transform.header.frame_id)
            child_frame_id = make_absolute(transform.child_frame_id)
            
            l = self._tfs.setdefault(child_frame_id, [])
            
            if l and transform.header.stamp < l[-1][0]:
                print child_frame_id, "frame's time decreased!"
                del l[:]
            
            l.append((transform.header.stamp, frame_id, Transform.from_Transform_message(transform.transform)))
            
            if l[0][0] <= transform.header.stamp - self._history_length*2:
                pos = 0
                while l[pos][0] <= transform.header.stamp - self._history_length:
                    pos += 1
                del l[:pos]
        
        for transform in msg.transforms:
            frame_id = make_absolute(transform.header.frame_id)
            
            dfs = self._dfs.pop(frame_id, {})
            for df in dfs.itervalues():
                df.callback(None)
    
    def _wait_for_new(self, child_frame_id):
        id_ = self._id_counter.next()
        def cancel(df_):
            self._dfs[child_frame_id].pop(id_)
            if not self._dfs[child_frame_id]:
                self._dfs.pop(child_frame_id)
        df = defer.Deferred(cancel)
        self._dfs.setdefault(child_frame_id, {})[id_] = df
        return df
    
    @util.cancellableInlineCallbacks
    def get_transform(self, to_frame, from_frame, time=None):
        assert time is None or isinstance(time, genpy.Time)
        
        to_tfs = {to_frame: Transform.identity()} # x -> Transform from to_frame to x
        to_pos = to_frame
        from_tfs = {from_frame: Transform.identity()} # x -> Transform from from_frame to x
        from_pos = from_frame
        
        while True:
            while True:
                try:
                    new_to_pos, t = self._interpolate(self._tfs.get(to_pos, []), time)
                except TooFutureError:
                    break
                except TooPastError:
                    raise
                else:
                    assert new_to_pos not in to_tfs
                    to_tfs[new_to_pos] = t * to_tfs[to_pos]
                    
                    if new_to_pos in from_tfs:
                        defer.returnValue(to_tfs[new_to_pos].inverse() * from_tfs[new_to_pos])
                    
                    to_pos = new_to_pos
            
            while True:
                try:
                    new_from_pos, t = self._interpolate(self._tfs.get(from_pos, []), time)
                except TooFutureError:
                    break
                except TooPastError:
                    raise
                else:
                    assert new_from_pos not in from_tfs
                    from_tfs[new_from_pos] = t * from_tfs[from_pos]
                    
                    if new_from_pos in to_tfs:
                        defer.returnValue(to_tfs[new_from_pos].inverse() * from_tfs[new_from_pos])
                    
                    from_pos = new_from_pos
            
            lst = [
                self._wait_for_new(to_pos),
                self._wait_for_new(from_pos),
            ]
            try:
                yield defer.DeferredList(lst, fireOnOneCallback=True, fireOnOneErrback=True)
            finally:
                for df in lst: df.cancel()
                for df in lst: df.addErrback(lambda fail: fail.trap(defer.CancelledError))
    
    def _interpolate(self, sorted_list, time):
        if time is None:
            if sorted_list:
                return sorted_list[-1][1], sorted_list[-1][2]
            else:
                raise TooFutureError()
        
        if not sorted_list or time > sorted_list[-1][0]:
            raise TooFutureError()
        if time < sorted_list[0][0]:
            raise TooPastError()
        
        pos = bisect.bisect_left(sorted_list, (time,))
        
        left = sorted_list[0 if pos == 0 else pos-1]
        right = sorted_list[pos]
        
        assert left[0] <= time
        assert right[0] >= time
        
        x = (time - left[0]).to_sec()/(right[0] - left[0]).to_sec()
        
        if left[1] != right[1]:
            raise TransformError('parent frame changed around this time')
        
        return left[1], left[2] + x*(right[2] - left[2])

class TooFutureError(Exception): # XXX rename
    pass

class TooPastError(Exception): # XXX rename
    pass
