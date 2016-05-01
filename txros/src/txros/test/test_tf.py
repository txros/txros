from __future__ import division

import traceback

import numpy
from twisted.internet import defer
from twisted.trial import unittest

import genpy
from geometry_msgs.msg import Transform, TransformStamped, Quaternion, Vector3
from std_msgs.msg import Header
from tf import transformations

from txros import NodeHandle, tf, util
from txros.test import util as test_util


class Test(unittest.TestCase):
    @defer.inlineCallbacks
    def test_tf(self):
        @defer.inlineCallbacks
        def f(nh):
            tf_broadcaster = tf.TransformBroadcaster(nh)
            @apply
            @util.cancellableInlineCallbacks
            def publish_thread():
                try:
                    while True:
                        t = nh.get_time()
                        tf_broadcaster.send_transform(TransformStamped(
                            header=Header(
                                stamp=t,
                                frame_id='/parent',
                            ),
                            child_frame_id='/child',
                            transform=Transform(
                                translation=Vector3(*[1, 2, 3]),
                                rotation=Quaternion(*transformations.quaternion_about_axis(t.to_sec(), [0, 0, 1])),
                            ),
                        ))
                        yield nh.sleep(0.1)
                except Exception:
                    traceback.print_exc()
            try:
                tf_listener = tf.TransformListener(nh, history_length=genpy.Duration(1)) # short history length so that we cover history being truncated

                try:
                    yield tf_listener.get_transform('/parent', '/child', nh.get_time() - genpy.Duration(100))
                except tf.TooPastError:
                    pass
                else:
                    self.fail('expected TooPastError')

                start_time = nh.get_time()
                for i in xrange(200):
                    time = start_time + genpy.Duration.from_sec(1 + i/100)
                    dt = 1e-3
                    transform = yield tf_listener.get_transform('/parent', '/child', time)
                    transform2 = yield tf_listener.get_transform('/parent', '/child', time + genpy.Duration.from_sec(1e-3))
                    assert numpy.allclose((transform2 - transform)/dt, [0, 0, 0, 0, 0, 1])
            finally:
                yield publish_thread.cancel()
                publish_thread.addErrback(lambda fail: fail.trap(defer.CancelledError))
        yield test_util.call_with_nodehandle(f)
