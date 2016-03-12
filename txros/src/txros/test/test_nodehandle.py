from __future__ import division

from twisted.internet import defer
from twisted.trial import unittest

from txros import NodeHandle
from txros.test import util as test_util


class Test(unittest.TestCase):
    @defer.inlineCallbacks
    def test_creation(self):
        yield test_util.call_with_nodehandle(lambda nh: defer.succeed(None))
    
    @defer.inlineCallbacks
    def test_params(self):
        @defer.inlineCallbacks
        def f(nh):
            k = '/my_param'
            v = ['hi', 2]
            
            assert not (yield nh.has_param(k))
            yield nh.set_param(k, v)
            assert (yield nh.has_param(k))
            assert (yield nh.get_param(k)) == v
            yield nh.delete_param(k)
            assert not (yield nh.has_param(k))
        yield test_util.call_with_nodehandle(f)
    
    @defer.inlineCallbacks
    def test_advertise(self):
        @defer.inlineCallbacks
        def f(nh):
            from std_msgs.msg import Int32
            pub = nh.advertise('/my_topic', Int32, latching=True)
            pub.publish(Int32(42))
            sub = nh.subscribe('/my_topic', Int32)
            yield sub.get_next_message()
            assert sub.get_last_message().data == 42
        yield test_util.call_with_nodehandle(f)
