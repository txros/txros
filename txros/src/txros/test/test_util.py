from __future__ import division

from twisted.internet import defer, reactor
from twisted.trial import unittest

from txros import util


class Test(unittest.TestCase):
    @defer.inlineCallbacks
    def test_wall_sleep(self):
        t1 = reactor.seconds()
        yield util.wall_sleep(2.)
        t2 = reactor.seconds()
        
        assert 1 <= t2 - t1 <= 3
    
    @defer.inlineCallbacks
    def test_wrap_timeout1(self):
        try:
            @util.cancellableInlineCallbacks
            def f():
                yield util.wall_sleep(1)
                defer.returnValue('retval')
            res = yield util.wrap_timeout(f(), 3)
        except util.TimeoutError:
            assert False
        else:
            assert res == 'retval'
    
    @defer.inlineCallbacks
    def test_wrap_timeout2(self):
        try:
            @util.cancellableInlineCallbacks
            def f():
                yield util.wall_sleep(3)
                defer.returnValue('retval')
            res = yield util.wrap_timeout(f(), 1)
        except util.TimeoutError:
            pass
        else:
            assert False
