from __future__ import division

from twisted.internet import defer
from twisted.trial import unittest

from txros import NodeHandle
from txros.test import util as test_util


class Test(unittest.TestCase):
    @defer.inlineCallbacks
    def test_creation(self):
        yield test_util.call_with_nodehandle(lambda nh: defer.succeed(None))
