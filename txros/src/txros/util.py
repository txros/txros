from __future__ import division

import numpy
from twisted.internet import threads, defer, reactor

import rospy

xyzw_array = lambda o: numpy.array([o.x, o.y, o.z, o.w])
xy_array = lambda o: numpy.array([o.x, o.y])

class TopicReader(object):
    def __init__(self, topic_name, topic_type):
        self._sub = rospy.Subscriber(topic_name, topic_type,
            lambda msg: reactor.callFromThread(self._cb, msg))
        self._dfs = []
    
    def _cb(self, msg):
        for df in self._dfs:
            reactor.callLater(0, df.callback, msg)
        self._dfs = []
    
    def get_next_message(self):
        df = defer.Deferred()
        self._dfs.append(df)
        return df

def wrap_blocking_func(f):
    def _(*args, **kwargs):
        return threads.deferToThread(f, *args, **kwargs)
    return _

def sleep(t):
    d = defer.Deferred(canceller=lambda d_: dc.cancel())
    dc = reactor.callLater(t, d.callback, None)
    return d
