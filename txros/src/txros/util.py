from __future__ import division

import traceback

import numpy
from twisted.internet import threads, defer, reactor
from twisted.python import failure

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

def deferred_has_been_called(df):
    still_running = True
    res2 = []
    def cb(res):
        if still_running:
            res2[:] = [res]
        else:
            return res
    df.addBoth(cb)
    still_running = False
    if res2:
        return True, res2[0]
    return False, None
def inlineCallbacks(f):
    from functools import wraps
    @wraps(f)
    def _(*args, **kwargs):
        gen = f(*args, **kwargs)
        stop_running = [False]
        def cancelled(df_):
            assert df_ is df
            stop_running[0] = True
            if currently_waiting_on:
                currently_waiting_on[0].cancel()
                try:
                    gen.throw(GeneratorExit) # GC will eventually get it, but move things along...
                except GeneratorExit:
                    pass
                except:
                    traceback.print_exc()
        df = defer.Deferred(cancelled)
        currently_waiting_on = []
        def it(cur):
            while True:
                try:
                    if isinstance(cur, failure.Failure):
                        res = cur.throwExceptionIntoGenerator(gen) # external code is run here
                    else:
                        res = gen.send(cur) # external code is run here
                    if stop_running[0]:
                        return
                except StopIteration:
                    df.callback(None)
                except defer._DefGen_Return as e:
                    # XXX should make sure direct child threw
                    df.callback(e.value)
                except:
                    df.errback()
                else:
                    if isinstance(res, defer.Deferred):
                        called, res2 = deferred_has_been_called(res)
                        if called:
                            cur = res2
                            continue
                        else:
                            currently_waiting_on[:] = [res]
                            def gotResult(res2):
                                assert currently_waiting_on[0] is res
                                currently_waiting_on[:] = []
                                if stop_running[0]:
                                    return
                                it(res2)
                            res.addBoth(gotResult) # external code is run between this and gotResult
                    else:
                        cur = res
                        continue
                break
        it(None)
        return df
    return _