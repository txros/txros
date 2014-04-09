from __future__ import division

from twisted.internet import defer, reactor, protocol
from twisted.python import failure

def sleep(t):
    d = defer.Deferred(canceller=lambda d_: dc.cancel())
    dc = reactor.callLater(t, d.callback, None)
    return d

def branch_deferred(df):
    branched_df = defer.Deferred()
    def cb(result):
        branched_df.callback(result)
        return result
    df.addBoth(cb)
    return branched_df

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

class InlineCallbacksCancelled(BaseException):
    def __str__(self):
        return 'InlineCallbacksCancelled()'
    __repr__ = __str__
def _step(cur, gen, currently_waiting_on, mine, df):
    if currently_waiting_on[0] is not mine:
        #print 'result', repr(cur), 'ignored'
        return
    currently_waiting_on[0] = None
    while True:
        try:
            if isinstance(cur, failure.Failure):
                res = cur.throwExceptionIntoGenerator(gen) # external code is run here
            else:
                res = gen.send(cur) # external code is run here
        except StopIteration:
            df.callback(None)
        except InlineCallbacksCancelled:
            pass
        except defer._DefGen_Return as e:
            # XXX should make sure direct child threw
            df.callback(e.value)
        except BaseException as e:
            df.errback()
        else:
            if isinstance(res, defer.Deferred):
                called, res2 = deferred_has_been_called(res)
                if called:
                    cur = res2
                    continue
                else:
                    currently_waiting_on[0] = res
                    res.addBoth(_step, gen, currently_waiting_on, currently_waiting_on[0], df) # external code is run between this and gotResult
            else:
                cur = res
                continue
        break
def cancellableInlineCallbacks(f):
    from functools import wraps
    @wraps(f)
    def _(*args, **kwargs):
        gen = f(*args, **kwargs)
        def cancelled(df_):
            #assert df_ is df
            assert currently_waiting_on[0] is not None
            x = currently_waiting_on[0]
            currently_waiting_on[0] = None
            x.cancel() # make optional?
            _step(failure.Failure(InlineCallbacksCancelled()), gen, currently_waiting_on, currently_waiting_on[0], None)
        df = defer.Deferred(cancelled)
        currently_waiting_on = [None]
        _step(None, gen, currently_waiting_on, currently_waiting_on[0], df)
        return df
    return _

class AutoServerFactory(protocol.ServerFactory):
    def __init__(self, func):
        self._func = func
    
    def buildProtocol(self, addr):
        p = self._func(addr)
        if p is not None:
            p.factory = self
        return p
