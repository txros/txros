from __future__ import division

import traceback

from twisted.internet import defer, reactor, protocol, stdio
from twisted.python import failure
from twisted.protocols import basic

def sleep(t):
    d = defer.Deferred(canceller=lambda d_: dc.cancel())
    dc = reactor.callLater(t, d.callback, None)
    return d

def branch_deferred(df, canceller=None):
    branched_df = defer.Deferred(canceller)
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

class DeferredCancelDeferred(defer.Deferred):
    def cancel(self):
        if not self.called:
            self.errback(failure.Failure(defer.CancelledError()))
            
            return self._canceller(self)
        elif isinstance(self.result, defer.Deferred):
            # Waiting for another deferred -- cancel it instead.
            return self.result.cancel()

class UncancellableDeferred(defer.Deferred):
    def cancel(self):
        raise TypeError('not cancellable')

class InlineCallbacksCancelled(BaseException):
    def __str__(self):
        return 'InlineCallbacksCancelled()'
    __repr__ = __str__
def _step(cur, gen, currently_waiting_on, mine, df, has_been_cancelled):
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
            if not has_been_cancelled:
                df.callback(None)
            else:
                df.errback(TypeError('InlineCallbacksCancelled exception was converted to non-exception'))
        except InlineCallbacksCancelled:
            if not has_been_cancelled:
                df.errback(TypeError('InlineCallbacksCancelled exception resulted unexpectedly'))
            else:
                df.callback(None)
        except defer._DefGen_Return as e:
            # XXX should make sure direct child threw
            if not has_been_cancelled:
                df.callback(e.value)
            else:
                df.errback(TypeError('InlineCallbacksCancelled exception was converted to non-exception'))
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
                    res.addBoth(_step, gen, currently_waiting_on, currently_waiting_on[0], df, has_been_cancelled) # external code is run between this and gotResult
            else:
                cur = res
                continue
        break
def cancellableInlineCallbacks(f):
    from functools import wraps
    @wraps(f)
    def runner(*args, **kwargs):
        gen = f(*args, **kwargs)
        def cancelled(df_):
            #assert df_ is df
            assert currently_waiting_on[0] is not None
            x = currently_waiting_on[0]
            currently_waiting_on[0] = None
            cancel_result = x.cancel() # make optional?
            
            if isinstance(cancel_result, defer.Deferred):
                @cancel_result.addBoth
                def _(res):
                    if isinstance(res, failure.Failure):
                        print res
                    df2 = UncancellableDeferred()
                    _step(failure.Failure(InlineCallbacksCancelled()), gen, currently_waiting_on, currently_waiting_on[0], df2, True)
                    return df2
            else:
                df2 = UncancellableDeferred()
                _step(failure.Failure(InlineCallbacksCancelled()), gen, currently_waiting_on, currently_waiting_on[0], df2, True)
                return df2
        df = DeferredCancelDeferred(cancelled)
        currently_waiting_on = [None]
        _step(None, gen, currently_waiting_on, currently_waiting_on[0], df, False)
        return df
    return runner

class AutoServerFactory(protocol.ServerFactory):
    def __init__(self, func):
        self._func = func
    
    def buildProtocol(self, addr):
        p = self._func(addr)
        if p is not None:
            p.factory = self
        return p


@cancellableInlineCallbacks
def nonblocking_raw_input(prompt):
    class P(basic.LineOnlyReceiver):
        delimiter = '\n'
    
        def __init__(self, prompt):
            self._prompt = prompt
            self.df = defer.Deferred()
    
        def connectionMade(self):
            self.transport.write(self._prompt)
    
        def lineReceived(self, line):
            self.df.callback(line)
            self.transport.loseConnection()
    p = P(prompt)
    f = stdio.StandardIO(p)
    try:
        res = yield p.df
        defer.returnValue(res)
    finally:
        f.loseConnection()

class TimeoutError(Exception): pass
@cancellableInlineCallbacks
def wrap_timeout(df, duration):
    timeout = sleep(duration)

    try:
        result, index = yield defer.DeferredList([df, timeout], fireOnOneCallback=True, fireOnOneErrback=True)
    finally:
        yield df.cancel()
        yield timeout.cancel()
        df.addErrback(lambda fail: fail.trap(defer.CancelledError))
        timeout.addErrback(lambda fail: fail.trap(defer.CancelledError))

    if index == 1:
        raise TimeoutError()
    else:
        defer.returnValue(result)


def launch_main(main_func):
    @defer.inlineCallbacks
    def _():
        try:
            yield main_func()
        except Exception:
            traceback.print_exc()
        reactor.stop()
    reactor.callWhenRunning(_)
    reactor.run()
