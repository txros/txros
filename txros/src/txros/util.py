from __future__ import division

import sys
import traceback
import types

from twisted.internet import defer, reactor, protocol, stdio
from twisted.python import failure
from twisted.protocols import basic
from typing import Generator, Callable

import genpy


def wall_sleep(duration: genpy.Duration) -> defer.Deferred:
    """
    Sleeps for a specified duration using a Deferred object.

    Args:
        duration (genpy.Duration): The amount of time to sleep for.

    Returns:
        defer.Deferred: The deferred object which sleeps for the specified amount
            of time.
    """
    if isinstance(duration, genpy.Duration):
        duration = duration.to_sec()
    elif not isinstance(duration, (float, int)):
        raise TypeError("expected float or genpy.Duration")
    df = defer.Deferred(canceller=lambda df_: dc.cancel())
    dc = reactor.callLater(duration, df.callback, None)
    return df


def sleep(duration):
    # printing rather than using DeprecationWarning because DeprecationWarning
    # is disabled by default, and that's useless.
    print("txros.util.sleep is deprecated! use txros.util.wall_sleep instead.")

    return wall_sleep(duration)


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
        raise TypeError("not cancellable")


class InlineCallbacksCancelled(BaseException):
    def __str__(self):
        return "InlineCallbacksCancelled()"

    __repr__ = __str__


def _step(cur, gen, currently_waiting_on, mine, df: defer.Deferred, has_been_cancelled: bool):
    if currently_waiting_on[0] is not mine:
        # print 'result', repr(cur), 'ignored'
        return
    currently_waiting_on[0] = None
    while True:
        try:
            if isinstance(cur, failure.Failure):
                res = cur.throwExceptionIntoGenerator(gen)  # external code is run here
            else:
                res = gen.send(cur)  # external code is run here
        except StopIteration:
            if not has_been_cancelled:
                df.callback(None)
            else:
                df.errback(
                    TypeError(
                        "InlineCallbacksCancelled exception was converted to non-exception"
                    )
                )
        except InlineCallbacksCancelled:
            if not has_been_cancelled:
                df.errback(
                    TypeError(
                        "InlineCallbacksCancelled exception resulted unexpectedly"
                    )
                )
            else:
                df.callback(None)
        except defer._DefGen_Return as e:
            # below implementation copied from twisted.internet.defer

            # returnValue() was called; time to give a result to the original
            # Deferred.  First though, let's try to identify the potentially
            # confusing situation which results when returnValue() is
            # accidentally invoked from a different function, one that wasn't
            # decorated with @inlineCallbacks.

            # The traceback starts in this frame (the one for
            # _inlineCallbacks); the next one down should be the application
            # code.
            appCodeTrace = sys.exc_info()[2].tb_next
            if isinstance(cur, failure.Failure):
                # If we invoked this generator frame by throwing an exception
                # into it, then throwExceptionIntoGenerator will consume an
                # additional stack frame itself, so we need to skip that too.
                appCodeTrace = appCodeTrace.tb_next
            # Now that we've identified the frame being exited by the
            # exception, let's figure out if returnValue was called from it
            # directly.  returnValue itself consumes a stack frame, so the
            # application code will have a tb_next, but it will *not* have a
            # second tb_next.
            if appCodeTrace.tb_next.tb_next:
                # If returnValue was invoked non-local to the frame which it is
                # exiting, identify the frame that ultimately invoked
                # returnValue so that we can warn the user, as this behavior is
                # confusing.
                ultimateTrace = appCodeTrace
                while ultimateTrace.tb_next.tb_next:
                    ultimateTrace = ultimateTrace.tb_next
                raise TypeError(
                    "returnValue() in %r would cause %r to exit: "
                    "returnValue should only be invoked by functions decorated "
                    "with inlineCallbacks"
                    % (
                        ultimateTrace.tb_frame.f_code.co_name,
                        appCodeTrace.tb_frame.f_code.co_name,
                    )
                )

            # end copying

            if not has_been_cancelled:
                df.callback(e.value)
            else:
                df.errback(
                    TypeError(
                        "InlineCallbacksCancelled exception was converted to non-exception"
                    )
                )
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
                    res.addBoth(
                        _step,
                        gen,
                        currently_waiting_on,
                        currently_waiting_on[0],
                        df,
                        has_been_cancelled,
                    )  # external code is run between this and gotResult
            else:
                cur = res
                continue
        break


def cancellableInlineCallbacks(f: Callable[[], Generator]):
    from functools import wraps

    @wraps(f)
    def runner(*args, **kwargs) -> DeferredCancelDeferred:
        # another direct copy from twisted.internet.defer; however, in this case, I wrote this
        try:
            gen = f(*args, **kwargs)
        except defer._DefGen_Return:
            raise TypeError(
                "inlineCallbacks requires %r to produce a generator; instead "
                "caught returnValue being used in a non-generator" % (f,)
            )
        if not isinstance(gen, types.GeneratorType):
            raise TypeError(
                "inlineCallbacks requires %r to produce a generator; "
                "instead got %r" % (f, gen)
            )
        # end copy

        def cancelled(_: defer.Deferred):
            """
            Serves as the canceller for the inline cancellable Deferred object.
            """
            # assert df_ is df
            assert currently_waiting_on[0] is not None
            x = currently_waiting_on[0]
            currently_waiting_on[0] = None
            cancel_result = x.cancel()  # make optional?

            if isinstance(cancel_result, defer.Deferred):

                @cancel_result.addBoth
                def _(res) -> UncancellableDeferred:
                    if isinstance(res, failure.Failure):
                        print(res)
                    df2 = UncancellableDeferred()
                    _step(
                        failure.Failure(InlineCallbacksCancelled()),
                        gen,
                        currently_waiting_on,
                        currently_waiting_on[0],
                        df2,
                        True,
                    )
                    return df2

            else:
                df2 = UncancellableDeferred()
                _step(
                    failure.Failure(InlineCallbacksCancelled()),
                    gen,
                    currently_waiting_on,
                    currently_waiting_on[0],
                    df2,
                    True,
                )
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
        delimiter = "\n"

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


class TimeoutError(Exception):
    pass


@cancellableInlineCallbacks
def wrap_timeout(df, duration, cancel_df_on_timeout=True):
    timeout = wall_sleep(duration)

    try:
        result, index = yield defer.DeferredList(
            [df, timeout], fireOnOneCallback=True, fireOnOneErrback=True
        )
    finally:
        if cancel_df_on_timeout:
            yield df.cancel()
            df.addErrback(lambda fail: fail.trap(defer.CancelledError))
        yield timeout.cancel()
        timeout.addErrback(lambda fail: fail.trap(defer.CancelledError))

    if index == 1:
        raise TimeoutError()
    else:
        defer.returnValue(result)


@cancellableInlineCallbacks
def wrap_time_notice(df, duration, description):
    """print description if df is taking more than duration to complete"""

    try:
        defer.returnValue((yield wrap_timeout(df, duration, False)))
    except TimeoutError:
        print("%s is taking a while..." % (description[0].upper() + description[1:],))
        res = yield df
        print("...%s succeeded." % (description,))
        defer.returnValue(res)


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
