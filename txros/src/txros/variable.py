from __future__ import annotations
import itertools
import weakref
from typing import Any, Optional, Callable, List, Dict

from twisted.internet import defer, reactor
from twisted.python import failure, log


class Event:
    """
    Represents an event in the txROS suite. Commonly used for monitoring changes
    in the value of specific objects.

    Attributes:
        observers (Dict[int, Callable[[Any], None]]): The totality of observers
            attached to the given event.
        id_generator (itertools.count[int]): A count generator responsible for generating
            unique new IDs for each observer added.
    """
    def __init__(self):
        self.observers: Dict[int, Callable[[Any], None]] = {}
        self.id_generator = itertools.count()
        self._once = None
        self.times = 0

    def run_and_watch(self, func: Callable[[Any], None]):
        func()
        return self.watch(func)

    def watch_weakref(self, obj: Any, func: Callable[[Any], None]):
        # func must not contain a reference to obj!
        watch_id = self.watch(lambda *args: func(obj_ref(), *args))
        obj_ref = weakref.ref(obj, lambda _: self.unwatch(watch_id))

    def watch(self, func: Callable[[Any], None]) -> int:
        """
        Adds an observer with the given callable.

        Args:
            function (Callable[[Any], None]): The function observer to add.
        """
        id = next(self.id_generator)
        self.observers[id] = func
        return id

    def unwatch(self, id: int) -> None:
        """
        Removes the observer with the given ID.

        Args:
            id (int): The ID of the observer to remove.
        """
        self.observers.pop(id)

    @property
    def once(self) -> Optional[Event]:
        res = self._once
        if res is None:
            res = self._once = Event()
        return res

    def happened(self, *event: Any) -> None:
        self.times += 1

        once, self._once = self._once, None

        for id, func in sorted(self.observers.items()):
            try:
                func(*event)
            except:
                log.err(None, "Error while processing Event callbacks:")

        if once is not None:
            once.happened(*event)

    def get_deferred(self, timeout: Optional[float] = None) -> defer.Deferred:
        once = self.once
        df = defer.Deferred()
        id1 = once.watch(lambda *event: df.callback(event))
        if timeout is not None:

            def do_timeout():
                df.errback(failure.Failure(defer.TimeoutError("in Event.get_deferred")))
                once.unwatch(id1)
                once.unwatch(x)

            delay = reactor.callLater(timeout, do_timeout)
            x = once.watch(lambda *event: delay.cancel())
        return df


class Variable:
    def __init__(self, value: Any):
        self.value = value
        self.changed = Event()
        self.transitioned = Event()

    def set(self, value: Any):
        if value == self.value:
            return

        oldvalue = self.value
        self.value = value
        self.changed.happened(value)
        self.transitioned.happened(oldvalue, value)

    def when_satisfies(self, func) -> Event:
        res = Event()

        def _(value):
            if func(value):
                res.happened(value)

        self.changed.watch(_)
        return res

    @defer.inlineCallbacks
    def get_when_satisfies(self, func) -> defer.Deferred:
        """
        Returns the value of the variable when the value passed into a callable
        returns true.

        Args:
            func (Callable[[Any], bool]): The check function.

        Returns:
            defer.Deferred: The defer object containing the return value.
        """
        while True:
            if func(self.value):
                defer.returnValue(self.value)
            yield self.changed.once.get_deferred()

    def get_not_none(self) -> defer.Deferred:
        """
        Returns the value of the variable when it is not None through a Deferred.

        Returns:
            defer.Deferred: The defer object containing the return value.
        """
        return self.get_when_satisfies(lambda val: val is not None)
