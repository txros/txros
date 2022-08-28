import asyncio
import time

from twisted.internet import defer, reactor
import unittest

from txros import util


class Test(unittest.IsolatedAsyncioTestCase):
    async def test_wall_sleep(self):
        t1 = time.time()
        await util.wall_sleep(2.0)
        t2 = time.time()

        assert 1 <= t2 - t1 <= 3

    async def test_wrap_timeout1(self):
        try:

            async def f():
                await util.wall_sleep(1)
                return "retval"

            await util.wrap_timeout(f(), 3)
        except asyncio.TimeoutError:
            assert False
        else:
            assert True

    async def test_wrap_timeout2(self):
        try:

            async def f():
                await util.wall_sleep(3)
                return "retval"

            await util.wrap_timeout(f(), 1)
        except asyncio.TimeoutError:
            pass
        else:
            assert False
