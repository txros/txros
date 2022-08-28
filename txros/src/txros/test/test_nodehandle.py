from __future__ import annotations

import asyncio
import unittest
from typing import TYPE_CHECKING

from txros import util
from txros.test import util as test_util

if TYPE_CHECKING:
    from txros.nodehandle import NodeHandle
    from txros.service import Service


class Test(unittest.IsolatedAsyncioTestCase):
    async def test_creation(self):
        def succeed(nh: NodeHandle):
            fut = asyncio.Future()
            fut.set_result(None)
            fut.done()
            return fut

        await test_util.call_with_nodehandle(lambda nh: succeed(nh))

    async def test_params(self):
        async def f(nh):
            k = "/my_param"
            v = ["hi", 2]

            assert not (await nh.has_param(k))
            await nh.set_param(k, v)
            assert await nh.has_param(k)
            assert (await nh.get_param(k)) == v
            await nh.delete_param(k)
            assert not (await nh.has_param(k))

        await test_util.call_with_nodehandle(f)

    async def test_advertise(self):
        async def f(nh: NodeHandle):
            from std_msgs.msg import Int32

            pub = nh.advertise("/my_topic", Int32, latching=True)
            await pub.setup()
            pub.publish(Int32(42))
            sub = nh.subscribe("/my_topic", Int32)
            await sub.setup()
            await sub.get_next_message()
            assert sub.get_last_message().data == 42

        await test_util.call_with_nodehandle(f)

    async def test_service(self):
        async def f(nh: NodeHandle):
            from roscpp_tutorials.srv import TwoInts, TwoIntsRequest, TwoIntsResponse

            async def callback(req: TwoIntsRequest):
                await util.wall_sleep(0.5)
                return TwoIntsResponse(sum=req.a + req.b)

            service: Service[TwoIntsRequest, TwoIntsResponse] = nh.advertise_service(
                "/my_service", TwoInts, callback
            )
            await service.setup()

            s = nh.get_service_client("/my_service", TwoInts)
            await s.wait_for_service()
            assert (await s(TwoIntsRequest(a=10, b=30))).sum == 40
            assert (await s(TwoIntsRequest(a=-10, b=30))).sum == 20

        await test_util.call_with_nodehandle(f)

    async def test_simulated_time(self):
        async def f(nh: NodeHandle):
            import time

            t1 = time.time()
            await nh.sleep(10)
            t2 = time.time()

            assert t2 - t1 < 5

        await test_util.call_with_nodehandle_sim_time(f)
