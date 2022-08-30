#!/usr/bin/python3

from twisted.internet import defer

import uvloop
import asyncio
import txros
from txros import util

from geometry_msgs.msg import PointStamped
from roscpp.srv import GetLoggers, GetLoggersRequest


async def main():
    nh = await txros.NodeHandle.from_argv("testnode", anonymous=True)

    pub = nh.advertise("point2", PointStamped, latching=True)
    await pub.setup()

    def cb(msg):
        pub.publish(msg)

    sub = nh.subscribe("point", PointStamped, cb)

    async def task1():
        msg = await sub.get_next_message()
        print(msg)

    await task1()

    async def task2():
        while True:
            await nh.sleep(1)
            x = await nh.get_param("/")

    await task2()

    async def task3():
        while nh.is_running():
            pub = nh.advertise("~point3", PointStamped, latching=True)
            await nh.sleep(0.001)
            await pub.shutdown()

    await task3()

    async def task4():
        print(
            (
                nh.get_service_client("/rosout/get_loggers", GetLoggers)(
                    GetLoggersRequest()
                )
            )
        )

    await task4()

    await asyncio.Future()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
