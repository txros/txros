import asyncio
import os
import shutil
import subprocess
import tempfile
import traceback

from typing import Callable, Awaitable

import genpy
from rosgraph_msgs.msg import Clock
from twisted.internet import defer, threads

from txros import NodeHandle, util


async def start_rosmaster():
    tmpd = tempfile.mkdtemp()
    try:
        logfile = f"{tmpd}/master.log"
        p = subprocess.Popen(["rosmaster", "--core", "-p", "0", "__log:=" + logfile])

        for i in range(1000):
            if os.path.exists(logfile):
                success = False
                with open(logfile, "rb") as f:
                    for line in f:
                        if b": Master initialized: port[0], uri[http://" in line:
                            port = int(line.split(b":")[-1].split(b"/")[0])
                            success = True
                            break
                if success:
                    break
            await util.wall_sleep(0.01)
        else:
            assert False, "rosmaster never came up"

        class ROSMaster:
            def get_port(self):
                return port

            def stop(self):
                p.terminate()
                loop = asyncio.get_event_loop()
                return loop.run_in_executor(None, p.wait)

        return ROSMaster()
    finally:
        shutil.rmtree(tmpd)


async def call_with_nodehandle(f: Callable[[NodeHandle], Awaitable]):
    rosmaster = await start_rosmaster()
    try:
        nh = await NodeHandle.from_argv(
            "node",
            argv=[
                "__ip:=127.0.0.1",
                f"__master:=http://127.0.0.1:{rosmaster.get_port()}"
            ],
            anonymous=True,
        )
        try:
            print("getting result...")
            result = await f(nh)
            print(f"got result: {result}")
        finally:
            print("asking node to shutdown...")
            await nh.shutdown()
        print("returning result!")
        return result
    finally:
        print("stopping rosmaster")
        await rosmaster.stop()


async def call_with_nodehandle_sim_time(f):
    rosmaster = await start_rosmaster()
    try:
        nh = await NodeHandle.from_argv(
            "node",
            argv=[
                "__ip:=127.0.0.1",
                "__master:=http://127.0.0.1:%i" % (rosmaster.get_port(),),
            ],
            anonymous=True,
        )
        try:
            async def clock_thread():
                try:
                    clock_pub = nh.advertise("/clock", Clock)
                    await clock_pub.setup()
                    t = genpy.Time.from_sec(12345)
                    while True:
                        clock_pub.publish(
                            Clock(
                                clock=t,
                            )
                        )
                        await util.wall_sleep(0.01)
                        t = t + genpy.Duration.from_sec(0.1)
                except Exception:
                    traceback.print_exc()
            clock_task = asyncio.create_task(clock_thread())

            try:
                await nh.set_param("/use_sim_time", True)

                nh2 = await NodeHandle.from_argv(
                    "node2",
                    argv=[
                        "__ip:=127.0.0.1",
                        "__master:=http://127.0.0.1:%i" % (rosmaster.get_port(),),
                    ],
                    anonymous=True,
                )
                try:
                    return await f(nh2)
                finally:
                    await nh2.shutdown()
            finally:
                clock_task.cancel()
        finally:
            await nh.shutdown()
    finally:
        await rosmaster.stop()
