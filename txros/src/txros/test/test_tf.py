import traceback

import numpy
import asyncio
from twisted.internet import defer
import unittest

import genpy
from geometry_msgs.msg import Transform, TransformStamped, Quaternion, Vector3
from std_msgs.msg import Header
from tf import transformations

from txros import txros_tf, util
from txros.test import util as test_util
from txros.nodehandle import NodeHandle


class Test(unittest.IsolatedAsyncioTestCase):
    async def test_tf(self):
        async def f(nh: NodeHandle):
            tf_broadcaster = txros_tf.TransformBroadcaster(nh)
            await tf_broadcaster.setup()

            async def publish_thread():
                try:
                    while True:
                        t = nh.get_time()
                        tf_broadcaster.send_transform(
                            TransformStamped(
                                header=Header(
                                    stamp=t,
                                    frame_id="/parent",
                                ),
                                child_frame_id="/child",
                                transform=Transform(
                                    translation=Vector3(*[1, 2, 3]),
                                    rotation=Quaternion(
                                        *transformations.quaternion_about_axis(
                                            t.to_sec(), [0, 0, 1]
                                        )
                                    ),
                                ),
                            )
                        )
                        await nh.sleep(0.1)
                except Exception:
                    traceback.print_exc()

            publish_task = asyncio.create_task(publish_thread())

            try:
                tf_listener = txros_tf.TransformListener(
                    nh, history_length=genpy.Duration(1)
                )  # short history length so that we cover history being truncated
                await tf_listener.setup()
                print("setup tf listener...")

                try:
                    await tf_listener.get_transform(
                        "/parent", "/child", nh.get_time() - genpy.Duration(100)
                    )
                except txros_tf.TooPastError:
                    print("got toopasterror")
                    pass
                else:
                    self.fail("expected TooPastError")

                start_time = nh.get_time()
                for i in range(200):
                    time = start_time + genpy.Duration.from_sec(1 + i / 100)
                    dt = 1e-3
                    transform = await tf_listener.get_transform(
                        "/parent", "/child", time
                    )
                    transform2 = await tf_listener.get_transform(
                        "/parent", "/child", time + genpy.Duration.from_sec(1e-3)
                    )
                    assert numpy.allclose(
                        (transform2 - transform) / dt, [0, 0, 0, 0, 0, 1]
                    )
            finally:
                publish_task.cancel()

        await test_util.call_with_nodehandle(f)
