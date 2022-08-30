#!/usr/bin/python3

import traceback

import uvloop
import asyncio
import genpy

import txros
from txros import txros_tf


async def main():
    nh = await txros.NodeHandle.from_argv("test_tf", anonymous=True)

    tf_listener = txros_tf.TransformListener(nh)
    await tf_listener.setup()

    while True:
        try:
            time = nh.get_time() - genpy.Duration(1)
            transform = await tf_listener.get_transform("/parent", "/child", time)
            transform2 = await tf_listener.get_transform(
                "/parent", "/child", time + genpy.Duration(0, 1000000)
            )
        except Exception:
            traceback.print_exc()
        else:
            print(time)
            print(transform)
            print((transform2 - transform) / 1e-3)
        await nh.sleep(0.01)


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
