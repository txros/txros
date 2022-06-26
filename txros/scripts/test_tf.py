#!/usr/bin/python3

import traceback

import genpy

import txros
from txros import util
from txros import txros_tf


@util.cancellableInlineCallbacks
def main():
    nh = yield txros.NodeHandle.from_argv("test_tf", anonymous=True)

    tf_listener = txros_tf.TransformListener(nh)

    while True:
        try:
            time = nh.get_time() - genpy.Duration(1)
            transform = yield tf_listener.get_transform("/parent", "/child", time)
            transform2 = yield tf_listener.get_transform(
                "/parent", "/child", time + genpy.Duration(0, 1000000)
            )
        except Exception:
            traceback.print_exc()
        else:
            print(time)
            print(transform)
            print((transform2 - transform) / 1e-3)
        yield nh.sleep(0.01)


util.launch_main(main)
