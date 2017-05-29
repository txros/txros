from __future__ import division

import os
import tempfile
import shutil
import subprocess
import traceback

from twisted.internet import defer, threads

import genpy
from rosgraph_msgs.msg import Clock

from txros import NodeHandle, util


@defer.inlineCallbacks
def start_rosmaster():
    tmpd = tempfile.mkdtemp()
    try:
        logfile = '%s/master.log' % (tmpd,)
        p = subprocess.Popen(['rosmaster', '--core', '-p', '0', '__log:=' + logfile])

        for i in xrange(1000):
            if os.path.exists(logfile):
                success = False
                with open(logfile, 'rb') as f:
                    for line in f:
                        if ': Master initialized: port[0], uri[http://' in line:
                            port = int(line.split(':')[-1].split('/')[0])
                            success = True
                            break
                if success:
                    break
            yield util.wall_sleep(.01)
        else:
            assert False, 'rosmaster never came up'

        class ROSMaster(object):

            def get_port(self):
                return port

            def stop(self):
                p.terminate()
                return threads.deferToThread(p.wait)
        defer.returnValue(ROSMaster())
    finally:
        shutil.rmtree(tmpd)


@defer.inlineCallbacks
def call_with_nodehandle(f):
    rosmaster = yield start_rosmaster()
    try:
        nh = yield NodeHandle.from_argv('node',
                                        argv=['__ip:=127.0.0.1',
                                              '__master:=http://127.0.0.1:%i' % (rosmaster.get_port(),), ],
                                        anonymous=True,)
        try:
            defer.returnValue((yield f(nh)))
        finally:
            yield nh.shutdown()
    finally:
        yield rosmaster.stop()


@util.cancellableInlineCallbacks
def call_with_nodehandle_sim_time(f):
    rosmaster = yield start_rosmaster()
    try:
        nh = yield NodeHandle.from_argv('node',
                                        argv=['__ip:=127.0.0.1',
                                              '__master:=http://127.0.0.1:%i' % (rosmaster.get_port(),), ],
                                        anonymous=True,)
        try:
            @apply
            @util.cancellableInlineCallbacks
            def clock_thread():
                try:
                    clock_pub = nh.advertise('/clock', Clock)
                    t = genpy.Time.from_sec(12345)
                    while True:
                        clock_pub.publish(Clock(
                            clock=t,
                        ))
                        yield util.wall_sleep(.01)
                        t = t + genpy.Duration.from_sec(.1)
                except Exception:
                    traceback.print_exc()
            try:
                yield nh.set_param('/use_sim_time', True)

                nh2 = yield NodeHandle.from_argv('node2',
                                                 argv=['__ip:=127.0.0.1',
                                                       '__master:=http://127.0.0.1:%i' % (rosmaster.get_port(),), ],
                                                 anonymous=True,)
                try:
                    defer.returnValue((yield f(nh2)))
                finally:
                    yield nh2.shutdown()
            finally:
                clock_thread.cancel()
                clock_thread.addErrback(lambda fail: fail.trap(defer.CancelledError))
        finally:
            yield nh.shutdown()
    finally:
        yield rosmaster.stop()
