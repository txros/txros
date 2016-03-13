from __future__ import division

import os
import tempfile
import shutil
import subprocess

from twisted.internet import defer, threads

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
                if success: break
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
            argv=[
                '__ip:=127.0.0.1',
                '__master:=http://127.0.0.1:%i' % (rosmaster.get_port(),),
            ],
            anonymous=True,
        )
        try:
            defer.returnValue((yield f(nh)))
        finally:
            yield nh.shutdown()
    finally:
        yield rosmaster.stop()
