from __future__ import division

import os
import random

from twisted.internet import defer, reactor, endpoints, protocol, error

from txros import NodeHandle, util


@defer.inlineCallbacks
def start_roscore():
    while True:
        port = random.randrange(49152, 65535+1) # dynamic TCP port range
        
        # check that port is free
        try:
            prot = yield endpoints.TCP4ClientEndpoint(reactor, '127.0.0.1', port).connect(util.AutoServerFactory(lambda addr: protocol.Protocol()))
        except error.ConnectionRefusedError:
            break
        else:
            prot.transport.loseConnection()
            continue
    
    p = reactor.spawnProcess(protocol.ProcessProtocol(), 'roscore',
        args=['roscore', '-p', str(port)], env=dict(os.environ, ROS_MASTER_URI='http://127.0.0.1:%i' % (port,)),
    )
    
    # wait until port is used
    for i in xrange(100): # 10 seconds max
        try:
            prot = yield endpoints.TCP4ClientEndpoint(reactor, '127.0.0.1', port).connect(util.AutoServerFactory(lambda addr: protocol.Protocol()))
        except error.ConnectionRefusedError:
            yield util.wall_sleep(.1)
            continue
        else:
            prot.transport.loseConnection()
            break
    else:
        assert False, 'roscore never came up'
    
    class ROSCore(object):
        def get_port(self):
            return port
        def stop(self):
            p.loseConnection()
            p.signalProcess('TERM')
    defer.returnValue(ROSCore())

@defer.inlineCallbacks
def call_with_nodehandle(f):
    roscore = yield start_roscore()
    try:
        nh = yield NodeHandle(
            ns='',
            name='node',
            addr='127.0.0.1',
            master_uri='http://127.0.0.1:%i' % (roscore.get_port(),),
            remappings={},
        )
        try:
            defer.returnValue((yield f(nh)))
        finally:
            yield nh.shutdown()
    finally:
        yield roscore.stop()
