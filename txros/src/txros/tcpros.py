from __future__ import division

import struct

from twisted.protocols import basic
from twisted.internet import defer, protocol

from txros import util

def deserialize_list(s):
    pos = 0
    res = []
    while pos != len(s):
        length, = struct.unpack('<I', s[pos:pos+4])
        if pos+4+length > len(s):
            raise ValueError('early end')
        res.append(s[pos+4:pos+4+length])
        pos = pos+4+length
    return res
def serialize_list(lst):
    return ''.join(struct.pack('<I', len(x)) + x for x in lst)

def deserialize_dict(s):
    res = {}
    for item in deserialize_list(s):
        key, value = item.split('=', 1)
        res[key] = value
    return res
def serialize_dict(s):
    return serialize_list('%s=%s' % (k, v) for k, v in s.iteritems())

class Server(basic.IntNStringReceiver):
    structFormat = '<I'
    prefixLength = struct.calcsize(structFormat)
    MAX_LENGTH = 2**32
    
    def __init__(self, handlers):
        self._handlers = handlers
        
        self.queue = None
    
    def stringReceived(self, string):
        if self.queue is None:
            # receive header
            header = deserialize_dict(string)
            self.queue = util.DeferredQueue()
            if 'service' in header:
                self._handlers['service', header['service']](header, self)
            elif 'topic' in header:
                self._handlers['topic', header['topic']](header, self)
            else:
                assert False
        else:
            self.queue.add(string)
    
    def connectionLost(self, reason):
        if self.queue is not None:
            self.queue.add(reason) # reason is a Failure

class Protocol(protocol.Protocol):
    def __init__(self):
        self._df_type = None
        self._buf = ''
        self._error = None
    
    def dataReceived(self, data):
        self._buf += data
        self._think()
    
    def connectionLost(self, reason):
        self._error = reason
        self._think()
    
    def _think(self):
        if self._df_type is None:
            return
        df, type_ = self._df_type
        
        if self._error is not None:
            df.callback(self._error)
            return
        
        if type_ == 'byte':
            if self._buf:
                byte, self._buf = self._buf[0], self._buf[1:]
                self._df_type = None
                df.callback(byte)
        elif type_ == 'string':
            if len(self._buf) >= 4:
                length, = struct.unpack('<I', self._buf[:4])
                if len(self._buf) >= 4 + length:
                    data, self._buf = self._buf[4:4+length], self._buf[4+length:]
                    self._df_type = None
                    df.callback(data)
        else:
            assert False
    
    def _receive(self, type_):
        assert self._df_type is None
        df = defer.Deferred()
        self._df_type = df, type_
        self._think()
        return df
    
    def receiveByte(self):
        return self._receive('byte')
    def receiveString(self):
        return self._receive('string')
    
    def sendByte(self, byte):
        assert len(byte) == 1
        self.transport.write(byte)
    def sendString(self, string):
        self.transport.write(struct.pack('<I', len(string)) + string)
