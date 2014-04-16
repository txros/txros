from __future__ import division

import random

from twisted.internet import defer, reactor

from std_msgs.msg import Header
from actionlib_msgs.msg import GoalID, GoalStatusArray
import genpy

from txros import util


class GoalManager(object):
    def __init__(self, action_client, goal):
        self._action_client = action_client
        self._goal = goal
        
        self._goal_id = '%016x' % (random.randrange(2**64),)
        
        assert self._goal_id not in self._action_client._goal_managers
        self._action_client._goal_managers[self._goal_id] = self
        
        self._feedback_dfs = []
        self._result_df = defer.Deferred()
        
        self._think_thread = self._think()
    
    @util.cancellableInlineCallbacks
    def _think(self):
        now = self._action_client._node_handle.get_time()
        
        yield self._action_client.wait_for_server()
        
        self._action_client._goal_pub.publish(self._action_client._goal_type(
            header=Header(
                stamp=now,
            ),
            goal_id=GoalID(
                stamp=now,
                id=self._goal_id,
            ),
            goal=self._goal,
        ))
    
    def _status_callback(self, status):
        pass # XXX update state
    
    def _result_callback(self, status, result):
        # XXX update state
        # XXX cancel feedback deferreds
        
        self.forget()
        
        self._result_df.callback(result)
    
    def _feedback_callback(self, status, feedback):
        # XXX update state
        
        old, self._feedback_dfs = self._feedback_dfs, []
        for df in old:
            df.callback(feedback)
    
    def get_result(self):
        return util.branch_deferred(self._result_df)
    
    def get_feedback(self):
        df = defer.Deferred()
        self._feedback_dfs.append(df)
        return df
    
    def cancel(self):
        self._action_client._cancel_pub.publish(GoalID(
            stamp=genpy.Time(0, 0),
            id=self._goal_id,
        ))
        
        # XXX update state
        
        #self.forget()
    
    def forget(self):
        '''leave action running, stop listening'''
        
        if self._goal_id in self._action_client._goal_managers:
            del self._action_client._goal_managers[self._goal_id]

class ActionClient(object):
    def __init__(self, node_handle, name, action_type):
        self._node_handle = node_handle
        self._name = name
        self._type = action_type
        self._goal_type = type(action_type().action_goal)
        self._result_type = type(action_type().action_result)
        self._feedback_type = type(action_type().action_feedback)
        
        self._goal_managers = {}
        
        self._goal_pub = self._node_handle.advertise(self._name + '/goal', self._goal_type)
        self._cancel_pub = self._node_handle.advertise(self._name + '/cancel', GoalID)
        self._status_sub = self._node_handle.subscribe(self._name + '/status', GoalStatusArray, self._status_callback)
        self._result_sub = self._node_handle.subscribe(self._name + '/result', self._result_type, self._result_callback)
        self._feedback_sub = self._node_handle.subscribe(self._name + '/feedback', self._feedback_type, self._feedback_callback)
        
        self._start_time = reactor.seconds()
    
    def _status_callback(self, msg):
        for status in msg.status_list:
            if status.goal_id.id in self._goal_managers:
                manager = self._goal_managers[status.goal_id.id]
                manager._status_callback(status.status)
    
    def _result_callback(self, msg):
        if msg.status.goal_id.id in self._goal_managers:
            manager = self._goal_managers[msg.status.goal_id.id]
            manager._result_callback(msg.status.status, msg.result)
    
    def _feedback_callback(self, msg):
        if msg.status.goal_id.id in self._goal_managers:
            manager = self._goal_managers[msg.status.goal_id.id]
            manager._feedback_callback(msg.status.status, msg.feedback)
    
    def send_goal(self, goal):
        return GoalManager(self, goal)
    
    def cancel_all_goals(self):
        self._cancel_pub.publish(GoalID(
            stamp=genpy.Time(0, 0),
            id='',
        ))
    
    def cancel_goals_at_and_before_time(self, time):
        self._cancel_pub.publish(GoalID(
            stamp=time,
            id='',
        ))
    
    def wait_for_server(self):
        return util.sleep(max(0, self._start_time + 3 - reactor.seconds()))
