from __future__ import division

import random
import traceback

from twisted.internet import defer

from std_msgs.msg import Header
from actionlib_msgs.msg import GoalID, GoalStatus, GoalStatusArray
import genpy

from txros import util


class GoalManager(object):

    def __init__(self, action_client, goal):
        self._action_client = action_client
        self._goal = goal

        self._goal_id = '%016x' % (random.randrange(2 ** 64),)

        assert self._goal_id not in self._action_client._goal_managers
        self._action_client._goal_managers[self._goal_id] = self

        self._feedback_dfs = []
        self._result_df = defer.Deferred()

        self._think_thread = self._think()

    @util.cancellableInlineCallbacks
    def _think(self):
        try:
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
        except:
            traceback.print_exc()

    def _status_callback(self, status):
        pass  # XXX update state

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
        return util.branch_deferred(self._result_df, lambda df_: self.cancel())

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

        # self.forget()

    def forget(self):
        '''leave action running, stop listening'''

        if self._goal_id in self._action_client._goal_managers:
            del self._action_client._goal_managers[self._goal_id]


class Goal(object):
    def __init__(self, goal_msg, status=GoalStatus.PENDING, status_text=''):
        if goal_msg.goal_id.id == '':
            self.goal = None
        self.goal = goal_msg
        self.status = status
        self.status_text = ''

    def __eq__(self, rhs):
        return self.goal.goal_id.id == rhs.goal.goal_id.id

    def status_msg(self):
        msg = GoalStatus()
        msg.goal_id = self.goal.goal_id
        msg.status = self.status
        msg.text = self.status_text
        return msg


class SimpleActionServer(object):
    '''
    A simplified implementation of an action server. At a given time, can only at most
    have a current goal and a next goal. If new goals arrive with one already queued
    to be next, the goal with the greater timestamp will bump to other.

    Usage:
    Contruct a SimpleActionServer with a node handle, topic namespace, and type
        serv = SimpleActionServer(nh, '/test_action', turtle_actionlib.msg.ShapeAction)
    The server must be started before any goals can be accepted
        serv.start()
    To accept a goal
        while not self.is_new_goal_available():  # Loop until there is a new goal
            yield nh.sleep(0.1)
        goal = serv.accept_new_goal()
    To publish feedback
        serv.publish_feedback(ShapeFeedback())
    To accept a preempt (a new goal attempted to replace the current one)
        if self.is_preempt_requested():
            goal = serv.accept_new_goal()  # Automaticly cancels old goal
    To finish a goal
        serv.set_succeeded(text='Wahoo!', result=ShapeResult(apothem=1))
        # or
        serv.set_aborted(text='Something went wrong!')

    TODO:
    - implement optional callbacks for new goals
    - ensure headers are correct for each message
    '''
    def __init__(self, node_handle, name, action_type, goal_cb=None, preempt_cb=None):
        self.started = False
        self._node_handle = node_handle
        self._name = name

        self.goal = None
        self.next_goal = None
        self.cancel_requested = False

        self.register_goal_callback(goal_cb)
        self.register_preempt_callback(preempt_cb)

        self.goal_cb = None
        self.preempt_cb = None

        self.status_frequency = 5.0

        self._goal_type = type(action_type().action_goal)
        self._result_type = type(action_type().action_result)
        self._feedback_type = type(action_type().action_feedback)

        self._status_pub = self._node_handle.advertise(self._name + '/status', GoalStatusArray)
        self._result_pub = self._node_handle.advertise(self._name + '/result', self._result_type)
        self._feedback_pub = self._node_handle.advertise(self._name + '/feedback', self._feedback_type)

        self._goal_sub = self._node_handle.subscribe(self._name + '/goal', self._goal_type, self._goal_cb)
        self._cancel_sub = self._node_handle.subscribe(self._name + '/cancel', GoalID, self._cancel_cb)

    def register_goal_callback(self, func):
        self.goal_cb = func

    def _process_goal_callback(self):
        if self.goal_cb:
            self.goal_cb()

    def _process_preempt_callback(self):
        if self.preempt_cb:
            self.preempt_cb()

    def register_preempt_callback(self, func):
        self.preempt_cb = func

    def start(self):
        self.started = True
        self._status_loop_defered = self._status_loop()

    def stop(self):
        self.goal = None
        self.next_goal = None
        self.started = False

    def accept_new_goal(self):
        if not self.started:
            print('SIMPLE ACTION SERVER: attempted to accept_new_goal without being started')
            return None
        if not self.next_goal:
            print('SIMPLE ACTION SERVER: attempted to accept_new_goal when no new goal is available')
        if self.goal:
            self.set_preempted(text='New goal accepted in simple action server')
        self.goal = self.next_goal
        self.cancel_requested = False
        self.next_goal = None
        self.goal.status = GoalStatus.ACTIVE
        self._publish_status()
        return self.goal.goal.goal

    def is_new_goal_available(self):
        return self.next_goal is not None

    def is_preempt_requested(self):
        return (self.next_goal and self.goal) or self.is_cancel_requested()

    def is_cancel_requested(self):
        return self.goal is not None and self.cancel_requested

    def is_active(self):
        return self.goal is not None

    def _set_result(self, status=GoalStatus.SUCCEEDED, text='', result=None):
        if not self.started:
            print('SimpleActionServer: attempted to set_succeeded before starting')
            return
        if not self.goal:
            print('SimpleActionServer: attempted to set_succeeded without a goal')
            return
        self.goal.status = status
        self.goal.status_text = text
        self._publish_status()
        result_msg = self._result_type()
        if result:
            result_msg.result = result
        result_msg.status = self.goal.status_msg()
        self._result_pub.publish(result_msg)
        self.goal = None

    def set_succeeded(self, result=None, text=''):
        self._set_result(status=GoalStatus.SUCCEEDED, text=text, result=result)

    def set_aborted(self, result=None, text=''):
        self._set_result(status=GoalStatus.ABORTED, text=text, result=result)

    def set_preempted(self, result=None, text=''):
        self._set_result(status=GoalStatus.PREEMPTED, text=text, result=result)

    def publish_feedback(self, feedback=None):
        if not self.started:
            print('SimpleActionServer: attempted to publish_feedback before starting')
            return
        if not self.goal:
            print('SimpleActionServer: attempted to publish_feedback without a goal')
            return
        feedback_msg = self._feedback_type()
        feedback_msg.status = self.goal.status_msg()
        if feedback:
            feedback_msg.feedback = feedback
        self._feedback_pub.publish(feedback_msg)

    def _publish_status(self, goal=None):
        msg = GoalStatusArray()
        if self.goal:
            msg.status_list.append(self.goal.status_msg())
        if self.next_goal:
            msg.status_list.append(self.next_goal.status_msg())
        if goal:
            msg.status_list.append(goal.status_msg())
        self._status_pub.publish(msg)

    @util.cancellableInlineCallbacks
    def _status_loop(self):
        while self.started:
            self._publish_status()
            yield self._node_handle.sleep(1.0 / self.status_frequency)

    def _cancel_cb(self, msg):
        if self.next_goal and msg.id == self.next_goal.goal.goal_id.id:  # Cancel next_goal
            self.next_goal.status = GoalStatus.RECALLED
            self.next_goal.status_text = 'Goal cancled'
            result_msg = self._result_type()
            result_msg.status = self.next_goal.status_msg()
            self._result_pub.publish(result_msg)
            self._publish_status()
            self.next_goal = None
            return
        elif self.goal and msg.id == self.goal.goal.goal_id.id and not self.cancel_requested:
            self.cancel_requested = True
            self._process_preempt_callback()

    @util.cancellableInlineCallbacks
    def _goal_cb(self, msg):
        if not self.started:
            defer.returnValue(None)
        new_goal = Goal(msg)
        if new_goal.goal is None:  # If goal field is empty, invalid goal
            defer.returnValue(None)
        if (self.goal and new_goal == self.goal) or (self.next_goal and new_goal == self.next_goal):  # Throw out duplicate goals
            defer.returnValue(None)
        now = yield self._node_handle.get_time()
        if new_goal.goal.goal_id.stamp == genpy.Time(): # If time is not set, replace with current time
            new_goal.goal.goal_id.stamp = now
        if self.next_goal is not None:  # If another goal is queued, handle conflict
            if new_goal.goal.goal_id.stamp < self.next_goal.goal.goal_id.stamp:  # next_goal is later, so reject new_goal
                new_goal.status = GoalStatus.REJECTED
                new_goal.status_text = "This goal was canceled because another goal was received by the simple action server"
                result_msg = self._result_type()
                result_msg.header.stamp = now
                result_msg.status = new_goal.status_msg()
                self._result_pub.publish(result_msg)
                self._publish_status(goal=new_goal)
                defer.returnValue(None)
            else:  # New goal is later so reject current next_goal
                self.next_goal.status = GoalStatus.REJECTED
                self.next_goal.status_text = 'Goal bumped by newer goal'
                result_msg = self._result_type()
                result_msg.header.stamp = now
                result_msg.status = self.next_goal.status_msg()
                self._result_pub.publish(result_msg)
                self._publish_status()
        self.next_goal = new_goal
        self._publish_status()
        if self.goal:
            self._process_preempt_callback()
        else:
            self._process_goal_callback()


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
        self._feedback_sub = self._node_handle.subscribe(
            self._name + '/feedback',
            self._feedback_type,
            self._feedback_callback)

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

    @util.cancellableInlineCallbacks
    def wait_for_server(self):
        while not (
            set(self._goal_pub.get_connections()) &
            set(self._cancel_pub.get_connections()) &
            set(self._status_sub.get_connections()) &
            set(self._result_sub.get_connections()) &
            set(self._feedback_sub.get_connections())
        ):
            yield util.wall_sleep(0.1)  # XXX bad bad bad
