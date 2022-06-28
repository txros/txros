from __future__ import annotations

import random
import traceback
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol

import genpy
from actionlib_msgs.msg import GoalID, GoalStatus, GoalStatusArray
from std_msgs.msg import Header
from twisted.internet import defer

from . import util

if TYPE_CHECKING:
    from .nodehandle import NodeHandle
    from .action import GoalManager


class ActionMessage(Protocol):
    action_goal: genpy.Message
    action_result: genpy.Message
    action_feedback: genpy.Message

    def __call__(self) -> ActionMessage:
        ...


class GoalManager:
    """
    Manages the interactions between a specific goal and an action client.
    """

    def __init__(self, action_client: ActionClient, goal: Goal):
        """
        Args:
            action_client (ActionClient): The txROS action client to use to manage
                the goal.
            goal (Goal): The txROS goal to manage.
        """
        self._action_client = action_client
        self._goal = goal

        self._goal_id = "%016x" % (random.randrange(2**64),)

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

            self._action_client._goal_pub.publish(
                self._action_client._goal_type(
                    header=Header(
                        stamp=now,
                    ),
                    goal_id=GoalID(
                        stamp=now,
                        id=self._goal_id,
                    ),
                    goal=self._goal,
                )
            )
        except:
            traceback.print_exc()

    def _status_callback(self, status):
        del status
        pass  # XXX update state

    def _result_callback(self, status, result):
        del status
        # XXX update state
        # XXX cancel feedback deferreds

        self.forget()

        self._result_df.callback(result)

    def _feedback_callback(self, status, feedback):
        # XXX update state
        del status

        old, self._feedback_dfs = self._feedback_dfs, []
        for df in old:
            df.callback(feedback)

    def get_result(self) -> Any:
        """
        Gets the result of the goal from the manager.

        Returns:
            Any: ???
        """
        return util.branch_deferred(self._result_df, lambda df_: self.cancel())

    def get_feedback(self) -> defer.Deferred:
        """
        Gets the feedback from all feedback Deferred objects.
        """
        df = defer.Deferred()
        self._feedback_dfs.append(df)
        return df

    def cancel(self) -> None:
        """
        Publishes a message to the action client requesting the goal to be cancelled.
        """
        self._action_client._cancel_pub.publish(
            GoalID(
                stamp=genpy.Time(0, 0),
                id=self._goal_id,
            )
        )

        # XXX update state

        # self.forget()

    def forget(self) -> None:
        """
        Leaves the manager running, but requests for the action client to stop
        work on the specific goal.
        """
        if self._goal_id in self._action_client._goal_managers:
            del self._action_client._goal_managers[self._goal_id]


class Goal:
    """
    Implementation of a goal object in the txros adaptation of the Simple Action
    Server.

    .. container:: operations

        .. describe:: x = y

            determines equality between two goals by comparing their ids.

    Parameters:
        goal (GoalStatus): The original goal message which constructs this class
        status (uint8): An enum representing the status of
        status_text (:class:`str`): A string representing the status of the goal
    """

    def __init__(
        self,
        goal_msg: GoalStatus,
        status: int = GoalStatus.PENDING,
        status_text: str = "",
    ):
        if goal_msg.goal_id.id == "":
            self.goal = None
        self.goal = goal_msg
        self.status = status
        self.status_text = status_text

    def __eq__(self, rhs: Goal):
        # assert isinstance(self.goal, GoalStatus), f"Value was {type(self.goal)}: {self.goal}"
        return self.goal.goal_id.id == rhs.goal.goal_id.id

    def status_msg(self) -> GoalStatus:
        """
        Constructs a GoalStatus message from the Goal class.

        Returns:
            GoalStatus: The constructed message.
        """
        msg = GoalStatus()

        # Type checking
        # assert isinstance(self.goal, GoalStatus), f"Value was {type(self.goal)}: {self.goal}"

        # Assemble message
        msg.goal_id = self.goal.goal_id
        msg.status = self.status
        msg.text = self.status_text
        return msg


class SimpleActionServer:
    """
    A simplified implementation of an action server. At a given time, can only at most
    have a current goal and a next goal. If new goals arrive with one already queued
    to be next, the goal with the greater timestamp will bump to other.

    .. code-block:: python

        >>> # Contruct a SimpleActionServer with a node handle, topic namespace, and type
        >>> serv = SimpleActionServer(nh, '/test_action', turtle_actionlib.msg.ShapeAction)
        >>> # The server must be started before any goals can be accepted
        >>> serv.start()
        >>> # To accept a goal
        >>> while not self.is_new_goal_available():  # Loop until there is a new goal
        ...     yield nh.sleep(0.1)
        >>> goal = serv.accept_new_goal()
        >>> # To publish feedback
        >>> serv.publish_feedback(ShapeFeedback())
        >>> # To accept a preempt (a new goal attempted to replace the current one)
        >>> if self.is_preempt_requested():
        ...     goal = serv.accept_new_goal()  # Automaticly cancels old goal
        >>> # To finish a goal
        >>> serv.set_succeeded(text='Wahoo!', result=ShapeResult(apothem=1))
        >>> # or
        >>> serv.set_aborted(text='Something went wrong!')
    """

    # TODO:
    # - implement optional callbacks for new goals
    # - ensure headers are correct for each message

    def __init__(
        self,
        node_handle: NodeHandle,
        name: str,
        action_type: ActionMessage,
        goal_cb: Callable = None,
        preempt_cb: Callable = None,
    ):
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

        self._status_pub = self._node_handle.advertise(
            self._name + "/status", GoalStatusArray
        )
        self._result_pub = self._node_handle.advertise(
            self._name + "/result", self._result_type
        )
        self._feedback_pub = self._node_handle.advertise(
            self._name + "/feedback", self._feedback_type
        )

        self._goal_sub = self._node_handle.subscribe(
            self._name + "/goal", self._goal_type, self._goal_cb
        )
        self._cancel_sub = self._node_handle.subscribe(
            self._name + "/cancel", GoalID, self._cancel_cb
        )

    def register_goal_callback(self, func: Optional[Callable]) -> None:
        self.goal_cb = func

    def _process_goal_callback(self):
        if self.goal_cb:
            self.goal_cb()

    def _process_preempt_callback(self):
        if self.preempt_cb:
            self.preempt_cb()

    def register_preempt_callback(self, func: Optional[Callable]) -> None:
        self.preempt_cb = func

    def start(self) -> None:
        """
        Starts the status loop for the server.
        """
        self.started = True
        self._status_loop_defered = self._status_loop()

    def stop(self) -> None:
        """
        Stops the status loop for the server, and clears all running goals and all
        goals scheduled to be run.
        """
        self.goal = None
        self.next_goal = None
        self.started = False

    def accept_new_goal(self) -> None:
        if not self.started:
            print(
                "SIMPLE ACTION SERVER: attempted to accept_new_goal without being started"
            )
            return None
        if not self.next_goal:
            print(
                "SIMPLE ACTION SERVER: attempted to accept_new_goal when no new goal is available"
            )
        if self.goal:
            self.set_preempted(text="New goal accepted in simple action server")
        self.goal = self.next_goal
        self.cancel_requested = False
        self.next_goal = None
        self.goal.status = GoalStatus.ACTIVE
        self._publish_status()
        return self.goal.goal.goal

    def is_new_goal_available(self) -> bool:
        """
        Whether the next goal of the server is defined.

        Returns:
            bool: Whether the next goal is not ``None``.
        """
        return self.next_goal is not None

    def is_preempt_requested(self) -> bool:
        """
        Whether the goal has been requested to be cancelled, and there is both
        a goal currently running and a goal scheduled to be run shortly.

        Returns:
            bool
        """
        return (self.next_goal and self.goal) or self.is_cancel_requested()

    def is_cancel_requested(self) -> bool:
        """
        Whether a goal is currently active and a cancel has been requested.

        Returns:
            bool
        """
        return self.goal is not None and self.cancel_requested

    def is_active(self) -> bool:
        """
        Returns whether there is an active goal running.

        Returns:
            bool
        """
        return self.goal is not None

    def _set_result(
        self,
        status: int = GoalStatus.SUCCEEDED,
        text: str = "",
        result: Optional[genpy.Message] = None,
    ) -> None:
        if not self.started:
            print("SimpleActionServer: attempted to set_succeeded before starting")
            return
        if not self.goal:
            print("SimpleActionServer: attempted to set_succeeded without a goal")
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

    def set_succeeded(
        self, result: Optional[genpy.Message] = None, text: str = ""
    ) -> None:
        """
        Sets the status of the current goal to be succeeded.

        Args:
            result (Optional[genpy.Message]): The message to attach in the result.
            text (str): The text to set in the result. Defaults to an empty string.
        """
        self._set_result(status=GoalStatus.SUCCEEDED, text=text, result=result)

    def set_aborted(
        self, result: Optional[genpy.Message] = None, text: str = ""
    ) -> None:
        """
        Sets the status of the current goal to aborted.

        Args:
            result (Optional[genpy.Message]): The message to attach in the result.
            text (str): The text to set in the result. Defaults to an empty string.
        """
        self._set_result(status=GoalStatus.ABORTED, text=text, result=result)

    def set_preempted(
        self, result: Optional[genpy.Message] = None, text: str = ""
    ) -> None:
        """
        Sets the status of the current goal to preempted.

        Args:
            result (Optional[genpy.Message]): The message to attach in the result.
            text (str): The text to set in the result. Defaults to an empty string.
        """
        self._set_result(status=GoalStatus.PREEMPTED, text=text, result=result)

    def publish_feedback(self, feedback: Optional[genpy.Message] = None) -> None:
        """
        Publishes a feedback message onto the feedback topic.

        Args:
            feedback (Optional[genpy.Message]): The optional feedback message to add
                to the sent message.
        """
        if not self.started:
            print("SimpleActionServer: attempted to publish_feedback before starting")
            return
        if not self.goal:
            print("SimpleActionServer: attempted to publish_feedback without a goal")
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
        cancel_current = False
        cancel_next = False
        if msg.stamp != genpy.Time():
            if self.next_goal and self.next_goal.goal_id.stamp < msg.stamp:
                cancel_next = True
            if self.goal and self.goal.goal_id.stamp < msg.stamp:
                cancel_current = True

        if msg.id == "" and msg.stamp == genpy.Time():
            cancel_current = self.goal is not None
            cancel_next = self.next_goal is not None
        elif msg.id != "":
            if self.goal and msg.id == self.goal.goal_id.id:
                cancel_current = True
            if self.next_goal and msg.id == self.next_goal.id.id:
                cancel_next = True
        if cancel_next:
            self.next_goal.status = GoalStatus.RECALLED
            self.next_goal.status_text = "Goal cancled"
            result_msg = self._result_type()
            result_msg.status = self.next_goal.status_msg()
            self._result_pub.publish(result_msg)
            self._publish_status()
            self.next_goal = None
        if cancel_current and not self.cancel_requested:
            self.cancel_requested = True
            self._process_preempt_callback()

    @util.cancellableInlineCallbacks
    def _goal_cb(self, msg):
        if not self.started:
            defer.returnValue(None)
        new_goal = Goal(msg)
        if new_goal.goal is None:  # If goal field is empty, invalid goal
            defer.returnValue(None)
        # Throw out duplicate goals
        if (self.goal and new_goal == self.goal) or (
            self.next_goal and new_goal == self.next_goal
        ):
            defer.returnValue(None)
        now = yield self._node_handle.get_time()
        if (
            new_goal.goal.goal_id.stamp == genpy.Time()
        ):  # If time is not set, replace with current time
            new_goal.goal.goal_id.stamp = now
        if self.next_goal is not None:  # If another goal is queued, handle conflict
            # If next goal is later, rejct new goal
            if new_goal.goal.goal_id.stamp < self.next_goal.goal.goal_id.stamp:
                new_goal.status = GoalStatus.REJECTED
                new_goal.status_text = "canceled because another goal was received by the simple action server"
                result_msg = self._result_type()
                result_msg.header.stamp = now
                result_msg.status = new_goal.status_msg()
                self._result_pub.publish(result_msg)
                self._publish_status(goal=new_goal)
                defer.returnValue(None)
            else:  # New goal is later so reject current next_goal
                self.next_goal.status = GoalStatus.REJECTED
                self.next_goal.status_text = "Goal bumped by newer goal"
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


class ActionClient:
    def __init__(self, node_handle: NodeHandle, name: str, action_type: ActionMessage):
        self._node_handle = node_handle
        self._name = name
        self._type = action_type
        self._goal_type = type(action_type().action_goal)
        self._result_type = type(action_type().action_result)
        self._feedback_type = type(action_type().action_feedback)

        self._goal_managers: dict[str, GoalManager] = {}

        self._goal_pub = self._node_handle.advertise(
            self._name + "/goal", self._goal_type
        )
        self._cancel_pub = self._node_handle.advertise(self._name + "/cancel", GoalID)
        self._status_sub = self._node_handle.subscribe(
            self._name + "/status", GoalStatusArray, self._status_callback
        )
        self._result_sub = self._node_handle.subscribe(
            self._name + "/result", self._result_type, self._result_callback
        )
        self._feedback_sub = self._node_handle.subscribe(
            self._name + "/feedback", self._feedback_type, self._feedback_callback
        )

    def _status_callback(self, msg: GoalStatusArray):
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

    def send_goal(self, goal: Goal) -> GoalManager:
        """
        Sends a goal to a goal manager.

        Returns:
            GoalManager: The manager of the goal.
        """
        return GoalManager(self, goal)

    def cancel_all_goals(self) -> None:
        """
        Sends a message to the mission cancellation topic requesting all goals to
        be cancelled.
        """
        self._cancel_pub.publish(
            GoalID(
                stamp=genpy.Time(0, 0),
                id="",
            )
        )

    def cancel_goals_at_and_before_time(self, time: genpy.rostime.Time) -> None:
        """
        Cancel all goals scheduled at and before a specific time.

        Args:
            time: The time to reference when selecting which goals to cancel.
        """
        self._cancel_pub.publish(
            GoalID(
                stamp=time,
                id="",
            )
        )

    @util.cancellableInlineCallbacks
    def wait_for_server(self):
        """
        Waits for a server connection. When at least one connection is received,
        the function terminates.
        """
        while not (
            set(self._goal_pub.get_connections())
            & set(self._cancel_pub.get_connections())
            & set(self._status_sub.get_connections())
            & set(self._result_sub.get_connections())
            & set(self._feedback_sub.get_connections())
        ):
            yield util.wall_sleep(0.1)  # XXX bad bad bad
