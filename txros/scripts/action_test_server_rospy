#!/usr/bin/python

import rospy
rospy.init_node('action_test_server_rospy_node')
import actionlib
from actionlib.msg import TestAction, TestFeedback, TestResult

def execute(goal):
    print 'got goal', goal.goal
    
    for i in xrange(3):
        rospy.sleep(1)
        action_server.publish_feedback(TestFeedback(
            feedback=goal.goal + i,
        ))
        print 'sent feedback'
    rospy.sleep(1)
    
    action_server.set_succeeded(TestResult(
        result=goal.goal + 1000,
    ))
    print 'sent result'

action_server = actionlib.SimpleActionServer('test_action', TestAction, execute_cb=execute, auto_start=False)
action_server.start()

rospy.spin()
