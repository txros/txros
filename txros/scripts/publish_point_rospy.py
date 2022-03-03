#!/usr/bin/python

from __future__ import division

import random

import rospy
from std_msgs.msg import Header
from geometry_msgs.msg import PointStamped, Point


rospy.init_node('publish_points')

pub = rospy.Publisher('point', PointStamped)

while not rospy.is_shutdown():
    pub.publish(PointStamped(
        header=Header(
            stamp=rospy.Time.now(),
            frame_id='/txros_demo',
        ),
        point=Point(*[random.gauss(0, 1) for i in xrange(3)]),
    ))
    rospy.sleep(1/4)
