txROS is an alternative Python client library for ROS (Robot Operating System).
It seeks to improve on rospy by avoiding threading and providing other,
potentially more useful interfaces, in addition to callbacks.
It does this by utilizing the Twisted networking library (including Deferreds and generator-based coroutines).

It is a work in progress, and as such, its API is not stable.
