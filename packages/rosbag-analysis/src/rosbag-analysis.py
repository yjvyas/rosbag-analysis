#!/usr/bin/env python3

import os
import rospy
import rosbag
import numpy as np
from std_msgs.msg import Int32, String
from duckietown.dtros import DTROS, NodeType
from std_msgs.msg import String

class RosbagAnalysisPublisher(DTROS):

    def __init__(self, node_name):
        # initialize the DTROS parent class
        super(RosbagAnalysisPublisher, self).__init__(node_name=node_name, node_type=NodeType.GENERIC)
        # construct publisher
        self.pub = rospy.Publisher('chatter', String, queue_size=10)
        

    def run(self):
        # do some analysis on messages
        filepath = os.environ['ROSBAG_FILEPATH']
        rospy.loginfo("Filepath received is {}".format(filepath))
        bag = rosbag.Bag(filepath)

        message_times = {}
        for topic, msg, t in bag.read_messages():
            if topic not in message_times:
                message_times[topic] = []
            message_times[topic].append(rospy.Time(msg.header.stamp.secs, msg.header.stamp.nsecs))
        
        message_durations = {}
        for topic in message_times:
            message_durations[topic] = []
            for i in range(1,len(message_times[topic])):
                message_durations[topic].append((message_times[topic][i] - message_times[topic][i-1]).to_sec())
            message_durations[topic] = np.array(message_durations[topic])

            topic_stats = """\n{:s}
              num_messages: {:d}
              period:
                min: {:4f}
                max: {:4f}
                average: {:4f}
                median: {:4f}""".format(topic, 
                                        len(message_times[topic]), 
                                        np.min(message_durations[topic]), 
                                        np.max(message_durations[topic]), 
                                        np.mean(message_durations[topic]), 
                                        np.median(message_durations[topic]))
            rospy.loginfo(topic_stats)
            self.pub.publish(topic_stats)

if __name__ == '__main__':
    # create the node
    node = RosbagAnalysisPublisher(node_name='rosbag_analysis')
    # run node
    node.run()