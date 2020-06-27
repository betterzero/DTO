from imutils.object_detection import non_max_suppression
from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
from collections import OrderedDict
import paho.mqtt.client as mqtt
from datetime import datetime
from imutils import paths
import urllib.request
import numpy as np
import urllib.parse
import threading
import argparse
import imutils
import base64
import time
import math
import sys
import cv2
import ast


device_edge_server_topic = "edgeserver2/available/area1"
# One edge server device has several user_node sub_topics and the quantity equal to the number of usernode
edge_server_clients = [
{"broker":"192.168.1.111","port":1883,"name":"blank","sub_topic":"usernode1/request/area1","pub_topic":device_edge_server_topic},
{"broker":"192.168.1.111","port":1883,"name":"blank","sub_topic":"usernode3/request/area1","pub_topic":device_edge_server_topic},
{"broker":"192.168.1.111","port":1883,"name":"blank","sub_topic":"usernode5/request/area1","pub_topic":device_edge_server_topic},
{"broker":"192.168.1.111","port":1883,"name":"blank","sub_topic":"usernode7/request/area1","pub_topic":device_edge_server_topic},
{"broker":"192.168.1.111","port":1883,"name":"blank","sub_topic":"usernode9/request/area1","pub_topic":device_edge_server_topic},
{"broker":"192.168.1.111","port":1883,"name":"blank","sub_topic":"usernode11/request/area1","pub_topic":device_edge_server_topic}
]

quantity_edge_server_clients = len(edge_server_clients)

update_reachable_device_topic_unique = []    # online device and the value is unique, so as to achieve the purpose of each iteration of the algorithm
update_reachable_device_topic_repeated = []  # remove the offline device, for edge server, the device is usernode and vice versa
pre_time = datetime.now()
iteration_round = 999                        # iteration round number
rount_interval = 0
round_count = 0                              # update round_count every round

# resource requirement and cost(designed to be scalable)
request_users_info = {}                      # the message this device subscribe
result_server_sent = {}                      # the result this server to send
pub_flag = False
computation_resource_capacity = 11600        # the total computation capacity of this server
residual_computation_capacity = 0            # the residual computation capacity of this edge server


class CommunicateDevice:

    def __init__(self):
        pass

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.connected_flag = True
            for i in range(quantity_edge_server_clients):
                if edge_server_clients[i]["client"] == client:
                    topic = edge_server_clients[i]["sub_topic"]
                    break
            client.subscribe(topic)
        else:
            print("Bad connection Returned code = ", rc)
            client.loop_stop()

    def on_message(self, client, userdata, message):
        global pub_flag
        global Run_Flag
        global round_count
        global request_users_info
        user_topic = message.topic
        update_reachable_device_topic_repeated.append(user_topic)
        # After receive the subscribed client topic, edge server need to consider the "every round" constraint
        if update_reachable_device_topic_unique.count(user_topic) == 0:
            update_reachable_device_topic_unique.append(user_topic)
            user_content = str(message.payload.decode("utf-8"))
            user_content_dict = eval(user_content)
            user_content_digital = user_content_dict[device_edge_server_topic]  # obtain the message that user send to this server
            request_users_info[user_topic] = user_content_digital               # store the key-value, i.e., the task quantity

        if len(update_reachable_device_topic_unique) == quantity_edge_server_clients and round_count < iteration_round:
            now_time = datetime.now()
            if now_time.second - pre_time.second >= 5:
                print(sum(request_users_info.values()))

            strategic_decision = StrategicDecision()
            strategic_decision.dto_ux_obtain_data_to_sent()
            #print("This server gets these UE information ", request_users_info)
            round_count += 1
            pub_flag = True
            if round_count == iteration_round:
                Run_Flag = False

            update_reachable_device_topic_unique.clear()

    def on_disconnect(self, client, userdata, rc):
        print("client disconnected")

    def create_multi_connections(self):
        for i in range(quantity_edge_server_clients):
            cname = "client"+str(i)
            t = int(time.time())
            client_id = cname+str(t)
            client = mqtt.Client(client_id)
            edge_server_clients[i]["client"] = client 
            edge_server_clients[i]["client_id"] = client_id
            edge_server_clients[i]["cname"] = cname
            broker = edge_server_clients[i]["broker"]
            port = edge_server_clients[i]["port"]
            try:
                client.connect(broker,port)
            except:
                print("Connection Fialed to broker ",broker)
                continue
            
            client.on_connect = self.on_connect
            client.on_disconnect = self.on_disconnect
            client.on_message = self.on_message
            client.loop_start()
            while not client.connected_flag:
                time.sleep(0.05)


class StrategicDecision:

    def __init__(self):
        pass

    def dto_ux_obtain_utilization_rate(self):
        if sum(request_users_info.values()) == 0:
            utilization_rate = 0
        else:
            utilization_rate = sum(request_users_info.values()) / computation_resource_capacity

        return utilization_rate

    def dto_ux_obtain_data_to_sent(self):
        global result_server_sent
        utilization_rate = self.dto_ux_obtain_utilization_rate()
        result_server_sent = [computation_resource_capacity, utilization_rate]
        return result_server_sent


class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


class AppDetection:

    def __init__(self):
        pass

    def detect_pedestrian(self):

        start = time.perf_counter()

        ap = argparse.ArgumentParser()
        ap.add_argument("-i", "--images", required=True, help="path to images directory")
        hog = cv2.HOGDescriptor()
        hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())  # set SVM

        image_Path = "./images";
        # loop over the image paths
        for imagePath in paths.list_images(image_Path):  # args["images"]
            # load the image and resize it to (1) reduce detection time
            # and (2) improve detection accuracy
            image = cv2.imread(imagePath)
            image = imutils.resize(image, width=min(400, image.shape[1]))
            orig = image.copy()

            # detect people in the image：
            (rects, weights) = hog.detectMultiScale(image, winStride=(4, 4),
                                                    padding=(8, 8), scale=1.05)

            # draw the original bounding boxes
            for (x, y, w, h) in rects:
                cv2.rectangle(orig, (x, y), (x + w, y + h), (0, 0, 255), 2)

            rects = np.array([[x, y, x + w, y + h] for (x, y, w, h) in rects])
            pick = non_max_suppression(rects, probs=None, overlapThresh=0.65)

            # draw the final bounding boxes
            for (xA, yA, xB, yB) in pick:
                cv2.rectangle(image, (xA, yA), (xB, yB), (0, 255, 0), 2)

            # show some information on the number of bounding boxes
            filename = imagePath[imagePath.rfind("/") + 1:]
            # print("[INFO] {}: {} original boxes, {} after suppression".format(filename, len(rects), len(pick)))

            # show the output imagesr
            # cv2.imshow("Before NMS" + str(count), orig)
            # cv2.imshow("After NMS" + str(thread_num), image)

        elapsed = (time.perf_counter() - start)

        print(elapsed)

        return elapsed



mqtt.Client.connected_flag = False

communicate_device = CommunicateDevice()
communicate_device.create_multi_connections()

active_thread_num = threading.active_count()
print("current threads = ", active_thread_num)
print("Creating  Connections ", quantity_edge_server_clients," edge_server_clients")

result_server_sent = [computation_resource_capacity, 0]

print("Publishing ")

Run_Flag = True
try:
    while Run_Flag:
        client = edge_server_clients[0]["client"]
        pub_topic = edge_server_clients[0]["pub_topic"]
        if client.connected_flag and pub_flag:
            #print("edge server "+ str(2) + " is publishing~~~~~~~~~~" + pub_topic)
            client.publish(pub_topic, str(result_server_sent))
            pub_flag = False

except KeyboardInterrupt:
    print("interrupted by keyboard")

for i in range(quantity_edge_server_clients):
    client = mqtt.Client(edge_server_clients[i]["client_id"])
    client.loop_stop()

time.sleep(1)
