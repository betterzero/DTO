from imutils.object_detection import non_max_suppression
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
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode1/request/area1/edge","pub_topic":device_edge_server_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode3/request/area1/edge","pub_topic":device_edge_server_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode5/request/area1/edge","pub_topic":device_edge_server_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode7/request/area1/edge","pub_topic":device_edge_server_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode9/request/area1/edge","pub_topic":device_edge_server_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode11/request/area1/edge","pub_topic":device_edge_server_topic}
]

quantity_edge_server_clients = len(edge_server_clients)

update_reachable_device_topic_unique = []    # online device and the value is unique, so as to achieve the purpose of each iteration of the algorithm
update_reachable_device_topic_repeated = []  # remove the offline device, for edge server, the device is usernode and vice versa
round_count = 0                              # update round_count every round
iteration_round = 1                          # iteration round number
process_detect_pedestrian_count = 0

# resource requirement and cost(designed to be scalable)
request_users_info = {}                      # the message this device subscribe
result_server_sent = {}                      # the result this server to send
computation_resource_capacity = 11600        # the total computation capacity of this server
residual_computation_capacity = 0            # the residual computation capacity of this edge server


class CommunicateDevice:

    def __init__(self):
        pass

    def on_message(self, client, userdata, message):
        global Run_Flag
        global request_users_info
        user_topic = message.topic
        update_reachable_device_topic_repeated.append(user_topic)

        if update_reachable_device_topic_unique.count(user_topic) == 0:
            update_reachable_device_topic_unique.append(user_topic)
            user_content = str(message.payload.decode("utf-8"))
            user_content_dict = eval(user_content)
            user_content_request_amount = user_content_dict[device_edge_server_topic]
            request_users_info[user_topic] = user_content_request_amount

        if len(update_reachable_device_topic_unique) == quantity_edge_server_clients:
            # print("This server gets these UE information ", request_users_info)
            Run_Flag = False
            update_reachable_device_topic_unique.clear()


    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.connected_flag = True
            for i in range(quantity_edge_server_clients):
                if edge_server_clients[i]["client"] == client:
                    topic = edge_server_clients[i]["sub_topic"]
                    break
            client.subscribe(topic)
        else:
            print("Bad connection Returned code = ",rc)
            client.loop_stop()

     
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


class AppDetection:

    thread_num = 0

    def __init__(self, t_num):
        self.thread_num = t_num

    def detect_pedestrian(self, thread_num):
        start = time.perf_counter()
        ap = argparse.ArgumentParser()
        ap.add_argument("-i", "--images", required=True, help="path to images directory")
        hog = cv2.HOGDescriptor()
        hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())  # set SVM

        image_Path="./images";
        # loop over the image paths
        for imagePath in paths.list_images(image_Path):
            image = cv2.imread(imagePath)
            image = imutils.resize(image, width=min(400, image.shape[1]))
            orig = image.copy()

            # detect people in the image：
            (rects, weights) = hog.detectMultiScale(image, winStride=(4, 4),
                padding=(8, 8), scale=1.05)

            # draw the original bounding boxes
            for (x, y, w, h) in rects:
                cv2.rectangle(orig, (x,y), (x+w, y+h), (0, 0, 255), 2)

            rects = np.array([[x, y, x+w, y+h] for (x, y, w, h) in rects])
            pick = non_max_suppression(rects, probs=None, overlapThresh=0.65)

            # draw the final bounding boxes
            for (xA, yA, xB, yB) in pick:
                cv2.rectangle(image, (xA, yA), (xB, yB), (0, 255, 0), 2)

            # show some information on the number of bounding boxes
            # filename = imagePath[imagePath.rfind("/")+1:]
            # print("[INFO] {}: {} original boxes, {} after suppression".format(filename, len(rects), len(pick)))

            # show the output imagesr
            # cv2.imshow("Before NMS" + str(count), orig)
            # cv2.imshow("After NMS" + str(self.thread_num), image)

        elapsed = (time.perf_counter()-start)
        print("~~~~~~~~~~ thread ", self.thread_num, "~~~~~~~~~~", elapsed)



mqtt.Client.connected_flag = False

communicate_device = CommunicateDevice()
communicate_device.create_multi_connections()

active_thread_num = threading.active_count()
print("current threads = ", active_thread_num)
print("Creating  Connections ", quantity_edge_server_clients, " user_node_clients")

print("Publishing ")

Run_Flag = True
try:
    while Run_Flag:
        client = edge_server_clients[0]["client"]
        pub_topic = edge_server_clients[0]["pub_topic"]
        if client.connected_flag:
            # print("edge server "+ str(2) + " is publishing~~~~~~~~~~" + pub_topic)
            client.publish(pub_topic, str(computation_resource_capacity))
        time.sleep(1)

    print("==========", sum(request_users_info.values()), "==========")

except KeyboardInterrupt:
    print("interrupted by keyboard")


for client in edge_server_clients:
   client.disconnect()
   client.loop_stop()

time.sleep(1)
