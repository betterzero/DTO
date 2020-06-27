import paho.mqtt.client as mqtt
from cvxpy import *
import numpy as np
import threading        
import logging
import random
import time


device_user_node_topic = "usernode1/request/area1"
# One usernode device has several edge server sub_topics and the quantity equal to the number of edge servers
user_node_clients = [
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"controller/area1","pub_topic":device_user_node_topic}
]

quantity_user_node_clients = len(user_node_clients)

update_reachable_device_topic_unique = []    # online device and the value is unique, so as to achieve the purpose of each iteration of the algorithm
update_reachable_device_topic_repeated = []  # remove the offline device, for edge server, the device is usernode and vice versa
round_count = 0                              # update round_count every round

# the message this device subscribe from controller, containing the details about the resource request, 
# the data structure is {server1:probability1, server2:probability2}
usable_server_info = {}
request_server_resource_amount = {}          # resource request for reachable edge server

# probability to each server
probability_to_server = {}                   # the probability that send request and offload to each reachable server


class CommunicateDevice:

    def __init__(self):
        pass

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.connected_flag = True
            for i in range(quantity_user_node_clients):
                if user_node_clients[i]["client"] == client:
                    topic = user_node_clients[i]["sub_topic"]
                    break
            client.subscribe(topic)
        else:
            print("Bad connection Returned code = ",rc)
            client.loop_stop()

    def on_message(self, client, userdata, message):
        global usable_server_info
        controller_topic = message.topic

        if update_reachable_device_topic_unique.count(controller_topic) == 0:
            update_reachable_device_topic_unique.append(controller_topic)
            server_content = str(message.payload.decode("utf-8"))
            server_content_dict_servers = eval(server_content)
            server_content_dict_device = server_content_dict_servers[device_user_node_topic]
            usable_server_info = server_content_dict_device

        if len(update_reachable_device_topic_unique) == quantity_user_node_clients:
            # print("This UE get information from controller: ", usable_server_info)
            generate_workload = GenerateWorkload(300)
            generate_workload.generate_request_resource_amount()
            time.sleep(1)
            update_reachable_device_topic_unique.clear()

    def on_disconnect(self, client, userdata, rc):
        print("client disconnected")

    def create_multi_connections(self):
        for i in range(quantity_user_node_clients):
            cname = "client"+str(i)
            t = int(time.time())
            client_id = cname+str(t)
            client = mqtt.Client(client_id)
            user_node_clients[i]["client"] = client 
            user_node_clients[i]["client_id"] = client_id
            user_node_clients[i]["cname"] = cname
            broker = user_node_clients[i]["broker"]
            port = user_node_clients[i]["port"]
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


class GenerateWorkload:

    traffic_intensity = 0

    def __init__(self, t_intensity):
        self.traffic_intensity = t_intensity

    def user_node_workload(self):
        return self.traffic_intensity

    def generate_request_resource_amount(self):
        global request_server_resource_amount

        for server in usable_server_info:
            request_server_resource_amount[server] = self.traffic_intensity * usable_server_info[server]



mqtt.Client.connected_flag = False

communicate_device = CommunicateDevice()
communicate_device.create_multi_connections()

active_thread_num = threading.active_count()
print("current threads = ", active_thread_num)
print("Creating  Connections ", quantity_user_node_clients, " user_node_clients")

print("Publishing ")

Run_Flag = True
try:
    while Run_Flag:
        client = user_node_clients[0]["client"]
        pub_topic = device_user_node_topic
        generate_workload = GenerateWorkload(300)
        if client.connected_flag:
            # print("user device "+ str(1) + " is publishing, target to controller")
            client.publish(pub_topic, generate_workload.user_node_workload())
            time.sleep(0.5)
            if len(usable_server_info) != 0:
                # print("user device "+ str(1) + " is publishing, target to edge server")
                client.publish("usernode1/request/area1/edge", str(request_server_resource_amount))
                time.sleep(0.5)
        time.sleep(1)

except KeyboardInterrupt:
    print("interrupted  by keyboard")

for client in user_node_clients:
   client.disconnect()
   client.loop_stop()

time.sleep(1)
