import paho.mqtt.client as mqtt
import numpy as np
import threading
import logging
import random
import copy
import math
import time


device_user_node = "usernode5"
# One usernode device has several edge server sub_topics and the quantity equal to the number of edge servers
user_node_clients = [
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode3","pub_topic":device_user_node}
]

quantity_user_node_clients = len(user_node_clients)

update_reachable_device_topic_unique = []    # online device and the value is unique, so as to achieve the purpose of each iteration of the algorithm
update_reachable_device_topic_repeated = []  # remove the offline device, for edge server, the device is usernode and vice versa

# related user
related_user_workload = {"usernode1": 300, "usernode3": 2400, "usernode5": 2400, "usernode7": 500, "usernode9": 2400, "usernode11": 2400, "usernode13": 300, "usernode15": 2400}

# related_edge_user_pair
related_edge_user_pair = {"edgeserver2": ["usernode1", "usernode3", "usernode5", "usernode7", "usernode9", "usernode11"], \
                          "edgeserver4": ["usernode3", "usernode5", "usernode9", "usernode11", "usernode13", "usernode15"]}

# resource requirement and cost(designed to be scalable)
usable_server_info = {"edgeserver2": 11600, "edgeserver4": 6600}
per_task_resource_requirement = 100
device_user_node_workload = 2400
request_resource_capacity = {}

# probability to each server
probability_to_server = {}
delay_error = {}
delay_pre = 0
delay_cur = 0


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
            print("Bad connection Returned code = ", rc)
            client.loop_stop()

    def on_message(self, client, userdata, message):
        global probability_to_server
        global delay_error
        user_node_topic = message.topic

        if update_reachable_device_topic_unique.count(user_node_topic) == 0:
            update_reachable_device_topic_unique.append(user_node_topic)
            user_node_content = str(message.payload.decode("utf-8"))
            user_node_info_list = eval(user_node_content)
            probability_to_server = user_node_info_list[0]
            delay_error = user_node_info_list[1]

        if len(update_reachable_device_topic_unique) == quantity_user_node_clients:
            # print("This UE get these information ", probability_to_server)
            strategic_decision = StrategicDecision()
            strategic_decision.ndto_strategy()
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


class StrategicDecision:
    
    def __init__(self):
        pass

    def get_delay(self):
        delay_device = 0

        for server in usable_server_info.keys():
            total_workload = 0
            for user_node in related_edge_user_pair[server]:
                total_workload += probability_to_server[user_node][server] * related_user_workload[user_node]
            if usable_server_info[server] > total_workload:
                delay_device += probability_to_server[device_user_node][server] * per_task_resource_requirement / (usable_server_info[server] - total_workload)

        return delay_device

    def get_t(self, server_dic):
        total_mu = 0
        total_mu_sqrt = 0
        for BS in list(server_dic.keys()):
            total_mu += server_dic[BS]
            total_mu_sqrt += (server_dic[BS] ** 0.5)
        if total_mu_sqrt == 0:
            return -1
        t = ( total_mu - device_user_node_workload ) / total_mu_sqrt
        return t

    def best_reply(self, server_remain):
        global probability_to_server
        probability_device = {}
        server_remain = dict(sorted(server_remain.items(), key=lambda x: x[1], reverse=True))
        server_dic = copy.deepcopy(server_remain)
        t = self.get_t(server_dic)

        for server in list(reversed(list(server_dic.keys()))):
            if t >= server_dic[server] ** 0.5:
                probability_device[server] = 0
                del server_dic[server]

                t = self.get_t(server_dic)
                if t == -1:
                    return probability_to_server[device_user_node]
            else:
                break

        for server in list(server_dic.keys()):
            probability_device[server] =  ( server_dic[server] - t * ( server_dic[server] ** 0.5 ) ) / device_user_node_workload
        
        return probability_device

    def ndto_strategy(self):
        global probability_to_server
        global delay_error
        global delay_pre
        global delay_cur

        delay_pre = self.get_delay()

        server_remain = {}
        for server in usable_server_info.keys():
            total_use = 0
            # server that connected with user_node
            for user_node in related_edge_user_pair[server]:
                if user_node != device_user_node:
                    total_use += related_user_workload[user_node] * probability_to_server[user_node][server]
            if usable_server_info[server] >= total_use:
                server_remain[server] = usable_server_info[server] - total_use
            else:
                server_remain[server] = 0

        probability_device = self.best_reply(server_remain)

        for server in probability_device.keys():
            probability_to_server[device_user_node][server] = probability_device[server]

        delay_cur = self.get_delay()
        delay_error_value = math.fabs(delay_pre - delay_cur)
        delay_error[device_user_node] = delay_error_value
        delay_pre = delay_cur


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
        pub_topic = user_node_clients[0]["pub_topic"]
        if client.connected_flag:
            #print("user device "+ str(5) + " is publishing~~~~~~~~~~")
            pub_ndto_info = str([probability_to_server, delay_error])
            client.publish(pub_topic, pub_ndto_info)
        time.sleep(1.5)

except KeyboardInterrupt:
    print("interrupted  by keyboard")

for client in user_node_clients:
   client.disconnect()
   client.loop_stop()

time.sleep(1)
