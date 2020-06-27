import paho.mqtt.client as mqtt
from datetime import datetime
from cvxpy import *
import numpy as np
import threading
import logging
import random
import time


device_controller_topic = "controller/area1"
# One usernode device has several edge server sub_topics and the quantity equal to the number of edge servers
user_node_clients = [
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode1/request/area1","pub_topic":device_controller_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode3/request/area1","pub_topic":device_controller_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode5/request/area1","pub_topic":device_controller_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode7/request/area1","pub_topic":device_controller_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode9/request/area1","pub_topic":device_controller_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode11/request/area1","pub_topic":device_controller_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode13/request/area1","pub_topic":device_controller_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode15/request/area1","pub_topic":device_controller_topic},
{"broker":"192.168.0.103","port":1883,"name":"blank","sub_topic":"usernode17/request/area1","pub_topic":device_controller_topic},
]
edge_server_clients = [
{"broker":"192.168.1.103","port":1883,"name":"blank","sub_topic":"edgeserver2/available/area1","pub_topic":device_controller_topic},
{"broker":"192.168.1.103","port":1883,"name":"blank","sub_topic":"edgeserver4/available/area1","pub_topic":device_controller_topic},
{"broker":"192.168.1.103","port":1883,"name":"blank","sub_topic":"edgeserver6/available/area1","pub_topic":device_controller_topic},
]


quantity_user_node_clients = len(user_node_clients)

update_reachable_device_topic_unique = []    # online device and the value is unique, so as to achieve the purpose of each iteration of the algorithm
update_reachable_device_topic_repeated = []  # remove the offline device, for edge server, the device is usernode and vice versa
round_count = 0                              # update round_count every round

# resource requirement and cost(designed to be scalable, can add communication resource, etc)
request_users_info = {}                      # the message this device subscribe

request_resource_capacity = {}               # the message this UE need to sent
computation_resource_capacity_edge2 = 11600  # Assume that controller know the computation capacity of every edge server
computation_resource_capacity_edge4 = 6600
computation_resource_capacity_edge6 = 15100


# probability to each server
probability_to_server = {}  # the probability that send request and offload to each reachable server, and the data structure is {server1:[],server2:[]}



class CommunicateDevice:

    def __init__(self):
        pass

    def on_message(self, client, userdata, message):
        global request_users_info
        user_topic = message.topic
        update_reachable_device_topic_repeated.append(user_topic)

        if update_reachable_device_topic_unique.count(user_topic) == 0 and user_topic != device_controller_topic:
            update_reachable_device_topic_unique.append(user_topic)
            user_content = str(message.payload.decode("utf-8"))
            user_content_digital = eval(user_content)
            request_users_info[user_topic] = user_content_digital

        if len(update_reachable_device_topic_unique) == quantity_user_node_clients: 
            #print("This controller gets these UE information ", request_users_info)
            strategic_decision = StrategicDecision()
            strategic_decision.central_algorithm_scheme()
            time.sleep(1)
            update_reachable_device_topic_unique.clear()

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

    def central_algorithm_scheme(self):
        global probability_to_server
        constraints = []
        y = Variable()

        # Constraint: the offload probability of all edge servers connected to the UE between [0,1], and the sum is 1
        probability_usernode1_server2 = Variable()
        
        probability_usernode3_server2 = Variable()
        probability_usernode3_server4 = Variable()
        
        probability_usernode5_server2 = Variable()
        probability_usernode5_server4 = Variable()
        
        probability_usernode7_server2 = Variable()
        probability_usernode7_server6 = Variable()
        
        probability_usernode9_server2 = Variable()
        probability_usernode9_server4 = Variable()
        probability_usernode9_server6 = Variable()
        
        probability_usernode11_server2 = Variable()
        probability_usernode11_server4 = Variable()
        probability_usernode11_server6 = Variable()
        
        probability_usernode13_server4 = Variable()

        probability_usernode15_server4 = Variable()
        probability_usernode15_server6 = Variable()

        probability_usernode17_server6 = Variable()

        constraints = [probability_usernode1_server2 >= 0, probability_usernode1_server2 <= 1,
                       probability_usernode3_server2 >= 0, probability_usernode3_server2 <= 1, probability_usernode3_server4 >=0, probability_usernode3_server4 <=1,
                       probability_usernode5_server2 >= 0, probability_usernode5_server2 <= 1, probability_usernode5_server4 >=0, probability_usernode5_server4 <=1,
                       probability_usernode7_server2 >= 0, probability_usernode7_server2 <= 1, probability_usernode7_server6 >=0, probability_usernode7_server6 <=1,
                       probability_usernode9_server2 >= 0, probability_usernode9_server2 <= 1, probability_usernode9_server4 >=0, probability_usernode9_server4 <=1, 
                       probability_usernode9_server6 >=0, probability_usernode9_server6 <=1,
                       probability_usernode11_server2 >= 0, probability_usernode11_server2 <= 1, probability_usernode11_server4 >=0, probability_usernode11_server4 <=1, 
                       probability_usernode11_server6 >=0, probability_usernode11_server6 <=1,
                       probability_usernode13_server4 >=0, probability_usernode13_server4 <=1,
                       probability_usernode15_server4 >=0, probability_usernode15_server4 <=1, probability_usernode15_server6 >=0, probability_usernode15_server6 <=1,
                       probability_usernode17_server6 >=0, probability_usernode17_server6 <=1
                       ]

        probability_total_usernode1 = probability_usernode1_server2
        probability_total_usernode3 = probability_usernode3_server2 + probability_usernode3_server4
        probability_total_usernode5 = probability_usernode5_server2 + probability_usernode5_server4
        probability_total_usernode7 = probability_usernode7_server2 + probability_usernode7_server6
        probability_total_usernode9 = probability_usernode9_server2 + probability_usernode9_server4 + probability_usernode9_server6
        probability_total_usernode11 = probability_usernode11_server2 + probability_usernode11_server4 + probability_usernode11_server6
        probability_total_usernode13 = probability_usernode13_server4
        probability_total_usernode15 = probability_usernode15_server4 + probability_usernode15_server6
        probability_total_usernode17 = probability_usernode17_server6

        constraints.append(probability_total_usernode1 == 1)
        constraints.append(probability_total_usernode3 == 1)
        constraints.append(probability_total_usernode5 == 1)
        constraints.append(probability_total_usernode7 == 1)
        constraints.append(probability_total_usernode9 == 1)
        constraints.append(probability_total_usernode11 == 1)
        constraints.append(probability_total_usernode13 == 1)
        constraints.append(probability_total_usernode15 == 1)
        constraints.append(probability_total_usernode17 == 1)


        edge_server_workload = {}
        edge_server_workload["edgeserver2/available/area1"] = probability_usernode1_server2 * request_users_info["usernode1/request/area1"] + \
                                                              probability_usernode3_server2 * request_users_info["usernode3/request/area1"] + \
                                                              probability_usernode5_server2 * request_users_info["usernode5/request/area1"] + \
                                                              probability_usernode7_server2 * request_users_info["usernode7/request/area1"] + \
                                                              probability_usernode9_server2 * request_users_info["usernode9/request/area1"] + \
                                                              probability_usernode11_server2 * request_users_info["usernode11/request/area1"]
        
        edge_server_workload["edgeserver4/available/area1"] = probability_usernode3_server4 * request_users_info["usernode3/request/area1"] + \
                                                              probability_usernode5_server4 * request_users_info["usernode5/request/area1"] + \
                                                              probability_usernode9_server4 * request_users_info["usernode9/request/area1"] + \
                                                              probability_usernode11_server4 * request_users_info["usernode11/request/area1"] + \
                                                              probability_usernode13_server4 * request_users_info["usernode13/request/area1"] + \
                                                              probability_usernode15_server4 * request_users_info["usernode15/request/area1"]
        
        edge_server_workload["edgeserver6/available/area1"] = probability_usernode7_server6 * request_users_info["usernode7/request/area1"] + \
                                                              probability_usernode9_server6 * request_users_info["usernode9/request/area1"] + \
                                                              probability_usernode11_server6 * request_users_info["usernode11/request/area1"] + \
                                                              probability_usernode15_server6 * request_users_info["usernode15/request/area1"] + \
                                                              probability_usernode17_server6 * request_users_info["usernode17/request/area1"]

        # This statement can define the capacity of differnent edge server
        constraints.append(edge_server_workload["edgeserver2/available/area1"] <= computation_resource_capacity_edge2)
        constraints.append(edge_server_workload["edgeserver4/available/area1"] <= computation_resource_capacity_edge4)
        constraints.append(edge_server_workload["edgeserver6/available/area1"] <= computation_resource_capacity_edge6)

        temp = 0
        total_task_num = sum(list(request_users_info.values()))

        temp += (1 - edge_server_workload["edgeserver2/available/area1"] / computation_resource_capacity_edge2) ** (-1) - 1
        temp += (1 - edge_server_workload["edgeserver4/available/area1"] / computation_resource_capacity_edge4) ** (-1) - 1
        temp += (1 - edge_server_workload["edgeserver6/available/area1"] / computation_resource_capacity_edge6) ** (-1) - 1

        target = temp / total_task_num

        obj = Minimize(target)

        prob = Problem(obj, constraints)
        prob.solve()


        probability_to_server["usernode1/request/area1"] = {"edgeserver2/available/area1":float(probability_usernode1_server2.value)}

        probability_to_server["usernode3/request/area1"] = {"edgeserver2/available/area1":float(probability_usernode3_server2.value), 
                                                            "edgeserver4/available/area1":float(probability_usernode3_server4.value)
                                                            }

        probability_to_server["usernode5/request/area1"] = {"edgeserver2/available/area1":float(probability_usernode5_server2.value), 
                                                            "edgeserver4/available/area1":float(probability_usernode5_server4.value)
                                                            }

        probability_to_server["usernode7/request/area1"] = {"edgeserver2/available/area1":float(probability_usernode7_server2.value), 
                                                            "edgeserver6/available/area1":float(probability_usernode7_server6.value)
                                                            }

        probability_to_server["usernode9/request/area1"] = {"edgeserver2/available/area1":float(probability_usernode9_server2.value), 
                                                            "edgeserver4/available/area1":float(probability_usernode9_server4.value), 
                                                            "edgeserver6/available/area1":float(probability_usernode9_server6.value)
                                                            }

        probability_to_server["usernode11/request/area1"] = {"edgeserver2/available/area1":float(probability_usernode11_server2.value), 
                                                             "edgeserver4/available/area1":float(probability_usernode11_server4.value), 
                                                             "edgeserver6/available/area1":float(probability_usernode11_server6.value)
                                                            }

        probability_to_server["usernode13/request/area1"] = {"edgeserver4/available/area1":float(probability_usernode13_server4.value)}

        probability_to_server["usernode15/request/area1"] = {"edgeserver4/available/area1":float(probability_usernode15_server4.value), 
                                                             "edgeserver6/available/area1":float(probability_usernode15_server6.value)
                                                            }

        probability_to_server["usernode17/request/area1"] = {"edgeserver6/available/area1":float(probability_usernode17_server6.value)}

        return probability_to_server



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
        pub_topic = device_controller_topic
        if client.connected_flag:
            #print("contro1ler device " + " is publishing~~~~~~~~~~")
            client.publish(pub_topic, str(probability_to_server))
            time.sleep(0.5)
        time.sleep(1)

except KeyboardInterrupt:
    print("interrupted  by keyboard")

for client in user_node_clients:
   client.disconnect()
   client.loop_stop()

time.sleep(1)
