import paho.mqtt.client as mqtt
from datetime import datetime
import xmlrpc.client
from cvxpy import *
import numpy as np
import threading        
import logging
import pywifi
import random
import time


device_user_node_topic = "usernode5/request/area1"
# One usernode device has several edge server sub_topics and the quantity equal to the number of edge servers
user_node_clients = [
{"broker":"192.168.1.111","port":1883,"name":"blank","sub_topic":"edgeserver2/available/area1","pub_topic":device_user_node_topic},
{"broker":"192.168.1.111","port":1883,"name":"blank","sub_topic":"edgeserver4/available/area1","pub_topic":device_user_node_topic}
]

quantity_user_node_clients = len(user_node_clients)

update_reachable_device_topic_unique = []    # online device and the value is unique, so as to achieve the purpose of each iteration of the algorithm
update_reachable_device_topic_repeated = []  # remove the offline device, for edge server, the device is usernode and vice versa
pre_time = datetime.now()
round_count = 0                              # update round_count every round

# resource requirement and cost(designed to be scalable, can add communication resource, etc)
server_signal = {}                           # the server signal this device receive
usable_server_info = {}                      # the message this device subscribe, need to be global
request_resource_capacity = {}               # the message this UE need to sent

# probability to each server
probability_to_server = {}                   # store the probability that send request and offload to each reachable server
traffic_intensity_global = 0


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
        global usable_server_info
        global pre_time

        server_topic = message.topic
        if update_reachable_device_topic_unique.count(server_topic) == 0:
            update_reachable_device_topic_unique.append(server_topic)
            server_content = str(message.payload.decode("utf-8"))
            server_content_list = eval(server_content)
            usable_server_info[server_topic] = server_content_list  # store the subscribed client(edge device) available resources

        if len(update_reachable_device_topic_unique) == quantity_user_node_clients:
            #print("This UE get these Severs information ", usable_server_info)
            now_time = datetime.now()
            if now_time.second - pre_time.second >= 2:
                pre_time = now_time
                update_usable_server_info = UpdateUsableServerInfo()
                update_usable_server_info.update_server_info()

            strategic_decision = StrategicDecision()
            strategic_decision.generate_request_computation_capacity()
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


class DetectSignal:
    
    def __init__(self):
        pass

    def detect_wifi(self):
        global server_signal

        server_signal = {"edgeserver2/available/area1": 0, "edgeserver4/available/area1": 0, "edgeserver6/available/area1": 0}

        signal_info = {}

        wifi = pywifi.PyWiFi()
        iface = wifi.interfaces()[0]

        iface.scan()
        time.sleep(3)
        result = iface.scan_results()

        for i in range(len(result)):
            signal_info[result[i].ssid] = result[i].signal

        for key in server_signal.keys():
            server_signal[key] = signal_info[key]

        print(server_signal)


class UpdateUsableServerInfo:

    def __init__(self):
        pass

    def update_server_info(self):
        global usable_server_info
        global server_signal

        detect_signal = DetectSignal()
        detect_signal.detect_wifi()

        low_signal_server = set()

        for ssid in server_signal.keys():
            if server_signal[ssid] < -60:
                low_signal_server.add(ssid)
            if server_signal[ssid] >= -60 and ssid in low_signal_server:
                low_signal_server.remove(ssid)

        for server_key in usable_server_info.keys():
            if server_key in low_signal_server:
                usable_server_info[server_key][0] = 0
                usable_server_info[server_key][1] = 1


class GenerateWorkload:

    traffic_intensity = 0

    def __init__(self, t_intensity):
        self.traffic_intensity = t_intensity

    def user_node_workload(self):
        return self.traffic_intensity


class StrategicDecision:

    def __init__(self):
        pass

    def user_node_measure(self):
        global usable_server_info
        global probability_to_server
        constraints = []
        y = Variable()

        # Constraint: the offload probability of all edge servers connected to the UE between [0,1], and the sum is 1
        probability_server2 = Variable()
        probability_server4 = Variable()
        constraints = [probability_server2 >= 0, probability_server2 <= 1, probability_server4 >=0, probability_server4 <=1]

        probability_total = 0
        probability_total = probability_server2 + probability_server4

        constraints.append(probability_total == 1)

        temp1_total = 0
        temp2_total = 0
        generate_workload = GenerateWorkload(2400)
        for server in usable_server_info.keys():
            if server == "edgeserver2/available/area1":
                temp1_total += probability_server2 / ( usable_server_info[server][0] * ( 1 - usable_server_info[server][1] ) ** 2 )
                temp2_total += probability_to_server[server] / ( usable_server_info[server][0] * ( 1 - usable_server_info[server][1] ) ** 2 )
            if server == "edgeserver4/available/area1":
                temp1_total += probability_server4 / ( usable_server_info[server][0] * ( 1 - usable_server_info[server][1] ) ** 2 )
                temp2_total += probability_to_server[server] / ( usable_server_info[server][0] * ( 1 - usable_server_info[server][1] ) ** 2 )

        constraints.append(temp1_total <= temp2_total)

        average_computing_delay = 0
        for server in usable_server_info.keys():
            if server == "edgeserver2/available/area1":
                temp = usable_server_info[server][0] * ( 1 - usable_server_info[server][1] ) / ( generate_workload.user_node_workload() ) + probability_to_server[server]
                average_computing_delay +=  temp * ( temp - probability_server2 ) ** (-1)
            if server == "edgeserver4/available/area1":
                temp = usable_server_info[server][0] * ( 1 - usable_server_info[server][1] ) / ( generate_workload.user_node_workload() ) + probability_to_server[server]
                average_computing_delay +=  temp * ( temp - probability_server4 ) ** (-1)

        constraints.append(y >= average_computing_delay)
        obj = Minimize(y)

        prob = Problem(obj, constraints)
        prob.solve()

        server_probability = {}
        for server in usable_server_info.keys():
            if server == "edgeserver2/available/area1":
                server_probability[server] = probability_server2.value
            if server == "edgeserver4/available/area1":
                server_probability[server] = probability_server4.value

        return server_probability

    # compute probability of every connection between UE and BS
    def dto_ux_compute_probability(self, beta):
        global probability_to_server
        global round_count 
        server_probability = {}
        # initial p, assume that every connection between UE and BS has the same probability p
        if round_count == 0:
            for server in usable_server_info.keys():
                server_probability[server] = 1 / len(usable_server_info)
            probability_to_server = server_probability
            round_count += 1
        else:
            server_probability = self.user_node_measure()
            for server in usable_server_info.keys():
                probability_to_server[server] = ( 1 - beta ) * probability_to_server[server] + beta * server_probability[server]
            
            round_count += 1

    # This method can be improved to request several different kinds of resources
    def generate_request_computation_capacity(self):
        # beta can be assigned with [0.01,0.05,0.1,0.2,0.5]
        global request_resource_capacity
        beta = 0.2
        generate_workload = GenerateWorkload(2400)
        traffic_intensity = generate_workload.user_node_workload()
        self.dto_ux_compute_probability(beta)
        for server in usable_server_info.keys():
            request_resource_capacity[server] = traffic_intensity * probability_to_server[server]

        return


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
            client.publish(pub_topic, str(request_resource_capacity))
        time.sleep(1.5)
except KeyboardInterrupt:
    print("interrupted  by keyboard")

for i in range(quantity_user_node_clients):
    client = mqtt.Client(user_node_clients[i]["client_id"])
    client.loop_stop()

time.sleep(1)
