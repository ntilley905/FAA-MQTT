#!/usr/bin/env python3

import yaml
import time
import paho.mqtt.client as paho
import requests
import requests.exceptions
import json
import sys
import atexit
connection_timeout = 30 # seconds
with open("config.yaml", "r") as yamlconfig:
    config = yaml.safe_load(yamlconfig)

def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        print("connected OK")
    else:
        print("Bad connection Returned code=",rc)
        client.bad_connection_flag=True

def on_disconnect(client, userdata, rc):
    print("disconnecting reason  "  +str(rc))
    client.connected_flag=False

def exit_function():
    print('Disconnecting...')
    client.publish("delays/status",'False',retain=True)
    client.loop_stop()
    client.disconnect()

atexit.register(exit_function)
paho.Client.connected_flag=False
paho.Client.bad_connection_flag=False
client= paho.Client("FAADelayProgram")
client.username_pw_set(config['mqtt_user'], config['mqtt_pass'])
client.on_connect=on_connect
client.on_disconnect=on_disconnect
print('Connecting to broker ',config['mqtt_broker'])
client.connect(config['mqtt_broker'])#connect
client.loop_start()
pubdatadelay = False
pubdatagdp = False
pubdatags = False
pubdataend = "None"

airports = config['airports']
print(airports)
#define the JSON message to be published later
message = { "Airport":"ABC", "Delay":False, "GroundDelay":False, "GroundStop":False, "EndTime":"Blank" }

#main loop
while True:
    while not client.connected_flag and not client.bad_connection_flag:
        print('Connecting...')
        time.sleep(30)
    if client.bad_connection_flag:
        client.loop_stop()
        raise SystemExit
    client.publish("delays/status","True",retain=True)
    for apt in airports:
        apiurl = "https://soa.smext.faa.gov/asws/api/airport/status/" + apt
        start_time = time.time()
        while True:
            try:
                data = requests.get(apiurl)
                break
            except requests.exceptions.ConnectionError:
                if time.time() > start_time + connection_timeout:
                    raise Exception('Connection error after {} seconds'.format(connection_timeout))
                else:
                    print("waiting...")
                    time.sleep(1)
        if data.status_code != 200:
            print('The API returned status code' + str(data.status_code))
            continue
        datadict = data.json()
        #set datadelay true or false based on API data
        datadelay = datadict["Delay"]

        if datadelay == False:
            #if no delays set everything to false
            pubdatadelay = False
            pubdatagdp = False
            pubdatags = False
            pubdataend = "None"
        else:
            pubdatadelay = True
            pubdatagdp = False
            pubdatags = False
            pubdataend = "None"
            #find GDP or GS
            for delay in datadict["Status"]:
                if delay["Type"] == "Ground Delay":
                    pubdatagdp = True
                elif delay["Type"] == "Ground Stop":
                    pubdatags = True
                    pubdataend = delay["EndTime"]
        message["Airport"] = apt
        message["Delay"] = pubdatadelay
        message["GroundDelay"] = pubdatagdp
        message["GroundStop"] = pubdatags
        message["EndTime"] = pubdataend
        jsonmessage = json.dumps(message)
        topic = "delays/" + apt
        client.publish(topic,jsonmessage,retain=True)
    time.sleep(60)


#####
