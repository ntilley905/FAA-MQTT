#!/usr/bin/env python3

import yaml
import time
import paho.mqtt.client as paho
import requests
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
            except ConnectionError:
                if time.time() > start_time + connection_timeout:
                    raise Exception('Connection error after {} seconds'.format(connection_timeout))
                else:
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
            #if there are delays set the generic delay to true and then check for more
            #set the number of delays we need to check in a variable
            datadelaynum = datadict["DelayCount"]
            n = 0
            #find GDP or GS
            while n != datadelaynum:
                #really clunky way to check, need to fix. the FAA API is written so that if there is a GDP, departure, or arrival delay, the type key will be present. but if there
                #is a GS there will be no type key. so I'm using try to check for the keys and handle each case differently
                if n > 4:
                    print("Something weird happened and N was greater than 4.")
                    break
                else:
                    try:
                        if datadict["Status"][n]["Type"] == "Ground Delay":
                            pubdatagdp = True
                            pass
                        else:
                            pubdatagdp = False
                    except KeyError:
                        pass
                    except IndexError:
                        print("IndexError occured. Total delay reasons were " + str(datadelaynum) + " and we were on iteration " + str(n))
                        print(datadict)
                        pubdatagdp = "Error"
                        pass
                    except:
                        print("Unexpected error occured during GDP test for " + airports[apt] + ":", sys.exc_info()[0])
                        pubdatagdp = "Error"
                        pass
                    try:
                        pubdataend = datadict["Status"][n]["EndTime"]
                        pubdatags = True
                        pass
                    except KeyError:
                        pass
                    except IndexError:
                        print("IndexError occured. Total delay reasons were " + str(datadelaynum) + " and we were on iteration " + str(n))
                        print(datadict)
                        pubdatags = "Error"
                        pass
                    except:
                        print("Unexpected error occured during GDP test for " + airports[apt] + ":", sys.exc_info()[0])
                        pubdatags = "Error"
                        pass
                    finally:
                        n += 1
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
