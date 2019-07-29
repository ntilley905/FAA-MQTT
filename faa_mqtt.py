#!/usr/bin/env python3

import yaml
import time
import paho.mqtt.client as paho
import requests
import json
import sys
with open("config.yaml", "r") as yamlconfig:
    config = yaml.safe_load(yamlconfig)

client= paho.Client("FAADelayProgram") #create client object client1.on_publish = on_publish #assign function to callback client1.connect(broker,port) #establish connection client1.publish("house/bulb1","on")
client.username_pw_set(config['mqtt_user'], config['mqtt_pass'])
client.connect(config['mqtt_broker'])#connect
pubdatadelay = False
pubdatagdp = False
pubdatags = False
pubdataend = "None"

#hard coded airports to check, want to change this to somehow be variable
airports = { 1:"ATL", 2:"DTW", 3:"LGA", 4:"JFK", 5:"BOS", 6:"ORD" }
#define the JSON message to be published later
message = { "Airport":"ABC", "Delay":False, "GroundDelay":False, "GroundStop":False, "EndTime":"Blank" }

#main loop
while True:
    
    for apt in airports:
        apiurl = "https://soa.smext.faa.gov/asws/api/airport/status/" + airports[apt]
        data = requests.get(apiurl)
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
        message["Airport"] = airports[apt]
        message["Delay"] = pubdatadelay
        message["GroundDelay"] = pubdatagdp
        message["GroundStop"] = pubdatags
        message["EndTime"] = pubdataend
        jsonmessage = json.dumps(message)
        topic = "delays/" + airports[apt]
        client.publish(topic,jsonmessage)
    time.sleep(60)
    

#####
