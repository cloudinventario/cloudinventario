#!/usr/bin/env python3
import os
import sys
import logging
import argparse
import yaml
import requests
import time
import copy
import random

def getArgs():
    parser = argparse.ArgumentParser(description='CloudInventory args')
    parser.add_argument('-c', '--config', action='store', required=True, help='Config file')
    parser.add_argument('--wait', action='store_true', required=False, help='Wait for all collectors to be finished')
    args = parser.parse_args()
    return args

def loadConfig(config_file):
    with open(config_file) as file:
       return yaml.safe_load(file)

def url_creator(host, port):
    return "http://" + str(host) + ":" + str(port) 

def status(host, port):
    url = url_creator(host, port) + "/status"
    return requests.get(url=url)

def collect(host, port, collector, collector_name):
    json_data = {"collectors": {collector_name: collector[collector_name]}}
    url = url_creator(host, port) + "/collect"
    return requests.post(url=url, json=json_data)

def status_collector(host, port, id):
    url = url_creator(host, port) + "/status/" + str(id)
    return requests.get(url=url)

def main(args):
    config = loadConfig(args.config)
    collectors = []

    print(f"Load runner len_col={len(config['collectors'])}, len_end={len(config['endpoints'])}, max_process={config['process']['tasks']}")
    for collector in config['collectors']:
        index = 0
        while (1):
            host = config['endpoints'][index]['host']
            port = config['endpoints'][index]['port']

            status_response = status(host, port).json()
            if status_response['ready']:
                print(f"Sending collector={collector} to host:port={host}:{port}")
                collect_response = collect(host, port, config['collectors'], collector).json()

                print(f"[+] Get response={collect_response['status']} description={collect_response['description']}")
                if collect_response['code'] == 200:
                    collectors.append((host, port, collect_response['IDs'], collect_response['status']))
                    break
            if (index + 1) == len(config['endpoints']):
                index = -1 # always increased at the end of while
                time_to_wait = (250 + random.randint(1, 250)) # in seconds
                time.sleep(time_to_wait * 0.001)
            index += 1

    if args.wait:
        # Copy for return
        collectors_copy = copy.deepcopy(collectors)
        while(len(collectors_copy) > 0):
            host, port, IDs, status_value = collectors_copy.pop(0)
            key_to_remove = []
            
            for key in IDs.keys():
                status_response = status_collector(host, port, IDs[key]).json()
                if status_response['status'] in ['success', 'error']:
                    print(f"Get collector={key} host:port={host}:{port} status={status_response['status']}")
                    # Get success or error (not found in queue), store to remove collector
                    key_to_remove.append(key) 
            # Remove all collectors that are finished
            for key in key_to_remove:
                del IDs[key]
            # If remain collectors add into array at the end
            if len(IDs) != 0:
                collectors_copy.append((host, port, IDs, status_value))
    return collectors

args = getArgs()
ret = main(args)
print(f"Collected {len(ret)} collectors")
sys.exit(ret)
