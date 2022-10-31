#!/usr/bin/env python3
import os
import sys
import logging
import argparse
import yaml
import requests
import time
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
    response_results = []
    collectors = []

    print(f"Load runner len_col={len(config['collectors'])}, len_end={len(config['endpoints'])}, max_process={config['process']['tasks']}")
    for collector in config['collectors']:
        index = 0
        while (1):
            host = config['endpoints'][index]['host']
            port = config['endpoints'][index]['port']
            response = status(host, port)
            data = response.json()
            if data['ready']:
                print(f"Sending collector={collector} to host, port={(host, port)}")
                response = collect(host, port, config['collectors'], collector)
                print(f"\tGet respond='{response.json()['status']}' description='{str(response.json()['description'])}'")
                if response.json()['code'] == 200:
                    response_results.append(response.json())
                    collectors.append((host, port, response.json()['IDs']))
                    break
            if (index + 1) == len(config['endpoints']):
                index = -1 
                time.sleep((250 + random.randint(1,250)) * 0.001)
            index += 1

    if args.wait:
        while(len(collectors) > 0):
            host, port, IDs = collectors.pop(0)
            key_to_remove = []
            for key in IDs.keys():
                response = status_collector(host, port, IDs[key])
                if response.json()['status'] in ['success', 'error']:
                    print(f"Get collector={key} host:port={host}:{port} status={response.json()['status']}")
                    key_to_remove.append(key)
            for key in key_to_remove:
                del IDs[key]
            if len(IDs) != 0:
                collectors.append((host, port, IDs))
    return response_results

args = getArgs()
ret = main(args)
print(f"Collected {len(ret)} collectors")
sys.exit(ret)
