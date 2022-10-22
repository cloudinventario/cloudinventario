#!/usr/bin/env python3
import os
import sys
import logging
import argparse
import yaml
import requests

def getArgs():
    parser = argparse.ArgumentParser(description='CloudInventory args')
    parser.add_argument('-c', '--config', action='store', required=True, help='Config file')
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

def status_task(host, port, id):
    url = url_creator(host, port) + "/status/" + str(id)
    return requests.get(url=url)

def main(args):
    config = loadConfig(args.config)
    response_results = []

    print(f"Load runner len_col={len(config['collectors'])}, len_end={len(config['endpoints'])}, max_process={config['process']['tasks']}")
    for collector in config['collectors']:
        index = 0
        while (1):
            host = config['endpoints'][index]['host']
            port = config['endpoints'][index]['port']
            response = status(host, port)
            data = response.json()
            if data['not_finished_tasks'] < config['process']['tasks']:
                print(f"Sending collector={collector} to host, port={(host, port)}")
                response = collect(host, port, config['collectors'], collector)
                response_results.append(response.json())
                print(f"\tGet respond='{response.json()['status']}'")
                break
            else:
                if (index + 1) == len(config['endpoints']):
                    # print(f"Use all endpoints, staring again")
                    index = 0
                else:
                    index += 1
    return response_results

args = getArgs()
ret = main(args)
sys.exit(ret)
