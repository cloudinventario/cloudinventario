import concurrent.futures
import logging
import re
import sys
import asyncio
from pprint import pprint

from proxmoxer import ProxmoxAPI

from cloudinventario.helpers import CloudCollector

# TEST MODE
TEST = 0


def setup(name, config, defaults, options):
    return CloudCollectorProxmox(name, config, defaults, options)


class CloudCollectorProxmox(CloudCollector):

    def __init__(self, name, config, defaults, options):
        super().__init__(name, config, defaults, options)

    def _config_keys():
        return {
            host: 'Host ip, Required',
            user: 'User name, Required',
            passwd: 'Password, Required',
            port: 'Port',
            verify_ssl_certs: 'Verify ssl certs, Required',
        }

    def _login(self):
        host = self.config['host']
        user = self.config['user']
        passwd = self.config['pass']
        port = self.config['port']
        ssl = self.config['verify_ssl_certs']

        self.proxmox = ProxmoxAPI(
            host,
            user=user,
            password=passwd,
            port=port,
            verify_ssl=ssl,
            service='PVE',
        )

        logging.info("logging config Proxmox in host={}".format(host))
        return self.proxmox

    def _fetch(self, collect):
        data = []
        # Get node
        nodes = self.proxmox.cluster.resources.get(type='node')
        # Get vm, container (qemu, lxc, openvz...)
        vms = self.proxmox.cluster.resources.get(type='vm')

        records = vms + nodes
        for rec in records:
            data.append(self._process_vm(rec))

        logging.info("Collected {} datas".format(len(data)))
        return data

    def _process_vm(self, rec):
        MB = (10**-6)

        # Load disks_list
        rec["disks_list"] = self.proxmox.get(f'nodes/{rec["node"]}/disks/list')
        # Load and process network
        rec["network"] = [{
            "ip": network.get('address'),
            "gateway": network.get('gateway'),
            "type": network.get('type'),
            "name": network.get('iface'),
            "active": network.get('active')
        } for network in self.proxmox.get(f'nodes/{rec["node"]}/network')]
        # Load and process storages
        rec["storages"] = [{
            "name": storage.get('storage'),
            "active": storage.get('active'),
            "type": storage.get('type'),
            "content": storage.get('content'),
            "capacity": storage.get('total') * MB if 'total' in rec else 0,
            "used": storage.get('used') * MB if 'used' in rec else 0,
            "free": storage.get('avail') * MB if 'avail' in rec else 0,
        } for storage in self.proxmox.get(f'nodes/{rec["node"]}/storage')]

        # Sum and conversion to MB
        storage = sum([storage.get('capacity') for storage in rec.get('storages', [])]) * MB if len(rec.get('storages', [])) > 0 else 0
        maxmem = rec['maxmem'] * MB if 'maxmem' in rec else 0
        maxdisk = rec['maxdisk'] * MB if 'maxdisk' in rec else 0

        logging.info("new {} node={} name={}".format(
            rec['type'], rec["node"], rec.get("name")))
        vm_data = {
            "created": rec.get("uptime"),
            "id": rec.get("id"),
            "project": rec.get("node"),
            "name": rec.get("name"),
            "cpus": rec.get("maxcpu"),
            "type": rec.get("type"),
            "memory": maxmem,
            "disks": maxdisk,
            "networks": rec.get("network"),
            "primary_ip": rec.get("network")[0].get("address"),
            "storage": storage,
            "storages": rec.get("storages"),
            "status": rec.get("status"),
            "is_on": 1 if rec.get("status", "") == 'running' else 0,
        }
        # NOT MAPPED
        # "cluster": rec["vdcName"],
        # "description": rec.get("Description"),
        # "os": rec["guestOs"],
        # "owner": rec["ownerName"],

        # Get real type of record (vm, container, node)
        record_type = {
            'lxc': 'container',
            'qemu': 'vm',
            'node': 'node',
        }
        type = record_type[rec['type']] if rec.get("type") in record_type else rec.get("type")
        return self.new_record(type, vm_data, rec)

    def _logout(self):
        self.proxmox = None
