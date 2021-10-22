import concurrent.futures
import logging, re, sys, asyncio
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

    # Get node from proxmox
    nodes = self.proxmox.cluster.resources.get(type='node')

    # Get all types of vm (qemu, lxc, openvz...) from proxmox
    vms = self.proxmox.cluster.resources.get(type='vm')

    records = vms + nodes
    for rec in records:
      data.append(self._process_vm(rec))

    logging.info("Collected {} datas".format(len(data)))
    return data

  def _process_vm(self, rec):
    record_type = {
      'lxc': 'container',
      'gemu': 'vm',
      'node': 'node',
    }
    logging.info("new {} node={} name={}".format(rec['type'], rec["node"], rec.get("name")))

    rec["disks_list"] = self.proxmox.get(f'nodes/{rec["node"]}/disks/list')
    rec["network"] = self.proxmox.get(f'nodes/{rec["node"]}/network')
    rec["storages"] = self.proxmox.get(f'nodes/{rec["node"]}/storage')
    storage = sum([storage.get('total') for storage in rec.get('storages', [])])

    vm_data = {
      # "cluster": rec["vdcName"],
      # "description": rec.get("Description"),
      # "os": rec["guestOs"],
      # "owner": rec["ownerName"],

      "created": rec.get("uptime"),
      "id": rec.get("id"),
      "project": rec.get("node"),
      "name": rec.get("name"),
      "cpus": rec.get("maxcpu"),
      "type": rec.get("type"),
      "memory": rec.get("maxmem"),
      "disks": rec.get("maxdisks"),
      "networks": rec.get("network"),
      "primary_ip": rec.get("network")[0].get("address"),
      "storage": storage,
      "storages": rec.get("storages"),
      "status": rec.get("status"),
      "is_on": 1 if rec.get("status") == 'running' else 0, 
    }

    type = record_type[rec['type']] if rec.get("type") in record_type else rec.get("type")
    return self.new_record(type, vm_data, rec)

  def _logout(self):
    self.proxmox = None
