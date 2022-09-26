import logging
from pprint import pprint

from hetzner.robot import Robot

from cloudinventario.helpers import CloudCollector

# TEST MODE
TEST = 0

def setup(name, config, defaults, options):
  return CloudCollectorHetznerRobot(name, config, defaults, options)

class CloudCollectorHetznerRobot(CloudCollector):

  def __init__(self, name, config, defaults, options):
    self.ERRORS = ['401']
    super().__init__(name, config, defaults, options)

  def check_permission(self, client, error):
    if str(error.status) in self.ERRORS:
      print("Don't have permission for service, stopped collecting user/client: {}, because: {}".format(client.conn.user, str(error)))
      logging.warning("Don't have permission for service, stopped collecting user/client:{}, because: {}".format(client.conn.user, str(error)))
      return True
    else:
      return False

  def _login(self):
    self.user = self.config['user']
    self.password = self.config['pass']

    logging.info("logging in with user={}".format(self.user))
    self.client = Robot(self.user, self.password)
    return self.client

  def _fetch(self, collect):
    data = []
    for server in list(self.client.servers):
      data.append(self._process_server(server))

    logging.info("collected {} data".format(len(data)))
    return data

  def _process_server(self, server):
    # Subnets
    subnets_data = []
    for subnet in list(server.subnets):
      subnets_data.append({
        'specific_ip': subnet.net_ip, # IP address
        'gateway': subnet.gateway,
        'mask': subnet.mask,
        'ip': subnet.server_ip, # Servers main IP address
        'ipv6': subnet.is_ipv6,
        'failover': subnet.failover, # True if subnet is a failover subnet
        'locked': subnet.locked, # Status of locking
        'traffic_hourly': subnet.traffic_hourly,
        'traffic_daily': subnet.traffic_daily,
        'traffic_monthly': subnet.traffic_monthly,
      })

    # IPs
    networks_data = []
    for ip in list(server.ips):
      networks_data.append({
        'ip': ip.server_ip,
        'specific_ip': ip.ip,
        'locked': ip.locked,
        'subnet_ip': ip.subnet_ip,
        'mac_address': ip.separate_mac, # Separate MAC address, if not set null
        'traffic_hourly': ip.traffic_hourly,
        'traffic_daily': ip.traffic_daily,
        'traffic_monthly': ip.traffic_monthly,
      })

    # Reverse DNS
    rdns_data = []
    for rdns_item in list(server.rdns):
      rdns_data.append({
        'ip': rdns_item.ip,
        'ptr': rdns_item.ptr, # PTR record
      })

    server_data = {
      "uniqueid": server.number, 
      "name": server.name,
      "primary_ip": server.ip,
      "status": server.status,
      "is_on": (server.status == "in process"),
      "cluster": server.datacenter,
      "owner": self.user,

      "networks": networks_data,
      "subnets": subnets_data,
      "rdns": rdns_data,

      "product": server.product,
      "traffic": server.traffic,
      "cancelled": server.cancelled,
      "is_vserver": server.is_vserver, 
      "paid_until": server.paid_until, 
    }
    return self.new_record('server', server_data, server.__dict__)

  def _logout(self):
    self.client = None
