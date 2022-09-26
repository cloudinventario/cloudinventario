import concurrent.futures
import logging, re, sys, asyncio, time
from pprint import pprint

from cloudinventario.cloudinventario import CloudInventario
from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventarioLibcloudDNS(resource, collector)

# resource collector using libcloud DNS module
class CloudInventarioLibcloudDNS(CloudInvetarioResource):

  def __init__(self, resource, collector):

    self.libcloud_config = {
      'driver_params': {},
      'driver': {
        'dns': 'route53',
      },
    }
    super().__init__(resource, collector)

  def _login(self, session):
    self.session = session

    # driver config
    config = {**self.libcloud_config}

    config['key'] = self.collector.config['access_key']
    config['secret'] = self.collector.config['secret_key']
    config['driver_params']['region'] = self.collector.config['region']
    config['driver_params']['token'] = self.collector.config.get('session_token')
    config['collect'] = ['dns']

    self.handle = CloudInventario.loadCollectorModule("libcloud", self.collector.name, config,
                    self.collector.defaults, self.collector.options)
    self.handle.resource_login(config)
    return

  def _fetch(self):
    return self.handle._resource_fetch()
