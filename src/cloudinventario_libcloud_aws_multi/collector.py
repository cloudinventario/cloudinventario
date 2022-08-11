import concurrent.futures
import logging, re, sys, asyncio, time
from pprint import pprint

from cloudinventario.cloudinventario import CloudInventario
from cloudinventario.helpers import CloudCollector
from cloudinventario_amazon_aws_multi.collector import CloudCollectorAmazonAWSMulti

DRIVER_MAP = {
   'elb':     'lb',
   's3':      'storage',
   'ec2':     'vm',
   'route53': 'dns',
}

def setup(name, config, defaults, options):
  return CloudCollectorLibcloudAWSMulti(name, config, defaults, options)

class CloudCollectorLibcloudAWSMulti(CloudCollectorAmazonAWSMulti):

  def __init__(self, name, config, defaults, options):
    self.libcloud_config = {
      'driver_params': {},
      'driver': {v: k for k, v in DRIVER_MAP.items()},
    }

    super().__init__(name, config, defaults, options)

  def _loadCollectorModule(self, name, cred, defaults, options):
    # driver config

    config = {**self.libcloud_config}
    # map config
    defaults['owner'] = cred['account_id']	# TODO!

    config['key'] = cred['access_key']
    config['secret'] = cred['secret_key']
    config['driver_params']['region'] = cred['region']
    config['driver_params']['token'] = cred['session_token']
    config['collect'] = [DRIVER_MAP.get(k, k) for k in self.config['collect']]

    return CloudInventario.loadCollectorModule("libcloud", name, config, defaults, options)
