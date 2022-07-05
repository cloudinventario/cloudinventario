import concurrent.futures
import logging, re, sys, asyncio, time
from pprint import pprint

import boto3
import botocore.exceptions as aws_exception

from cloudinventario.helpers import CloudCollector, CloudInvetarioResourceManager

# TEST MODE
TEST = 0

def setup(name, config, defaults, options):
  return CloudInvetarioAmazonAWSResource(name, config, defaults, options)

class CloudInvetarioAmazonAWSResource(CloudCollector):

  def __init__(self, name, config, defaults, options):
    self.ERRORS = ['AccessDenied', 'UnauthorizedOperation']
    super().__init__(name, config, defaults, options)

  def check_permission(self, client, error):
    if type(error) == aws_exception.ClientError and error.response['Error']['Code'] in self.ERRORS:
      print("Don't have permission for service, stopped collecting user: {}, because: {}".format(client, error.response['Error']['Code']))
      logging.warning("Don't have permission for service, stopped collecting user: {}, because: {}".format(client, error.response['Error']['Code']))
      return True
    else:
      return False