import concurrent.futures
import logging, re, sys, asyncio, time
from pprint import pprint

import boto3
import boto3.session
from botocore.exceptions import ClientError

from cloudinventario.cloudinventario import CloudInventario
from cloudinventario.helpers import CloudCollector
from cloudinventario_amazon_aws_resource.collector import CloudInvetarioAmazonAWSResource

# TEST MODE
TEST = 0

def setup(name, config, defaults, options):
  return CloudCollectorAmazonAWSMulti(name, config, defaults, options)

class CloudCollectorAmazonAWSMulti(CloudInvetarioAmazonAWSResource):

  def __init__(self, name, config, defaults, options):
    super().__init__(name, config, defaults, options)

  def load_resource_collectors(self, res_list):
    return None

  def _loadCollectorModule(self, name, cred, defaults, options):
    return CloudInventario.loadCollectorModule("amazon-aws", name, cred, defaults, options)

  def _login(self):
    access_key = self.config['access_key']
    secret_key = self.config['secret_key']
    region = self.config['region']
    roles = self.config.get('roles')
    regions = self.config.get('regions')
    continue_on_error = self.config.get('continue-on-error', False)

    self.status_error = []
    self.creds = []
    self.primary_region = region

    for logger in ["boto3", "botocore", "urllib3"]:
      logging.getLogger(logger).propagate = False
      logging.getLogger(logger).setLevel(logging.WARNING)

    logging.info("assuming AWS roles")
    #self._add_creds_regions(None, access_key, secret_key, None, regions)
    if roles:
      client = boto3.client('sts', aws_access_key_id = access_key, aws_secret_access_key = secret_key)

      for role in roles:
        try:
          role_regions = role.get('region', regions)
          assumed = client.assume_role(
            RoleArn = "arn:aws:iam::{}:role/{}".format(role['account'], role['role']),
            RoleSessionName = "ASSR-{}".format(role['account'])
          )
          as_creds = assumed['Credentials']
          self._add_creds_regions(role['name'], str(role['account']), as_creds['AccessKeyId'], as_creds['SecretAccessKey'], as_creds['SessionToken'], role_regions)
        except Exception as error:
          logging.warning(f"AccessDenied on User: {role['account']} to perform: {role['role']}")
          if not continue_on_error:
            raise error
          logging.warning(f"Skipping User: {role['account']}")
          self.status_error.append({'source': self.__dict__['name'], 'status': "error", 'error': f"AccessDenied on User: {role['account']} to perform: {role['role']}"}) #  store_status(self, source, status, runtime=None, error=None):

    # create clients
    self.clients = []
    for cred in self.creds:
      name = self.name
      if cred['name'] is not None:
        name = "{}@{}".format(name, cred['name'])
        self.defaults['project'] = cred['name']

      cred['collect'] = self.config['collect']
      handle = self._loadCollectorModule(name, cred, self.defaults, self.options)
      handle.login()

      self.clients.append({
        "account_id": cred['account_id'] or 0,
        "handle": handle
      })

    return True

  def _add_creds_regions(self, name, account_id, access_key, secret_key, session_token = None, regions = None):
    if regions:
       for region in regions:
         self._add_creds(name, account_id, access_key, secret_key, session_token, region)
    else:
      self._add_creds(None, account_id, access_key, secret_key)
    return self.creds

  def _add_creds(self, subname, account_id, access_key, secret_key, session_token = None, region = None):
    if region:
      self.creds.append({
        "name": subname,
        "access_key": access_key,
        "secret_key": secret_key,
        "session_token": session_token,
        "region": region,
        "account_id": account_id
      })
    else:
      # XXXX: discover enable regions using EC2 (what if other services have different enabled ?)
      client = boto3.client('ec2', aws_access_key_id = access_key, aws_secret_access_key = secret_key,
                               aws_session_token = session_token, region_name = self.primary_region)
      try:
        region_list = client.describe_regions()
      except ClientError as e:
        logging.error("Failed to discover enabled regions, please specify manually or grant permission")
        raise
      regions = [region['RegionName'] for region in region_list['Regions']]
      for region in regions:
        self.creds.append({
          "name": subname,
          "access_key": access_key,
          "secret_key": secret_key,
          "session_token": session_token,
          "region": region,
          "account_id": account_id
        })
    return self.creds

  def _fetch(self, collect):
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers = self.options["tasks"] or 1) as executor:
      futures = []
      for client in self.clients:
         futures.append(executor.submit(client['handle'].fetch, collect))
      for future in concurrent.futures.as_completed(futures):
        try:
          res.extend(future.result())
        except Exception as e:
          logging.error("Exception while processing account={}".format(client['account_id']))
          raise
    return res

  def _logout(self):
    self.clients = None
