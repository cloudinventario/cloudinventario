import concurrent.futures
import logging
import re
import sys
import asyncio
import time
import datetime
from pprint import pprint

import boto3

from cloudinventario_amazon_aws_resource.collector import CloudInvetarioAmazonAWSResource
from cloudinventario.helpers import CloudCollector, CloudInvetarioResourceManager

# TEST MODE
TEST = 0


def setup(name, config, defaults, options):
  return CloudCollectorAmazonUsageCost(name, config, defaults, options)


class CloudCollectorAmazonUsageCost(CloudInvetarioAmazonAWSResource):

  def __init__(self, name, config, defaults, options):
    super().__init__(name, config, defaults, options)

  def _config_keys():
    return {
       access_key: 'AWS AccessKeyID',
       secret_key: 'AWS SecretAccessKey',
       session_token: 'AWS SessionToken',
       region: 'AWS Region',
       account_id: 'AWS Account',
       date: 'Date for usage and cost'
    }

  def _get_dependencies(self):
    return []

  def _login(self):
    access_key = self.config['access_key']
    secret_key = self.config['secret_key']
    session_token = self.config.get('session_token')
    self.region = region = self.config['region']
    self.account_id = self.config.get('account_id')
    self.end_date = datetime.datetime.strptime(str(self.config['date']), '%Y-%m-%d') if 'date' in self.config else datetime.datetime.now()

    for logger in ["boto3", "botocore", "urllib3"]:
      logging.getLogger(logger).propagate=False
      logging.getLogger(logger).setLevel(logging.WARNING)

    if self.account_id is None:
      sts=boto3.client('sts', aws_access_key_id = access_key,
                       aws_secret_access_key = secret_key)
      ident=sts.get_caller_identity()
      self.account_id=ident['Account']

    logging.info("logging in AWS Usage and Cost with account_id={}, region={}".format(
        self.account_id, region))
    self.session=boto3.Session(aws_access_key_id = access_key, aws_secret_access_key = secret_key,
                                  aws_session_token = session_token, region_name = region)
    self.client=self.session.client('ce')

    self.instance_types={}

    return self.session

  def _fetch(self, collect):
    data=[]
    # Collect only 1 day and 30 days
    data.append(self._process_cost_usage(1))
    data.append(self._process_cost_usage(30))

    return data

  def _process_cost_usage(self, days):
    # Set period for request
    period_type = 'MONTHLY' if days > 29 else 'DAILY' 
    # Extract, prepare proper start and end dates format
    start_date = (self.end_date - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
    end_date = self.end_date.strftime('%Y-%m-%d')

    # Send request and get responde
    response = self.client.get_cost_and_usage(
    TimePeriod = {
        'Start': start_date,
        'End': end_date
        },
    Granularity = period_type,
    Metrics = [
        'AmortizedCost',
        'UsageQuantity'
      ]
    )

    # Check if request was not null
    result = response.get('ResultsByTime', [])
    if len(result) == 0: return None

    # Count amortizedCost 
    amortizedCost = sum([float(cost.get('Total').get('AmortizedCost').get('Amount')) for cost in result])
    # Extract first UNIT in which is cost
    unit = result[0].get('Total').get('AmortizedCost').get('Unit')

    logging.info("new Usage and Cost record with period_type={}".format(period_type))
    data={
            "period_type": period_type,
            "from": start_date,
            "to": end_date,

            "cost": amortizedCost,
            "unit": unit,
        }
    return self.new_record('usage_cost', data, response)

  def _logout(self):
    self.client = None


