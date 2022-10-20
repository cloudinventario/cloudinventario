import boto3, json
from pprint import pprint

import botocore.exceptions as aws_exception

from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventarioRds(resource, collector)

class CloudInventarioRds(CloudInvetarioResource):

  def __init__(self, resource, collector):
    super().__init__(resource, collector)

  def _login(self, session):
    self.session = session
    self.client = self.get_client()

  def _get_client(self):
    client = self.session.client('rds')
    return client

  def _fetch(self):
    data = []

    paginator = self.client.get_paginator('describe_db_instances')
    response_iterator = paginator.paginate()

    for page in response_iterator:
      for db_instance in page['DBInstances']:
        data.append(self.process_resource(db_instance))
    return data

  def _process_resource(self, db):
    storage = db['PendingModifiedValues'].get('AllocatedStorage') or db['AllocatedStorage']
    instance_type = db['DBInstanceClass'][3:]
    instance_def = self.collector._get_instance_type(instance_type)

    data = {
      "name": db.get('DBName'),
      "type": instance_type,
      "dbtype": db['Engine'],
      "instance_type": db['DBInstanceClass'],
      "cpus": instance_def["cpu"],
      "memory": instance_def["memory"],
      "cluster": db['AvailabilityZone'],
      "clusterid": db.get('DBClusterIdentifier'),
      "created": db['InstanceCreateTime'],
      "status": db['DBInstanceStatus'],
      "is_on": (db['DBInstanceStatus'] == "available" and 1 or 0),
      "primary_fqdn": db['Endpoint']['Address'],
      "maintenance_window": db['PreferredMaintenanceWindow'],
      "dbencrypted": db['StorageEncrypted'],
      "public": db['PubliclyAccessible'],
      "storage": storage * 1024, # in MiB
      "port": db['Endpoint']['Port'],
      "multi_az": db['MultiAZ'],
      "version": db['EngineVersion'],
      "uniqueid": db['DBInstanceIdentifier'],
      "storage_type": db['StorageType'],
      "storage_iops": db.get('Iops'),
      "auto_minor_upgrade": db['AutoMinorVersionUpgrade'],
      "dbuser": db['MasterUsername'],
      "dbname": db['DBName'],
      "is_public": db['PubliclyAccessible'],
      "tags": self.collector._get_tags(db, "TagList")
    }

    return self.new_record(self.res_type, data, db)
