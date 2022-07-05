import logging
import boto3

import botocore.exceptions as aws_exception

from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventarioSnapshot(resource, collector)

class CloudInventarioSnapshot(CloudInvetarioResource):

  def __init__(self, resource, collector):
    super().__init__(resource, collector)

  def _login(self, session):
    self.session = session
    self.client = self.get_client()

  def _get_client(self):
    client = self.session.resource('ec2')
    return client

  def _fetch(self):
    data = []
    snapshots = self.client.snapshots.filter(OwnerIds=[
        self.collector.account_id
    ])

    for snapshot in snapshots:
      data.append(self._process_resource(snapshot))
    logging.info("collected {} snapshosts".format(len(data)))
    return data
      

  def _process_resource(self, snapshot):
    GIB_TO_MIB = 1024

    logging.info("collecting snapshot with ID={}".format(snapshot.id))
    data = {
    'id': snapshot.id,
    'data_encryption_key_id': snapshot.data_encryption_key_id,
    'description': snapshot.description,
    'encrypted': snapshot.encrypted,
    'kms_key_id': snapshot.kms_key_id,
    'outpost_arn': snapshot.outpost_arn,
    'owner_alias': snapshot.owner_alias,
    'progress': snapshot.progress,
    'is_on': (snapshot.progress != '100%'),
    'created': snapshot.start_time,
    'status': snapshot.state,
    'state_message': snapshot.state_message,
    'tags': snapshot.tags,
    'volume_id': snapshot.volume_id,
    'storage': snapshot.volume_size * GIB_TO_MIB,
    }

    return self.new_record(self.res_type, data, snapshot.meta.__dict__)
