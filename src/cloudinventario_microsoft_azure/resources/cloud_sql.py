import logging
import re
from copy import Error

from cloudinventario.helpers import CloudInvetarioResource

from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.sql import SqlManagementClient


def setup(resource, collector):
    return CloudInventarioAzureCloudSQL(resource, collector)


class CloudInventarioAzureCloudSQL(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)

    def _login(self, credentials):
        try:
            self.credentials = credentials
            subscription_id = self.collector.subscription_id

            self.sql_client = SqlManagementClient(
                credential=credentials, subscription_id=subscription_id)

            logging.info("logging config for AzureCloudSQL={}".format(
                self.collector.subscription_id))
        except Error as e:
            logging.error(e)

    def _fetch(self):
        data = []
        for sql in list(self.sql_client.servers.list()):
            data.append(self._process_resource(sql.as_dict()))

        logging.info("Collected {} cloud sqls".format(len(data)))
        return data

    def _process_resource(self, sql):
        logging.info("new AzureCloudSQL name={}".format(sql.get('name')))
        data = {
            "id": sql.get('id'),
            "name": sql.get('name'),
            "type": sql.get('type'),
            "location": sql.get('location'),
            "tags": sql.get('tags', []),
            "kind": sql.get('kind'),
            "version": sql.get('version'),
            "networks": sql.get('private_endpoint_connections', []),
            "domain": sql.get('fully_qualified_domain_name'),
            "status": sql.get('state'),
            "instances": re.search(r'resourceGroups/(.*?)/', sql.get('id', '')).group(1),
            "is_on": 1 if instance.get('state').lower() == "ready" else 0
        }

        return self.new_record(self.res_type, data, sql)

    def _logout(self):
        self.sql_client.close()
        self.sql_client = None
