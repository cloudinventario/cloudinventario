import logging
import re
from copy import Error

from cloudinventario.helpers import CloudInvetarioResource

from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.rdbms.mariadb import MariaDBManagementClient

def setup(resource, collector):
    return CloudInventarioAzureMariaDB(resource, collector)


class CloudInventarioAzureMariaDB(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)

    def _login(self, credentials):
        try:
            self.credentials = credentials
            subscription_id = self.collector.subscription_id

            self.mariadb_client = MariaDBManagementClient(
                credential=credentials, subscription_id=subscription_id)

            logging.info("logging config for AzureMariaDB={}".format(
                self.collector.subscription_id))
        except Error as e:
            logging.error(e)

    def _fetch(self):
        data = []
        for sql in list(self.mariadb_client.servers.list()):
            data.append(self._process_resource(sql.as_dict()))

        logging.info("Collected {} MariaDB".format(len(data)))
        return data

    def _process_resource(self, sql):
        logging.info("new AzureMariaDB name={}".format(sql.get('name')))
        data = {
            "id": sql.get('id'),
            "name": sql.get('name'),
            "type": sql.get('type'),
            "location": sql.get('location'),
            "tags": sql.get('tags', []),
            "storage": sql.get('storage_profile', {}).get('storage_mb', 0),
            "version": sql.get('version'),
            "networks": sql.get('private_endpoint_connections', []),
            "domain": sql.get('fully_qualified_domain_name'),
            "status": sql.get('user_visible_state'),
            "instances": re.search(r'resourceGroups/(.*?)/', sql.get('id', '')).group(1),
            "is_on": 1 if sql.get('user_visible_state', '').lower() == "ready" else 0
        }

        return self.new_record(self.res_type, data, sql)

    def _logout(self):
        self.sql_client.close()
        self.sql_client = None
