import logging
import re
from copy import Error

from cloudinventario.helpers import CloudInvetarioResource
from cloudinventario_microsoft_azure.collector import CloudCollectorMicrosoftAzure

from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.rdbms.mysql import MySQLManagementClient

def setup(resource, collector):
    return CloudInventarioAzureMysql(resource, collector)


class CloudInventarioAzureMysql(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)

    def _login(self, credentials):
        try:
            self.credentials = credentials
            subscription_id = self.collector.subscription_id

            self.sql_name = 'MySQL'
            self.sql_client = MySQLManagementClient(
                credential=credentials, subscription_id=subscription_id)

            logging.info("logging config for Azure{}={}".format(self.sql_name, 
                self.collector.subscription_id))
        except Error as e:
            logging.error(e)

    def _fetch(self):        
        return CloudCollectorMicrosoftAzure._fetch_sql(self)

    def _logout(self):
        self.sql_client.close()
        self.sql_client = None
