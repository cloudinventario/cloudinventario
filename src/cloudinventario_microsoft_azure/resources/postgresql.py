import logging
import re
from copy import Error

from cloudinventario.helpers import CloudInvetarioResource
from cloudinventario_microsoft_azure.collector import CloudCollectorMicrosoftAzure

from azure.core.exceptions import AzureError
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.rdbms.postgresql import PostgreSQLManagementClient

def setup(resource, collector):
    return CloudInventarioAzurePostgreSQL(resource, collector)


class CloudInventarioAzurePostgreSQL(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)

    def _login(self, credentials):
        try:
            self.credentials = credentials
            subscription_id = self.collector.subscription_id

            self.sql_name = 'PostgreSQL'
            self.sql_client = PostgreSQLManagementClient(
                credential=credentials, subscription_id=subscription_id)

            logging.info("logging config for Azure{}={}".format(self.sql_name, 
                self.collector.subscription_id))
        except AzureError as error
            logging.error(f"AzureError: {error}")
        except Error as e:
            logging.error(e)
        except Exception as error:
            raise error


    def _fetch(self):        
        return CloudCollectorMicrosoftAzure._fetch_sql(self)

    def _logout(self):
        self.sql_client.close()
        self.sql_client = None
