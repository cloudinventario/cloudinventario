import logging
import re
from copy import Error
from azure.mgmt.rdbms import postgresql

from boto3 import resource

from azure.core.exceptions import AzureError

from cloudinventario.helpers import CloudInvetarioResource
from cloudinventario_microsoft_azure.resources.mysql import CloudInventarioAzureMysql
from cloudinventario_microsoft_azure.resources.mariadb import CloudInventarioAzureMariaDB
from cloudinventario_microsoft_azure.resources.postgresql import CloudInventarioAzurePostgreSQL
from cloudinventario_microsoft_azure.resources.sql_server import CloudInventarioAzureSQLServer  


def setup(resource, collector):
    return CloudInventarioAzureMetaSQL(resource, collector)


class CloudInventarioAzureMetaSQL(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)
        self.resource = resource

    def _login(self, credentials):
        try:
            self.credentials = credentials
            self.subscription_id = self.collector.subscription_id

            self.mysql = CloudInventarioAzureMysql(self.resource, self.collector)
            self.postgresql = CloudInventarioAzurePostgreSQL(self.resource, self.collector)
            self.mariadb = CloudInventarioAzureMariaDB(self.resource, self.collector)
            self.sql_server = CloudInventarioAzureSQLServer(self.resource, self.collector)

            self.mysql._login(credentials)
            self.postgresql._login(credentials)
            self.mariadb._login(credentials)
            self.sql_server._login(credentials)
            logging.info("logging config for AzureMetaSQL={}".format(
                self.subscription_id))
        except AzureError as error
            logging.error(f"AzureError: {error}")
        except Error as e:
            logging.error(e)
        except Exception as error:
            raise error


    def _fetch(self):
        data = []
        # MySQL
        data.extend(self.mysql.fetch())
        # PostgreSQL
        data.extend(self.postgresql.fetch())
        # MariaDB
        data.extend(self.mariadb.fetch())
        # SQL server
        data.extend(self.sql_server.fetch())

        logging.info("Collected {} cloud sqls".format(len(data)))
        return data

    def _logout(self):
        self.credentials = None
        self.subscription_id = None
