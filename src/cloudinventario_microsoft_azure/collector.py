import concurrent.futures
from copy import Error
import logging, re, sys, asyncio, time
from pprint import pprint
from typing import Dict, List
import datetime

from sqlalchemy.sql.sqltypes import Boolean

from cloudinventario.helpers import CloudCollector

from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient, resources
from azure.mgmt.network import NetworkManagementClient

from azure.mgmt.compute.v2021_03_01.models._models_py3 import VirtualMachine
from azure.mgmt.resource.resources.v2021_04_01.models._models_py3 import (
    GenericResourceExpanded,
)

# TEST MODE
TEST = 0


def setup(name, config, defaults, options):
    return CloudCollectorMicrosoftAzure(name, config, defaults, options)


class CloudCollectorMicrosoftAzure(CloudCollector):
    def __init__(self, name, config, defaults, options):
        super().__init__(name, config, defaults, options)

    def _login(self):
        self.subscription_id = self.config["subscription_id"]
        self.tenant_id = self.config["tenant_id"]
        self.client_id = self.config["client_id"]
        self.client_secret = self.config["client_secret"]

        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        
        logging.info("logging in MicrosoftAzure={}".format(self.tenant_id))
        return self.credential

    def _fetch(self, collect):
        return []

    def _get_dependencies(self):
        return []

    def _logout(self):
        self.credential = None
