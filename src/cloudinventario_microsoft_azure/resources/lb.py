import logging
import re
from copy import Error

from cloudinventario.helpers import CloudInvetarioResource
from azure.mgmt.network import NetworkManagementClient


def setup(resource, collector):
    return CloudInventarioAzureLoadBalancer(resource, collector)


class CloudInventarioAzureLoadBalancer(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)

    def _login(self, credentials):
        try:
            self.credentials = credentials
            subscription_id = self.collector.subscription_id

            self.network_client = NetworkManagementClient(
                credential=credentials, subscription_id=subscription_id)

            logging.info("logging config for AzureLoadBalancer={}".format(
                self.collector.subscription_id))
        except Error as e:
            logging.error(e)

    def _fetch(self):
        data = []
        for balancer in list(self.network_client.load_balancers.list_all()):
            data.append(self._process_resource(balancer.as_dict()))

        logging.info("Collected {} load balancers".format(len(data)))
        return data

    def _process_resource(self, balancer):
        subnets = [{
            'id': ip.get('id'),
            'name': ip.get('name'),
            'ip_address': ip.get('private_ip_address')
        } for ip in balancer.get('frontend_ip_configurations', [])]

        logging.info("new AzureLoadBalancer name={}".format(
            balancer.get('name')))
        data = {
            "id": balancer.get('id'),
            "name": balancer.get('name'),
            "tags": balancer.get('tags', []),
            "type": balancer.get('type'),
            "subnets": subnets,
            "location": balancer.get('location'),
            "instances": re.search(r'resourceGroups/(.*?)/', balancer.get('id', '')).group(1),

            # "status": balancer.get('provisioning_state'),
            # "scheme": balancer['loadBalancingScheme'],
            # "backends": balancer['instanceGroups'],
            # "description": balancer['description'],
            # "is_on": True if status == "on" else False,
        }

        return self.new_record(self.res_type, data, balancer)

    def _logout(self):
        self.network_client.close()
        self.network_client = None
