import json, logging, traceback
from pprint import pprint

from libcloud.container.providers import get_driver as ct_get_driver
from libcloud.container.types import Provider as ContainerProvider
from libcloud.common.types import LibcloudError

from cloudinventario.helpers import CloudInvetarioResource


def setup(resource, collector):
    return CloudInventarioContainer(resource, collector)


class CloudInventarioContainer(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)

    def _login(self, config):
        self.config = config
        # self.config = self.collector.config

        Container = ct_get_driver(self.config['driver_container'])
        self.ct_driver = Container(
            self.config['key'],
            self.config['secret'],
            # Pass every additional attribute as dict into Container
            **self.config['driver_params']
        )

        logging.info("logging config for container with driver {}".format(self.config['driver_lb']))

    def _fetch(self):
        data = []
        containers = self.ct_driver.list_containers()

        for container in containers:          
            data.append(self._process_resource(container.__dict__))

        logging.info("Collected {} containers".format(len(data)))
        return data

    def _process_resource(self, container):
        container = self.collector._object_to_dict(container)
        image  = {
            "id": container['image'].id,
            "name": container['image'].name,
            "path": container['image'].path,
            "version": container['image'].version,
            "extra": container['image'].extra,
        }

        logging.info("new Container name={}".format(container["name"]))
        data = {
            "id": container["id"],
            "name": container["name"],
            "cluster": self.collector.zone,
            "project": self.collector.project_name,
            "state": container['state'],
            "image": image,
            "public_ip": container['ip_addresses'],
            "is_on": container["state"].lower() == 'running',
            }

        return self.new_record(self.res_type, data, container)


    def _logout(self):
        self.credentials = None
