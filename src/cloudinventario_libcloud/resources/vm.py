import json, logging, traceback
from pprint import pprint

from libcloud.compute.providers import get_driver

from cloudinventario.helpers import CloudInvetarioResource


def setup(resource, collector):
    return CloudInventarioVM(resource, collector)


class CloudInventarioVM(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)

    def _login(self, config):
        self.config = config
        self.driver = self.config['driver']['vm']

        # Load driver to get provider
        ComputeEngine = get_driver(self.driver)

        self.driver_vm = ComputeEngine(
            self.config['key'],
            self.config['secret'],
            # Pass every additional attribute as dict into computeEngine
            **self.config['driver_params']
        )

        logging.info("logging config for VirtualMachine with driver {}".format(self.driver))
        
    def _fetch(self):
            data = []
            instances = self.driver_vm.list_nodes()

            for instance in instances:
                # Process instance
                data.append(self._process_vm(instance.__dict__))

            logging.info("Collected {} vm".format(len(data)))
            return data

    def _process_vm(self, rec):
        # To check if some attribute is object (or array of object) to give every information
        rec = self.collector._object_to_dict(rec)

        logging.info("new VM name={}".format(rec["name"]))
        vm_data = {
            "uniqueid": rec["id"],
            "created": rec["created_at"],
            "name": rec["name"],
            "size": rec["size"],
            "image": rec["image"],
            "cluster": self.collector.zone,
            "project": self.collector.project_name,
            "primary_ip": rec["public_ips"][0] if len(rec["public_ips"]) > 0 else None,
            "public_ip": rec["public_ips"],
            "private_ip": rec["private_ips"],
            "status": rec["state"],
            "is_on": rec["state"].lower() == 'running',
            "tags": rec["extra"]['labels'] if 'labels' in rec["extra"] else rec["extra"].get('tags'),
        }

        return self.new_record('vm', vm_data, rec)

    def _logout(self):
        self.credentials = None
