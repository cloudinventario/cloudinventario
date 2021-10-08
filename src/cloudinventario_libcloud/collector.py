import logging, re
from pprint import pprint

from libcloud.compute.providers import get_driver

from cloudinventario.helpers import CloudCollector, CloudInvetarioResourceManager

# TEST MODE
TEST = 0


def setup(name, config, defaults, options):
    return CloudCollectorLibcloud(name, config, defaults, options)


class CloudCollectorLibcloud(CloudCollector):
    def __init__(self, name, config, defaults, options):
        super().__init__(name, config, defaults, options)

    def _config_keys():
        return {
            key: 'First parameters which can be access_key or client_email, Required',
            secret: 'Second parameters which can be secret_key or private_key, Required',
            driver: 'Driver contains type for driver VM, LB, Storage, Container, Required',
            driver_params: 'Additional parameters needed for driver',
        }

    def _get_dependencies(self):
        return []

    def _is_not_primitive(self, obj):
        return hasattr(obj, '__dict__')

    def _object_to_dict(self, obj):
        for key in obj["extra"]:
            items = obj["extra"][key]
            # If field is obj
            if self._is_not_primitive(items):
                attributes = dict()
                for attribute in items.__dict__.itemss():
                    attributes[attribute[0]] = attribute[1]
                obj["extra"][key] = str(attributes)
            # If field is array of obj (need to check first items other will be the same type as first one)
            elif isinstance(items, list) and len(items) > 0 and self._is_not_primitive(items[0]):
                for item in items:
                    attributes = dict()
                    for attribute in item.__dict__.items():
                        attributes[attribute[0]] = attribute[1]
                    obj["extra"][key] = str(attributes)
        return obj

    def _login(self):
        # Get zone or region for cluster field
        self.zone = self.config['driver_params']['zone'] if 'zone' in self.config['driver_params'] else self.config[
            'driver_params']['region'] if 'region' in self.config['driver_params'] else None
        self.project_name = self.config['driver_params'].get('project')

        logging.info("logging config LibCloud drivers as {}".format(re.sub("[\{\}']", '', str(self.config['driver']))))
        return self.config

    def _fetch(self, collector):
        return []

    def _logout(self):
        self.driver = None
