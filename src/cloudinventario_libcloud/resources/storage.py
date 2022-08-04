import json, logging, traceback
from pprint import pprint

from libcloud.storage.providers import get_driver as st_get_driver
from libcloud.storage.types import Provider as StorageProvider
from libcloud.common.types import LibcloudError

from cloudinventario.helpers import CloudInvetarioResource


def setup(resource, collector):
    return CloudInventarioStorage(resource, collector)


class CloudInventarioStorage(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)

    def _login(self, config):
        self.config = config
        # self.config = self.collector.config
        self.driver = self.config['driver']['storage']

        Storage = st_get_driver(self.driver)
        self.st_driver = Storage(
            self.config['key'],
            self.config['secret'],
            # Pass every additional attribute as dict into Storage
            **self.config['driver_params']
        )

        logging.info("logging config for Storage with driver {}".format(self.driver))

    def _fetch(self):
        data = []
        storages = self.st_driver.list_containers()

        for storage in storages:
            storage.objects = self._process_objects(storage)             
            data.append(self._process_resource(storage.__dict__))

        logging.info("Collected {} storages".format(len(data)))
        return data
    
    def _process_objects(self, storage):
        objects_result = []
        try:
            objects = storage.list_objects()
            for st in objects:
                objects_result.append({
                    'name': st.name,
                    'size': st.size,
                    'hash': st.hash,
                    'driver': st.driver,
                    'container': st.container,
                    'meta_data': st.meta_data,
                    'extra': st.extra,
                })
        except LibcloudError as liberror:
            logging.warn("Storage is not from {} region, it will not have 'objects'".format(self.collector.zone))
        except Exception as error:
            raise error
        return objects_result

    def _process_resource(self, storage):
        storage = self.collector._object_to_dict(storage)

        logging.info("new Storage name={}".format(storage["name"]))
        data = {
            "name": storage["name"],
            "cluster": self.collector.zone,
            "project": self.collector.project_name,
            "driver": storage['driver'],
            "objects": storage["objects"]
            }

        return self.new_record(self.res_type, data, storage)


    def _logout(self):
        self.credentials = None
