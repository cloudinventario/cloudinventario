import logging

from libcloud.loadbalancer.providers import get_driver as lb_get_driver

from cloudinventario.helpers import CloudInvetarioResource


def setup(resource, collector):
    return CloudInventarioLB(resource, collector)


class CloudInventarioLB(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)
    
    def _is_not_primitive(self, obj):
        return hasattr(obj, '__dict__')

    def _login(self, config):
        self.config = config
        # self.config = self.collector.config

        LoadBalancer = lb_get_driver(self.config['driver_lb'])
        self.lb_driver = LoadBalancer(
            self.config['key'],
            self.config['secret'],
            # Pass every additional attribute as dict into loadBalancers
            **self.config['driver_params']
        )

        logging.info("logging config for LoadBalancer with driver {}".format(self.config['driver_lb']))

    def _fetch(self):
        data = []
        balancers = self.lb_driver.list_balancers()

        for balancer in balancers:
            # Process instance
            balancer.instances = self._process_member(balancer.list_members())
            data.append(self._process_resource(balancer.__dict__))

        logging.info("Collected {} lb".format(len(data)))
        return data

    def _process_member(self, members):
        instances = []
        for member in members:
            member = member.__dict__
            instances.append({
                "id": member["id"],
                "ip": member["ip"],
                "port": member["port"],
                "extra": member["extra"],
            })
        return instances

    def _process_resource(self, balancer):
        # To check if some attribute is object (or array of object) to give every information
        for key in balancer["extra"]:
            item = balancer["extra"][key]
            # If field is object
            if self._is_not_primitive(item):
                attributes = dict()
                for attribute in item.__dict__.items():
                    attributes[attribute[0]] = attribute[1]
                balancer["extra"][key] = str(attributes)
            # If field is array of object (need to check first item other will be the same type as first one)
            elif isinstance(item, list) and len(item) > 0 and self._is_not_primitive(item[0]):
                for object in item:
                    attributes = dict()
                    for attribute in object.__dict__.items():
                        attributes[attribute[0]] = attribute[1]
                    balancer["extra"][key] = str(attributes)

        logging.info("new LoadBalancer name={}".format(balancer["name"]))
        data = {
            "id": balancer["id"],
            "name": balancer["name"],
            "cluster": self.collector.zone,
            "project": self.collector.project_name,
            "ip": balancer["ip"],
            "port": balancer["port"],
            "instances": balancer["instances"],
            "status": balancer["state"],
            "tags": balancer["extra"]['labels'] if 'labels' in balancer["extra"] else balancer["extra"].get('tags'),
        }

        return self.new_record(self.res_type, data, balancer)
