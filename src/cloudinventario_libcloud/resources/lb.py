import logging

from libcloud.loadbalancer.providers import get_driver as lb_get_driver

from cloudinventario.helpers import CloudInvetarioResource


def setup(resource, collector):
    return CloudInventarioLB(resource, collector)


class CloudInventarioLB(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)

    def _login(self, config):
        self.config = config
        # self.config = self.collector.config
        self.driver = self.config['driver']['lb']

        LoadBalancer = lb_get_driver(self.driver)
        self.lb_driver = LoadBalancer(
            self.config['key'],
            self.config['secret'],
            # Pass every additional attribute as dict into loadBalancers
            **self.config['driver_params']
        )

        logging.info("logging config for LoadBalancer with driver {}".format(self.driver))

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
        balancer = self.collector._object_to_dict(balancer)

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
