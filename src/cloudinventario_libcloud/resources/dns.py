import json, logging, traceback, time
from pprint import pprint

from libcloud.dns.providers import get_driver as dns_get_driver

from cloudinventario.helpers import CloudInvetarioResource


def setup(resource, collector):
    return CloudInventarioDNS(resource, collector)


class CloudInventarioDNS(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)

    def _login(self, config):
        self.config = config
        self.driver = self.config['driver']['dns']

        # Load driver to get provider
        DNS = dns_get_driver(self.driver)

        self.driver_dns = DNS(
            self.config['key'],
            self.config['secret'],
            **self.config['driver_params']
        )

        logging.info("logging config for DNS with driver {}".format(self.driver))

    def _fetch(self):
            data = []
            dns_s = self.driver_dns.list_zones()

            for dns in dns_s:
                # Process record
                records = self.driver_dns.list_records(dns)
                for record in records:
                    data.append(self._process_record(record.__dict__))

                # Process domain/zone
                data.append(self._process_dns(dns.__dict__))

                # Slow down as we make many requests
                time.sleep(1/4)

            logging.info("Collected {} dns".format(len(data)))
            return data

    def _process_record(self, record):
        record = self.collector._object_to_dict(record)

        logging.info("new DNS record name={}".format(record["name"]))
        data = {
            '__table': 'dns_record',

            "domain_id": -1,
            "domain_name": record["zone"].domain,

            "uniqueid": record["id"],
            "name": record["name"],
            "type": record["type"],
            "data": str(record["data"]),
            "ttl": record["ttl"],

            # TODO: add tags
        }
        return self.new_record('dns_record', data, record)

    def _process_dns(self, dns):
        rec = self.collector._object_to_dict(dns)

        logging.info("new DNS domain domain={}".format(rec["domain"]))
        data = {
            '__table': 'dns_domain',

            'uniqueid': dns["id"],
            'name': dns["domain"],
            'type': dns["type"],
            'ttl': dns["ttl"],
            "description": dns["extra"].get("Comment"),

            # TODO: add tags
        }
        return self.new_record('dns_domain', data, dns)

    def _logout(self):
        self.credentials = None
