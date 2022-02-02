import logging, re, requests, json

from cloudinventario.helpers import CloudCollector

# TEST MODE
TEST = 0


def setup(name, config, defaults, options):
    return CloudCollectorCRTsh(name, config, defaults, options)


class CloudCollectorCRTsh(CloudCollector):
    def __init__(self, name, config, defaults, options):
        super().__init__(name, config, defaults, options)

    def _config_keys():
        return {
            identity: 'Identity or domain for which will be search, Required',
            wildcard: 'Add wildcard into identity for searching, Boolean (default False)',
            deduplicate: 'Deduplicate (pre)certificate pairs, Boolean (default False)',
            expired: 'Exclude expired certificates, Boolean (default False)',
        }

    def _login(self):
        self.identity = self.config["identity"]
        self.expired = self.config["expired"] if 'expired' in self.config else False
        self.wildcard = self.config["wildcard"] if 'wildcard' in self.config else False 
        self.deduplicate = self.config["deduplicate"] if 'deduplicate' in self.config else False
        
        logging.info("logging in CRT.sh={}".format(self.identity))
        return self.identity

    def _fetch(self, collect):
        data = []
        base_url = "https://crt.sh/?q={}&output=json"
        if not self.expired:
            base_url = base_url + "&exclude=expired"
        if self.deduplicate:
            base_url = base_url + "&deduplicate=Y"
        if self.wildcard and "%" not in self.identity:
            self.identity = "%.{}".format(self.identity)

        url = base_url.format(self.identity)
        request = requests.get(url)
        logging.info("Create request as {}".format(url))

        if request.ok:
            try:
                content = request.content.decode('utf-8')
                json_data = json.loads(content)
                for item in json_data:
                    data.append(self._process(item))
            except Exception as e:
                logging.error("Error after requesting {} {}".format(url, e))
        
        logging.info("Collected {} logs".format(len(data)))
        return data

    def _process(self, rec):
        logging.info("new CRT log={}".format(rec.get('id')))
        data = {
            "id": rec.get('id'),
            "created": rec.get('entry_timestamp'),
            "not_before": rec.get('not_before'),
            "not_after": rec.get('not_after'),
            "serial_number": rec.get('serial_number'),
            "name": rec.get('common_name'),
            "alias": rec.get('name_value').split('\n'),
            "issuer_name": rec.get('issuer_name'), # issuer_name
            "issuer_ca_id": rec.get('issuer_ca_id')
        }
        return self.new_record('crt', data, rec)

    def _logout(self):
        self.identity = None
