"""Classes used by CloudInventario."""
import requests
import datetime
import json
import logging
import importlib
import dns.resolver
import dns.exception
from pprint import pprint

import cloudinventario.platform as platform
from cloudinventario.limiter import CloudInventarioLimiter

class CloudEncoder(json.JSONEncoder):
  def default(self, z):
    if isinstance(z, datetime.datetime):
      return (str(z))
    else:
      return super().default(z)

class CloudCollector:
  """Cloud collector."""

  def __init__(self, name, config, defaults, options):
    self.name = name
    self.config = config
    self.defaults = {**defaults}
    self.options = {**options}

    self.limiter = CloudInventarioLimiter()
    self.limiter.add_source(self.name, self.config)

    self.allow_self_signed = options.get('allow_self_signed', config.get('allow_self_signed', False))
    self.verify_ssl = self.options.get('verify_ssl_certs', config.get('verify_ssl_certs', True))
    requests.packages.urllib3.disable_warnings()

    # TODO: separate this ?
    self.resolver = dns.resolver.Resolver()
    self.resolver.timeout = 1
    self.resolver.lifetime = 1
    self.resolver.use_search_by_default = False

    self.resource_manager = None
    self.resource_collectors = {}
    return

  def _init(self, **kwargs):
    self.collector_pkg = kwargs['collector_pkg']
    self.resources = kwargs['resources']
    self.dependencies = self._get_dependencies()

    self.resource_collectors = self.load_resource_collectors(self.resources) or {}
    return True

  def __pre_request(self):
    pass

  def __post_request(self):
    pass

  def login(self):
    self.__pre_request()
    try:
      session = self._login()
      if session is None or session is False:
        raise Exception("Login failed")
      self.resource_login(session)
      return 0
    except:
      logging.warning("Failed to login the following collector: {}".format(self.name))
      raise
    finally:
      self.__post_request()

  def resource_login(self, session):
    for resource, res_collector in self.resource_collectors.items():
      try:
        logging.info("Passing session to: {}".format(resource))
        res_collector.login(session)
      except Exception:
        logging.warning("Failed to pass session to the following resource: {}".format(resource))
        raise
    return True

  def fetch(self, collect = None):
    self.__pre_request()
    try:
      data = []
      data.extend(self._resource_fetch())
      data.extend(self._fetch(collect))

      data = list(filter(lambda x: x, data))
      if 'status_error' in self.__dict__:
        if len(self.status_error) > 0:
          return {'data': data, 'errors': self.status_error}
      return data
    except Exception as error:
      if not (self.options['check_permission'] and self.check_permission(self.client, error)):
        raise
    finally:
      self.__post_request()

  def _resource_fetch(self):
    if not self.resource_manager:
      return []

    data = []
    try:
      res = ''
      for res in self.resource_collectors.values(): # self.resource_collectors is already ordered by dependecy
        data.extend(res.fetch())
      return data
    except Exception:
      if not (self.options['check_permission'] and self.check_permission(self.client, error)):
        raise

  def logout(self):
    self.__pre_request()
    try:
      res = self._logout()
      return res
    except:
      raise
    finally:
      self.__post_request()

  def get_resource_data(self, resource):
    if resource in self.resource_collectors:
      return self.resource_collectors[resource].data
    else:
      return []

  def delete_resource_data(self, resource):
    if resource in self.resource_collectors:
      self.resource_collectors[resource].data = None

  def set_resource_data(self, resource, new_data):
    if resource in self.resource_collectors:
      self.resource_collectors[resource].data = new_data

  def load_resource_collectors(self, res_list):
    try:
      self.resource_manager = CloudInvetarioResourceManager(res_list, self.collector_pkg, self)
      res_collectors = self.resource_manager.get_resource_objs(self.dependencies)
      return res_collectors
    except:
      raise

  def get_dependencies(self):
    try:
      logging.debug("Getting dependencies for the following module: {}".format(self.name))
      dep_list = self._get_dependencies() or []
      dep_list = dep_list + self.config.get('_dependencies', [])
      return dep_list
    except Exception:
      logging.error("Failed to get dependencies for the following collector: {}".format(self.name))
      raise

  def _get_dependencies(self):
    return None

  def _resolve_fqdn(self, fqdn):
    try:
      result = self.resolver.query(fqdn, "A")
      a_list = []
      for val in result:
        a_list.append(val.to_text())
      if len(result) > 0:
        a_list.sort()
        return a_list[0]
    except dns.exception.DNSException:
      pass
    return None

  def new_record(self, rectype, attrs, details):
    attr_keys = ["__table",
                 "created", "uniqueid", "name", "project", "owner"]
    attr_keys_inventory = [
                 "location", "description",
                 "cpus", "memory", "disks", "storage", "primary_ip", "primary_fqdn",
                 "os", "os_family",
                 "is_on"]
    attr_keys_dns = [
                 "domain_id", "domain_name", "ttl", "type", "data"]

    # TODO: handle this in separate function !
    if attrs.get('__table') in ['dns_domain', 'dns_record']:
       attr_keys = attr_keys + attr_keys_dns
    else:
       attr_keys = attr_keys + attr_keys_inventory

    # apply defaults
    attrs = {**self.defaults, **attrs}

    attr_json_keys = [ "networks", "storages", "tags"]
    rec = {
      "source_id": -1,		# TODO: should be mapped during save
      "source_name": self.name,
      "source_version": None,

      "inventory_type": rectype,

      "attributes": None
    }

    check, message = self.limiter.add_counter(self.name, self.config)
    if not check:
      logging.warning(message)
      return None

    for key in attr_keys:
      if attrs.get(key):
        rec[key] = attrs.pop(key)
      else:
        rec[key] = None

    fqdn_keys = filter(lambda k: k.endswith("_fqdn"), rec.keys())
    for key in fqdn_keys:
      if rec[key] is not None and rec[key] != '':
        key_ip = "{}_ip".format(key[0:-5])
        if key_ip in rec and rec[key_ip] is None:
          rec[key_ip] = self._resolve_fqdn(rec[key])

#    for key in attr_tag_keys:
#      data = attrs.get(key, [])
#      rec[key] = ",".join(map(lambda k: "{}={}".format(k, data[k]), data.keys()))

    for key in attr_json_keys:
      if not attrs.get(key):
        rec[key] = '[]'
      else:
        rec[key] = json.dumps(attrs[key], default=str) # added default=str -> problem with AttachTime,CreateTime
        del(attrs[key])

    for key in ["cluster", "status"]: # fields that possibly contain data structures
      value = attrs.get(key)
      if not value:
        rec[key] = None
      else:
        if type(value) in [dict, list]:
          rec[key] = json.dumps(value, default=str)
        else:
          rec[key] = value


    if "os_family" not in rec and rec.get("os"):
      rec["os_family"] = platform.get_os_family(rec.get("os"), rec.get("description"))

    if rec.get("os"):
      rec["os"] = platform.get_os(rec.get("os"), rec.get("description"))

    if len(attrs) > 0:
      rec["attributes"] = json.dumps(attrs, default=str)
    rec["details"] = json.dumps(details, cls=CloudEncoder, default=str)

    return rec

  def check_permission(self, client, error):
      pass

class CloudInvetarioResourceManager:

  def __init__(self, res_list, collector_pkg, collector):
    self.res_list = res_list or []
    self.collector_pkg = collector_pkg
    self.collector = collector
    self.dep_classif = {  # dependency_classification
      "dependency": set(),
      "not_dependency": set(),
    }

  def get_resource_objs(self, res_dep_list = []):
    obj_dict = {}

    # sorting based on whether a resource needs priority in fetching or not
    res_list = list(set((res_dep_list or []) + self.res_list))
    for resource in res_list:
      if resource in res_dep_list:
        self.dep_classif["dependency"].add(resource)
      else:
        self.dep_classif["not_dependency"].add(resource)

    res_list = []
    res_list.extend(self.dep_classif["dependency"] or [])
    res_list.extend(self.dep_classif["not_dependency"] or [])
    if res_list:
      res_list.sort()

    for res in res_list:
      try:
        mod_name = self.collector_pkg + ".resources." + res
        logging.debug("Importing module: {}".format(mod_name))
        res_mod = importlib.import_module(mod_name)
      except Exception as e:
        logging.error("Failed to load the following module:{}, reason: {}".format(mod_name, e))
        raise
      obj_dict[res] = res_mod.setup(res, self.collector)

    return obj_dict


class CloudInvetarioResource():

  def __init__(self, res_type, collector):
    self.res_type = res_type
    self.collector = collector
    self.session = None
    self.client = None
    self.data = None
    self.raw_data = []

  def login(self, session):
    try:
      self._login(session)
    except Exception:
      raise

  def fetch(self):
    try:
      logging.debug("fetching resource={}".format(self.res_type))
      self.raw_data = []
      self.data = self._fetch()
      return self.data
    except Exception as error:
      if self.collector.options['check_permission'] and self.collector.check_permission(self.client, error):
        return []
      else:
        logging.error("Failed to fetch the data of the following type of cloud resource: {}". format(self.res_type))
        raise

  def process_resource(self, resource_data):
    try:
      #logging.debug("processing resource={}".format(self.res_type))
      data = self._process_resource(resource_data)
      return data
    except Exception:
      logging.error("Failed to process the following type of resource: {}".format(self.res_type))
      raise

  def get_client(self):
    try:
      client = self._get_client()
      return client
    except Exception:
      logging.error("Failed to get the client of the following type of resource: {}".format(self.res_type))
      raise

  def get_data(self):
    try:
      if self.data is None:
        self.data = self.fetch()
      return self.data
    except Exception:
      logging.error("Failed to get the data of the following of resource: {}".format(self.res_type))

  def get_raw_data(self):
    try:
      if self.raw_data is None:
        self.data = self.fetch()
      return self.raw_data
    except Exception:
      logging.error("Failed to get the raw data of the following of resource: {}".format(self.res_type))

  def new_record(self, rectype, attrs, details):
    self.raw_data.append(attrs)
    return self.collector.new_record(rectype, attrs, details)
