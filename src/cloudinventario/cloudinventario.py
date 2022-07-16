"""CloudInventario"""
import os
import sys
import importlib
import re
import threading
import time
import logging
from pprint import pprint
import traceback
import psutil

from cloudinventario.storage import InventoryStorage

COLLECTOR_PREFIX = 'cloudinventario'

class CloudInventario:

    def __init__(self, config):
        self.config = config
        self.lock = threading.Lock()

    @property
    def collectors(self):
        collectors = []
        for col in self.config['collectors'].keys():
            if self.config['collectors'][col].get("disabled") != True:
                collectors.append(col)
        return collectors

    @property
    def expiredCollectors(self):
        # TODO
        pass

    def collectorConfig(self, collector):
        return self.config['collectors'][collector]

    def loadCollector(self, collector, options=None):
        mod_cfg = self.collectorConfig(collector)

        mod_name = mod_cfg['module']
        mod_config = mod_cfg['config']
        mod_defaults = mod_cfg.get('default', {})
        return CloudInventario.loadCollectorModule(mod_name, collector, mod_config, mod_defaults, options)

    @staticmethod
    def loadCollectorModule(mod_name, collector, config, defaults=None, options=None):
        # basic safety, should throw error
        mod_name = re.sub(r'[/.]', '_', mod_name)
        mod_name = re.sub(r'_', '__', mod_name)
        mod_name = re.sub(r'-', '_', mod_name)
        mod_pkg = COLLECTOR_PREFIX + '_' + mod_name

        mod = importlib.import_module(mod_pkg + '.collector')
        mod_instance = mod.setup(collector, config, defaults, options or {})

        # XXX: init for resource collectors (I don't like it)
        mod_instance._init(
            collector_pkg=mod_pkg,
            resources=config.get('collect', [])
        )
        return mod_instance

    def login(self, collector, options=None):
        # workaround for buggy libs
        wd = os.getcwd()
        os.chdir("/tmp")
        try:
            instance = self.loadCollector(collector, options)
            instance.login()
            instance.logout()
            print(f"+ {collector}: OK")
            logging.info(f"{collector}: OK")
            return 0
        except Exception as e:
            print(f"+ {collector}: FAILED\nException: {repr(e)}")
            logging.info("", exc_info=True)
            logging.info(f"{collector}: FAILED, Exception: {repr(e)}")
            return 1
        finally:
            os.chdir(wd)

    def doMetric(self, options, metric_name, set=1, source=None, stage=None):
        if 'metrics' in options:
            metric =  options['metrics'][metric_name]
            if stage and source:
                metric = metric.labels(stage=stage, source=source)
            elif source:
                metric = metric.labels(source=source)

            if set == 1:
               metric.inc()
            else:
               metric.set(set)

    def pushMetrics(self, options):
        if 'pushadd_prometheus' in options:
            options['pushadd_prometheus'](options['registry'])
        

    def collect(self, collector, options=None):
        # workaround for buggy libs
        wd = os.getcwd()
        os.chdir("/tmp")
        inventory = None

        self.doMetric(options, 'cloudinventario_source')
        self.doMetric(options, 'cloudinventario_entries_collected', source=collector)
        try:
            runtime_start = time.time()
            instance = self.loadCollector(collector, options)
            instance.login()
            inventory = instance.fetch()
            instance.logout()
            runtime = time.time() - runtime_start

            self.doMetric(options, 'cloudinventario_cpu_usage', source=collector, set=psutil.cpu_percent(round(runtime)))
            self.doMetric(options, 'cloudinventario_mem_usage', source=collector, set=psutil.virtual_memory()[2])
            self.doMetric(options, 'cloudinventario_runtime', source=collector, set=runtime)
            self.doMetric(options, 'cloudinventario_success', source=collector)
        except Exception as e:
          # Find stage from trackback
            runtime = time.time() - runtime_start
            self.doMetric(options, 'cloudinventario_cpu_usage', source=collector, set=psutil.cpu_percent(round(runtime)))
            self.doMetric(options, 'cloudinventario_mem_usage', source=collector, set=psutil.virtual_memory()[2])
            self.doMetric(options, 'cloudinventario_runtime', source=collector, set=runtime)

            tb = str(traceback.format_exc()).split('\n')
            for  index, line in enumerate(tb):
              if 'in collect' in line:
                stage = re.search(r'instance\.(.*)\(', tb[index+1])
                stage = stage.group(1) if stage else None 
                break
            self.doMetric(options, 'cloudinventario_error', source=collector, stage=stage)
            
            logging.error("Exception while processing collector={}".format(collector)) 
            raise
        finally:
            self.pushMetrics(options)
            os.chdir(wd)
        return inventory

    def store(self, inventory, runtime=None):
        store_config = self.config["storage"]

        with self.lock:
            store = InventoryStorage(store_config)

            store.connect()
            store.save(inventory, runtime)
            store.disconnect()

        return True

    def store_status(self, source, status, runtime=None, error=None):
        store_config = self.config["storage"]

        with self.lock:
            store = InventoryStorage(store_config)
            store.connect()
            store.log_status(source, status, runtime, error)
            store.disconnect()
        return True

    def cleanup(self, days):
        store_config = self.config["storage"]
        store = InventoryStorage(store_config)

        store.connect()
        store.cleanup(days)
        store.disconnect()
