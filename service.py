#!/usr/bin/env python3
import os
import sys
import logging
import setproctitle
import multiprocessing
import psutil
import time
import re
import traceback
import argparse

# Flask
from flask import Flask, request
from flask_executor import Executor

# PROMETHEUS
from prometheus_client import Counter, Gauge, generate_latest

# SENTRY
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.excepthook import ExcepthookIntegration

# Cloudinventario
DN = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DN + '/src')
from cloudinventario.cloudinventario import CloudInventario
import cloudinventario.storage as storage

# Create APP
app = Flask(__name__)
executor = Executor(app)
CONFIG, METRICS_DICT = None, None
TASKS = []

# --- ROUTES ---
@app.route("/metrics")
def metrics():
  return generate_latest()
# curl -X GET http://0.0.0.0:8000/metrics

@app.route("/status/<job_id>")
def status_job(job_id):
  logging.info(f"Status about job={job_id}")
  if job_id in TASKS:
    if executor.futures.done(job_id):
      TASKS.remove(job_id)
      future = executor.futures.pop(job_id)
      doMetrics(future, METRICS_DICT)
      return {"status": "success", "result": future.result()}
    else:
      return {"status": "pending", "result": "Task is still running"}
  return {"status": "error", "result": "Task not found"}
# curl -X GET http://0.0.0.0:8000/status/

@app.route("/status")
def status():
  finished_task_id = []

  for task in TASKS:
    if executor.futures.done(task):
      finished_task_id.append(task)

      TASKS.remove(task)
      future = executor.futures.pop(task)
      doMetrics(future, METRICS_DICT)
  result =  {
    "status": "success",
    "finished_tasks": len(finished_task_id),
    "not_finished_tasks": len(TASKS),
    "names_finished_tasks": finished_task_id,
    "names_not_finished_tasks": TASKS,
    }
  logging.info(f"Status about tasks={result}")
  return result
# curl -X GET http://0.0.0.0:8000/status

@app.route("/collect", methods=["POST"])
def collect():
    collector_config = request.get_json()
    logging.info(f"[+] Processing collector {collector_config['collectors'].keys()}")
    collector_config['storage'] = CONFIG['storage'] 
    cinv = CloudInventario(collector_config)

    try:
      ids = {}
      for col in cinv.collectors:
        METRICS_DICT['cloudinventario_source'].inc()
        METRICS_DICT['cloudinventario_entries_collected'].labels(source=col).inc()
        data = {         
          "config": collector_config,
          "name": col,
          "options": {'tasks': int(CONFIG['process']['tasks']), 'check_permission': False}
        }

        id = col + ":" + str(time.time())
        ids[col] = id
        TASKS.append(id)
        executor.submit_stored(id, collect, data)
        # future_result = future_result.result() # sync

        METRICS_DICT['cloudinventario_up'].inc() if executor.futures.done(id) else None
      return {"status": "success", "task_id": str(ids)}
    except Exception as e: 
      print(traceback.format_exc())
      return {"status": "error", "exception": str(e)}
# curl -X POST -H "Content-Type: application/json" -d '{"collectors": {"aws1": {"module": "amazon-aws","config": {"access_key": "","secret_key": "", "region": "eu-west-1","collect": ["snapshot"]}}}}' http://0.0.0.0:8000/collect

# --- HELPERS METHOD ---
def doMetrics(future, metrics_dict):
  future_result = future.result()
  metrics_dict['cloudinventario_cpu_usage'].labels(source=future_result[1]['name']).set(future_result[1]['cpu_usage'])
  metrics_dict['cloudinventario_mem_usage'].labels(source=future_result[1]['name']).set(future_result[1]['mem_usage'])
  metrics_dict['cloudinventario_runtime'].labels(source=future_result[1]['name']).set(future_result[1]['runtime'])
  if future_result[0] is True:
    metrics_dict['cloudinventario_success'].labels(source=future_result[1]['name']).inc()
  else:
    metrics_dict['cloudinventario_error'].labels(source=future_result[1]['name'], stage=future_result[1]['stage']).inc()

def collect(data):
   config = data['config']
   name = data['name']
   options = data['options']

   proctitle = setproctitle.getproctitle()
   setproctitle.setproctitle("[cloudinventario] {}".format(name))
   multiprocessing.current_process().name = name

   cinv = CloudInventario(config)

   logging.info("collector name={}".format(name))
   runtime_start = time.time()
   try:
     # Check if testing login
    #  if args.test_login:
    #     return cinv.login(name, options)
     runtime_start = time.time()
     inventory = cinv.collect(name, options)
     runtime = time.time() - runtime_start
     cpu_usage = psutil.cpu_percent(round(runtime))
     mem_usage = psutil.virtual_memory()[2]
     if inventory is not None:
        logging.info("storing data for name={}".format(name))
        cinv.store(inventory, runtime)
        return True, {'name': name, 'runtime': runtime, 'cpu_usage': cpu_usage, 'mem_usage': mem_usage}
     else:
        cinv.store_status(name, storage.STATUS_FAIL, runtime)
        logging.info("collector failed name={}".format(name))
   except Exception as e:
     runtime = time.time() - runtime_start
     cpu_usage = psutil.cpu_percent(round(runtime))
     mem_usage = psutil.virtual_memory()[2]
     trace = traceback.format_exc()
     tb = str(traceback.format_exc()).split('\n')
     for  index, line in enumerate(tb):
        if 'in collect' in line:
          stage = re.search(r'([a-z]*?)\.(.*)\(', tb[index+1])
          stage = stage.group(2) if stage else None 
          break

     cinv.store_status(name,  storage.STATUS_ERROR, runtime, trace)
     logging.error("collector failed with exception", exc_info=e)
     return False, {'name': name, 'runtime': runtime, 'cpu_usage': cpu_usage, 'mem_usage': mem_usage, 'stage': stage}
   finally:
     setproctitle.setproctitle(proctitle)
   return False, {'name': name, 'runtime': runtime, 'cpu_usage': cpu_usage, 'mem_usage': mem_usage, 'stage': 'end'}

# --- CONFIGS ---
# create metrics for Prometheus
def prometheusConfig():
  logging.info("Prometheus initializing metrics")
  metrics_dict = dict()
  metrics_dict['cloudinventario_entries_collected'] = Counter(
      'cloudinventario_entries_collected',
      'Number of collected Cloudinventario entries',
      ['source']
  )
  metrics_dict['cloudinventario_source'] = Counter(
      'cloudinventario_source',
      'Count of Cloudinventario source'
  )
  metrics_dict['cloudinventario_up'] = Counter(
      'cloudinventario_up',
      'if last Cloudinventario was success'
  )
  metrics_dict['cloudinventario_runtime'] = Gauge(
      'cloudinventario_runtime',
      'Runtime for cloudinventario',
      ['source']
  )
  metrics_dict['cloudinventario_success'] = Gauge(
      'cloudinventario_success',
      'How many Cloudinventario ended as success',
      ['source']
  )
  metrics_dict['cloudinventario_error'] = Gauge(
      'cloudinventario_error',
      'How many Cloudinventario ended as error',
      ['source', 'stage']
  )
  metrics_dict['cloudinventario_cpu_usage'] = Gauge(
      'cloudinventario_cpu_usage',
      'How much CPU was used during collection Cloudinventario entry',
      ['source']
  )
  metrics_dict['cloudinventario_mem_usage'] = Gauge(
      'cloudinventario_mem_usage',
      'How much CPU was used during collection Cloudinventario entry',
      ['source']
  )
  return metrics_dict

# load config and init for Sentry
def sentryConfig():
  dsn = os.getenv("SENTRY_DSN")

  if not dsn:
    return False
  environment = os.getenv("SENTRY_ENVIRONMENT")
  traces_sample_rate = float(os.getenv("SENTRY_TSR"))
  event_level = int(os.getenv("SENTRY_EVENT_LEVEL"))
  level = int(os.getenv("SENTRY_LEVEL"))

  logging.info("SentryConfig with dsn={}, env={}, traces_sample_rate={}, event_lvl={}".format(dsn, environment, traces_sample_rate, event_level))
  sentry_sdk.init(
    dsn=dsn, 
    integrations=[
        LoggingIntegration(
            level=level,
            event_level=event_level
        ),
        ExcepthookIntegration(always_run=True)
    ],
    environment=environment, 
    traces_sample_rate=traces_sample_rate, 
  )
  return True

# process port and host from args (easer option for more services)
def getArgs():
  parser = argparse.ArgumentParser(description='CloudInventory args')
  parser.add_argument('--port', action='store', help='Endpoint port')
  parser.add_argument('--host', action='store', help='Endpoint host')
  return parser.parse_args()

# load config for Process
def processesConfig():
  args = getArgs()
  app.config['EXECUTOR_TYPE'] = 'process'#'thread'
  app.config['EXECUTOR_MAX_WORKERS'] = int(os.getenv('PROCESS_FORKS') or 1)
  logging.info(f"Confif with EXECUTOR_MAX_WORKERS={os.getenv('PROCESS_FORKS') or 1}, PROCESS_TASKS={os.getenv('PROCESS_TASKS')}")
  return {
    'storage': {'dsn': os.getenv('STORAGE_DSN')},
    'process': {
      'forks': int(os.getenv('PROCESS_FORKS') or 1),
      'tasks': int(os.getenv('PROCESS_TASKS') or 1),
      'die_after_request': os.getenv('PROCESS_DIE_AFTER_REQUEST')
    },
    'endpoint_host': args.host if args.host else os.getenv('ENDPOINT_HOST'),
    'endpoint_port': args.port if args.port else os.getenv('ENDPOINT_PORT')
  }
# export ENDPOINT_PORT=8000 ENDPOINT_HOST=0.0.0.0 PROCESS_FORKS=2 PROCESS_TASKS=1 PROCESS_DIE_AFTER_REQUEST=False STORAGE_DSN=sqlite:///cloudinventory.db SENTRY_LEVEL=40 SENTRY_DSN=https://f27cb7376403487a8d068ca2edaa0863@o1307650.ingest.sentry.io/6552197 SENTRY_ENVIRONMENT=dev SENTRY_TSR=1.0 SENTRY_EVENT_LEVEL=40

if __name__ == '__main__':
  # Load config form env and args port/host
  CONFIG = processesConfig()
  # Create metrics
  METRICS_DICT = prometheusConfig()
  # Initializing sentry logging 
  sentryConfig()
  logging.info(f"Initializing DB with url {CONFIG['storage']}")
  cinv = CloudInventario({'storage': CONFIG['storage']})
  cinv.store(None)

  logging.info(f"Running server with {CONFIG['endpoint_host']}:{CONFIG['endpoint_port']}")
  app.run(debug=True, host=CONFIG['endpoint_host'], port=CONFIG['endpoint_port'])
