import logging, re
from pprint import pprint
from datetime import datetime, timedelta
from sqlalchemy.pool import NullPool
import json

import sqlalchemy as sa

TABLE_PREFIX = "ci_"

STATUS_OK = "OK"
STATUS_FAIL = "FAIL"
STATUS_ERROR = "ERROR"

class InventoryStorage:

   def __init__(self, config):
     self.config = config
     self.dsn = config["dsn"]
     self.engine = self.__create()
     self.conn = None
     self.version = 0



   def __del__(self):
     if self.conn:
       self.disconnect()
     self.engine.dispose()

   def __create(self):
     return sa.create_engine(self.dsn, echo=False, poolclass=NullPool)

   def connect(self):
     self.conn = self.engine.connect()
     #self.conn.execution_options(autocommit=True)
     if not self.__check_schema():
       self.__create_schema()
     self.__prepare()
     return True

   def __check_schema(self):
     return False

   def __create_schema(self):
     meta = sa.MetaData()
     self.source_table = sa.Table(TABLE_PREFIX + 'source', meta,
       sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),

       sa.Column('ts', sa.String, default=sa.func.now()),
       sa.Column('source', sa.String),
       sa.Column('version', sa.Integer, default=1),

       sa.Column('runtime', sa.Integer),
       sa.Column('entries', sa.Integer),
       sa.Column('status', sa.String),
       sa.Column('error', sa.Text),

       sa.UniqueConstraint('source', 'version')
     )

     self.inventory_table = sa.Table(TABLE_PREFIX + 'inventory', meta,
       sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
       sa.Column('source_id', sa.Integer, nullable=True),
       sa.Column('source_name', sa.String, nullable=False),
       sa.Column('source_version', sa.Integer, nullable=False),

       sa.Column('inventory_type', sa.String, nullable=False),

       sa.Column('uniqueid', sa.String), # TODO: nullable=False),
       sa.Column('name', sa.String),
       sa.Column('cluster', sa.String),
       sa.Column('project', sa.String),
       sa.Column('location', sa.String),
       sa.Column('created', sa.String),

       sa.Column('cpus', sa.Integer),
       sa.Column('memory', sa.Integer),
       sa.Column('disks', sa.Integer),
       sa.Column('storage', sa.Integer),

       sa.Column('primary_ip', sa.String),
       sa.Column('primary_fqdn', sa.String),

       sa.Column('os', sa.String),
       sa.Column('os_family', sa.String),

       sa.Column('status', sa.String),
       sa.Column('is_on', sa.Integer),

       sa.Column('networks', sa.String),
       sa.Column('storages', sa.String),

       sa.Column('owner', sa.String),
       sa.Column('tags', sa.Text),

       sa.Column('description', sa.String),

       sa.Column('attributes', sa.Text),
       sa.Column('details', sa.Text),

       sa.UniqueConstraint('source_version', 'source_name', 'inventory_type', 'name', "cluster", 'project', 'uniqueid')
     )

     self.dns_domain = sa.Table(TABLE_PREFIX + 'dns_domain', meta,
       sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),

       sa.Column('source_id', sa.Integer, nullable=True),
       sa.Column('source_name', sa.String, nullable=False),
       sa.Column('source_version', sa.Integer, nullable=False),

       sa.Column('inventory_type', sa.String, nullable=False),

       sa.Column('cluster', sa.String),
       sa.Column('project', sa.String),
       sa.Column('created', sa.String),

       sa.Column('uniqueid', sa.String, nullable=False),
       sa.Column('name', sa.String),
       sa.Column('type', sa.String),
       sa.Column('ttl', sa.String),

       sa.Column('owner', sa.String),
       sa.Column('tags', sa.Text),

       sa.Column('description', sa.String),

       sa.Column('attributes', sa.Text),
       sa.Column('details', sa.Text),

       #sa.UniqueConstraint('source_version', 'source_name', 'inventory_type', 'name', 'uniqueid')  # TODO !
     )

     self.dns_record = sa.Table(TABLE_PREFIX + 'dns_record', meta,
       sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),

       sa.Column('source_id', sa.Integer, nullable=False),
       sa.Column('source_name', sa.String, nullable=False),
       sa.Column('source_version', sa.Integer, nullable=False),

       sa.Column('domain_id', sa.Integer, nullable=False),
       sa.Column('domain_name', sa.String, nullable=False),

       sa.Column('inventory_type', sa.String, nullable=False),

       sa.Column('cluster', sa.String),
       sa.Column('project', sa.String),
       sa.Column('created', sa.String),

       sa.Column('uniqueid', sa.String, nullable=False),
       sa.Column('name', sa.String),
       sa.Column('type', sa.String),
       sa.Column('ttl', sa.String),
       sa.Column('data', sa.Text),

       sa.Column('owner', sa.String),
       sa.Column('tags', sa.Text),

       sa.Column('description', sa.String),

       sa.Column('attributes', sa.Text),
       sa.Column('details', sa.Text),

       #sa.UniqueConstraint('source_version', 'source_name', 'inventory_type', 'name', 'uniqueid') # TODO !
     )

     self.usage_cost = sa.Table(TABLE_PREFIX + 'usage_cost', meta,
       sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),

       sa.Column('source_id', sa.Integer, nullable=False),
       sa.Column('source_name', sa.String, nullable=False),
       sa.Column('source_version', sa.Integer, nullable=False),

       sa.Column('inventory_type', sa.String, nullable=False),

       sa.Column('period_type', sa.String),
       sa.Column('period_from', sa.String),
       sa.Column('period_to', sa.String),

       sa.Column('cost_centre', sa.String),
       sa.Column('cost', sa.Float),
       sa.Column('unit', sa.String),

       sa.Column('attributes', sa.Text),
       sa.Column('details', sa.Text),
       sa.Column('attachment', sa.LargeBinary),
     )

     meta.create_all(self.engine, checkfirst = True)

     self.TABLES = {
       'inventory':  self.inventory_table,
       'dns_domain': self.dns_domain,
       'dns_record': self.dns_record,
       'usage_cost': self.usage_cost,
     }
     return True

   def __prepare(self):
     pass

   def __get_sources_version_max(self):
     # get active version
     res = self.conn.execute(sa.select([
                   self.source_table.c.source,
                   sa.func.max(self.source_table.c.version).label("version")])
     	              .group_by(self.source_table.c.source))
     res = res.fetchall()
     if res and res[0]["version"]:
       sources = [dict(row) for row in res]
     else:
       sources = []
     return sources

   def __get_source_version_max(self, name):
     sources = self.__get_sources_version_max()
     for source in sources:
       if name == source["source"]:
         return source["version"]
     return 0

   def log_status(self, source, status, runtime = None, error = None):
     version = self.__get_source_version_max(source)

     data = {
       "source_id": -1,
       "source_name": source,
       "source_version": version + 1,
       "status": status,
       "runtime": runtime,
       "error": error
     }

     with self.engine.begin() as conn:
       conn.execute(self.source_table.insert(), data)
     return True

   def save(self, data, runtime = None):
     if data is None:
       return False

     sources = self.__get_sources_version_max()

     # increment versions
     versions = {}
     for source in sources:
       source["version"] += 1
       versions[source["source"]] = source["version"]

     # collect data sources versions
     source_entries = {}
     for rec in data:
       if rec["source_name"] not in versions.keys():
         versions[rec["source_name"]] = 1
         sources.append({ "source": rec["source_name"],
                          "version": versions[rec["source_name"]] })
       rec["source_version"] = versions.get(rec["source_name"], 1)
       source_entries.setdefault(rec["source_name"], 0)
       source_entries[rec["source_name"]] += 1

     # save entry counts
     sources_save = []
     for source in sources:
       if not source["source"] in source_entries:
         continue
       source["entries"] = source_entries[source["source"]]
       source["status"] = STATUS_OK
       source["runtime"] = runtime
       sources_save.append(source)

     # known tables
     data_to_insert = dict()
     for table in self.TABLES.keys():
       data_to_insert[table] = []

     for item in data:
         table = item.pop('__table', 'inventory') or 'inventory'

         data_to_insert[table].append(item)
#         data_to_insert[table].append(dict(item, **json.loads(item['attributes'])))

     if len(sources) == 0:
       return False

     # store data
     with self.engine.begin() as conn:
       conn.execute(self.source_table.insert(), sources_save)

       for table in data_to_insert.keys():
          if len(data_to_insert[table]) > 0:
            conn.execute(self.TABLES[table].insert(), data_to_insert[table])
     return True

   def cleanup(self, days):
     res = self.conn.execute(sa.select([
                   self.source_table.c.source,
                   self.source_table.c.version])
		.where(self.source_table.c.ts <= datetime.today() - timedelta(days=days)))
     res = res.fetchall()

     with self.engine.begin() as conn:
       for row in res:
         logging.debug("prune: source={}, version={}".format(row["source"], row["version"]))
         conn.execute(self.source_table.delete().where(
               (self.source_table.c.source == row["source"]) &
                  (self.source_table.c.version == row["version"])
           ))

         for table in self.TABLES.keys():
           conn.execute(self.TABLES[table].delete().where(
                 (self.TABLES[table].c.source_name == row["source"]) &
                    (self.TABLES[table].c.source_version == row["version"])
             ))
     return True

   def disconnect(self):
     self.conn.invalidate()
     self.conn.close()
     self.conn = None
     return True
