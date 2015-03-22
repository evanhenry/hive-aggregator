#!/usr/bin/env python
"""
HiveAggregator
Developed by Trevor Stanhope

HiveAggregator is a minimal CherryPy instance

TODO:
- Log post --> train learners
"""

# Libraries
import json
import ast
import cherrypy
import os
import sys
import numpy as np
from datetime import datetime, timedelta
from cherrypy.process.plugins import Monitor
from cherrypy import tools
from pymongo import MongoClient
from bson import json_util
import zmq
from sklearn import svm

# Constants
try:
    CONFIG_FILE = sys.argv[1]
except Exception as err:
    CONFIG_FILE = None

## Pretty Print
def pretty_print(task, msg):
    date = datetime.strftime(datetime.now(), '%d/%b/%Y:%H:%M:%S')
    print('[%s] %s %s' % (date, task, msg))

# HiveAggregator CherryPy server
class HiveAggregator:
    
    ## Initialize
    def __init__(self, config_path):
        
        # Configuration
        if not config_path:
            self.USER_ID = "trevstanhope"
            self.AGGREGATOR_ID = "MAA"
            self.ZMQ_SERVER = "tcp://*:1980"
            self.CHERRYPY_LISTEN_INTERVAL = 0.1
            self.CHERRYPY_BACKUP_INTERVAL = 1500
            self.CHERRYPY_CHECK_INTERVAL = 1500
            self.CHERRYPY_PORT = 8080
            self.CHERRYPY_ADDR = "0.0.0.0"
            self.MONGO_ADDR = "127.0.0.1"
            self.MONGO_PORT = 27017
            self.MONGO_DB = "%Y%m%d"
            self.TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
            self.DATA_PATH = "data/"
            self.LOGS_FILE = "logs.json"
            self.SAMPLES_FILE = "samples.json"
            self.CSV_FILE = "%Y-%m-%d %H:%M:%S"
            self.ENVIRONMENT_PARAMETERS = ["ext_t", "ext_h", "pa"]
            self.HEALTH_PARAMETERS = ["int_t", "int_h"]
            self.ACTIVITY_PARAMETERS = ["db", "hz"]
            self.ALL_PARAMETERS = ["time","int_t","ext_t","int_h","ext_h","hz","db","volts","amps","pa"]
        else:
            self.load_config(config_path)
        
        # Initializers
        self.init_zmq()
        self.init_tasks()
        self.init_mongo()
        self.init_sklearn()
    
    ## Load Configuration
    def load_config(self, config_path):
        pretty_print('CONFIG', 'Loading Config File')
        with open(config_file) as config:
            settings = json.loads(config.read())
            for key in settings:
                try:
                    getattr(self, key)
                except AttributeError as error:
                    print('\t' + key + ' : ' + str(settings[key]))
                    setattr(self, key, settings[key])
    
    ## Initialize ZMQ
    def init_zmq(self):      
        pretty_print('ZMQ', 'Initializing ZMQ')
        try:
            self.context = zmq.Context()
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.ZMQ_SERVER)
        except Exception as error:
            pretty_print('ERROR', str(error))
    
    ## Initialize Tasks
    def init_tasks(self):
        pretty_print('CHERRYPY', 'Initializing Monitors')
        try:
            Monitor(cherrypy.engine, self.listen, frequency=self.CHERRYPY_LISTEN_INTERVAL).subscribe()
            Monitor(cherrypy.engine, self.backup, frequency=self.CHERRYPY_BACKUP_INTERVAL).subscribe()
            Monitor(cherrypy.engine, self.check, frequency=self.CHERRYPY_CHECK_INTERVAL).subscribe()
        except Exception as error:
            pretty_print('ERROR', str(error))
    
    ## Initialize MongoDB
    def init_mongo(self):
        pretty_print('MONGO', 'Initializing Mongo')
        try:
            self.mongo_client = MongoClient(self.MONGO_ADDR, self.MONGO_PORT)
        except Exception as error:
            pretty_print('ERROR', str(error))
    
    ## Initialize SKlearn
    def init_sklearn(self):     
        pretty_print('SKLEARN', 'Initializing SKlearn')
        try:
            ## Learning Estimators
            self.svc_health = svm.SVC(kernel='rbf')
            self.svc_environment = svm.SVC(kernel='rbf')
            self.svc_activity = svm.SVC(kernel='rbf')
        except Exception as error:
            pretty_print('ERROR', str(error))
        
    ## Query Samples in Range to JSON-file
    def query_samples(self, days):
        pretty_print('MONGO', 'Querying samples for last %s days' % str(days))
        with open(self.DATA_PATH + self.SAMPLES_FILE, 'w') as jsonfile:
            result = []
            for d in range(days):
                date = datetime.now() - timedelta(days = d)
                db_name = datetime.strftime(date, self.MONGO_DB)
                mongo_db = self.mongo_client[db_name]
                for name in mongo_db.collection_names():
                    if not name == 'system.indexes':
                        matches = mongo_db[name].find({'type':'sample'})
                        for sample in matches:
                            sample['time'] = datetime.strftime(sample['time'], self.TIME_FORMAT)
                            result.append(sample)
            dump = json_util.dumps(result, indent=4)
            jsonfile.write(dump)
        return result
            
    ## Query Logs in Range to JSON-file
    def query_logs(self, days):
        pretty_print('MONGO', 'Querying Logs for last %s days' % str(days))
        with open(self.DATA_PATH + self.LOGS_FILE, 'w') as jsonfile:
            result = []
            for d in range(days):
                date = datetime.now() - timedelta(days = d)
                db_name = datetime.strftime(date, self.MONGO_DB)
                mongo_db = self.mongo_client[db_name]
                for name in self.mongo_db.collection_names():
                    if not name == 'system.indexes':
                        matches = mongo_db[name].find({'type':'log'})
                        for log in matches:
                            log['time'] = datetime.strftime(log['time'], self.TIME_FORMAT)
                            result.append(log)
            dump = json_util.dumps(result, indent=4)
            jsonfile.write(dump)
        return result
    
    ## Dump to CSV
    #! Needs rewrite for speed
    def dump_csv(self, days):
        pretty_print('MONGO', 'Dumping to CSV for last %s days' % str(days))
        results = self.query_samples(days)
        try:
            with open(self.DATA_PATH + 'samples.csv', 'w') as csvfile:
                for sample in results:
                    del sample['_id']
                    a = [i for i in sample.values()]
                    a.append('\n')
                    out = ','.join(a)
                    csvfile.write(out)
        except Exception as error:
            pretty_print('ERROR', str(error))
       
    ## Receive Sample
    def receive_message(self):
        pretty_print('ZMQ', 'Receiving Message')
        try:
            packet = self.socket.recv()
            message = json.loads(packet)
            pretty_print('ZMQ', 'OKAY: %s' % str(message))
            return message
        except Exception as error:
            pretty_print('ERROR', str(error))
            
    ## Store Sample
    def store_sample(self, sample):
        pretty_print('MONGO', 'Storing Sample')
        try:
            sample['time'] = datetime.now()
            db_name = datetime.strftime(datetime.now(), self.MONGO_DB) # this is the mongo db it saves to
            mongo_db = self.mongo_client[db_name]
            hive = mongo_db[sample['hive_id']]
            sample_id = hive.insert(sample)
            pretty_print('MONGO', 'Sample ID: %s' % str(sample_id))
            return str(sample_id)
        except Exception as error:
            pretty_print('ERROR', str(error))
                        
    ## Store Log
    def store_log(self, log):
        pretty_print('MONGO', 'Storing Log')
        try:
            log['time'] = datetime.now()
            hive = self.mongo_db[sample['hive_id']]
            log_id = hive.insert(log)
            pretty_print('MONGO', 'Log ID: %s' % str(log_id))
            return str(log_id)
        except Exception as error:
            pretty_print('ERROR', str(error))
            
    ### Send Response
    def send_response(self, status, sample_id):
        pretty_print('ZMQ', 'Sending Response to Hive')
        try:
            response = {
                'id' : sample_id,
                'status' : status,
                'type' : 'response',
                'time' : datetime.strftime(datetime.now(), self.TIME_FORMAT),
                }
            dump = json.dumps(response)
            self.socket.send(dump)
            pretty_print('ZMQ', str(response))
        except Exception as error:
            pretty_print('ERROR', str(error))   
    
    """
    Periodic Functions
    """
    ## Listen for Next Sample
    def listen(self):
        pretty_print('CHERRYPY', 'Listening for nodes')
        message = self.receive_message()
        if message['type'] == 'sample':
            sample_id = self.store_sample(message)
            if sample_id:
                status = 'ok'
            else:
                status = 'bad'
            self.send_response(status, sample_id)
        
    ## Backup
    def backup(self):
        pretty_print('CHERRYPY', 'Backing up data')
    
    ## Check Database
    def check(self):
        pretty_print('CHERRYPY', 'Checking database')
    
    """
    Handler Functions
    """
    ## Render Index
    @cherrypy.expose
    def index(self):
        html = open('static/index.html').read()
        return html
    
    ## Handle Posts
    @cherrypy.expose
    def default(self, *args, **kwargs):
        try:
            if kwargs['type'] == 'log':
                self.store_sample(kwargs)
                self.train_estimators(kwargs)
            elif kwargs['type'] == 'graph':
                return self.query_samples(int(kwargs['range_select']))
            elif kwargs['type'] == 'save':
                self.dump_csv(int(kwargs['range_select']))
            else:
                pass
        except Exception as err:
            pretty_print('ERROR', str(err))
        return None
    
# Main
if __name__ == '__main__':
    aggregator = HiveAggregator(CONFIG_FILE)
    cherrypy.server.socket_host = aggregator.CHERRYPY_ADDR
    cherrypy.server.socket_port = aggregator.CHERRYPY_PORT
    currdir = os.path.dirname(os.path.abspath(__file__))
    conf = {
        '/': {'tools.staticdir.on':True, 'tools.staticdir.dir':os.path.join(currdir,'static')},
        '/data': {'tools.staticdir.on':True, 'tools.staticdir.dir':os.path.join(currdir,'data')}, # NEED the '/' before the folder name
    }
    cherrypy.quickstart(aggregator, '/', config=conf)
