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
            self.MONGO_DB = "%Y%m"
            self.TIME_FORMAT = "%Y%m%d%H%M%S"
            self.DATA_PATH = "data/"
            self.LOGS_FILE = "logs.json"
            self.SAMPLES_FILE = "samples.json"
            self.CSV_FILE = "%Y-%m-%d %H:%M:%S "
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
            self.dbname = datetime.strftime(datetime.now(), self.MONGO_DB)            
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
    
    ## Train Estimators
    def train_estimators(self, log):
        pretty_print('SKLEARN', 'Training Estimators')
        try:
            logs = self.mongo_db[log['hive_id']].find({'type':'log'})
            data = []
            targets = []
            for log in logs:
                start = log['time']
                end = log['time'] - timedelta(hours = 1) #! set to next log entry
                samples = self.mongo_db[log['hive_id']].find({'type':'sample', 'time':{'$gt':end, '$lt':start}})
                for sample in samples:
                    params = [sample['int_t'], sample['ext_t']]
                    state = int(log['health'])
                    data.append(params)
                    targets.append(state)
            self.svc_health.fit(data, targets)
        except Exception as error:
            pretty_print('ERROR', str(error))
        
    ## Query Samples in Range to JSON-file
    def query_samples(self, hours):
        pretty_print('MONGO', 'Querying samples for last %s hrs' % str(hours))
        time_range = datetime.now() - timedelta(hours = hours) # get datetime
        with open(self.DATA_PATH + self.SAMPLES_FILE, 'w') as jsonfile:
            result = []
            for name in self.mongo_db.collection_names():
                if not name == 'system.indexes':
                    # Add filters here to only include average values
                    matches = self.mongo_db[name].find({'type':'sample', 'time':{'$gt': time_range, '$lt':datetime.now()}})
                    for sample in matches:
                        sample['time'] = datetime.strftime(sample['time'], self.TIME_FORMAT)
                        result.append(sample)
            dump = json_util.dumps(result, indent=4)
            jsonfile.write(dump)
            
    ## Query Logs in Range to JSON-file
    def query_logs(self, hours):
        pretty_print('MONGO', 'Querying Logs for last %s hrs' % str(hours))
        time_range = datetime.now() - timedelta(hours = hours) # get datetime
        with open(self.DATA_PATH + self.LOGS_FILE, 'w') as jsonfile:
            result = []
            for name in self.mongo_db.collection_names():
                if not name == 'system.indexes':
                    matches = self.mongo_db[name].find({'type':'log', 'time':{'$gt': time_range, '$lt':datetime.now()}})
                    for log in matches:
                        log['time'] = datetime.strftime(log['time'], self.TIME_FORMAT)
                        result.append(log)
            dump = json_util.dumps(result, indent=4)
            jsonfile.write(dump)
    
    ## Dump to CSV
    #! Needs rewrite for speed
    def dump_csv(self, hours):
        pretty_print('MONGO', 'Dumping to CSV for last %s hrs' % str(hours))
        time_range = datetime.now() - timedelta(hours = hours) # get datetime
        try:
            for name in self.mongo_db.collection_names():
                if not name == 'system.indexes':
                    pretty_print('CSV', 'Collection: %s' % name)
                    filename = datetime.strftime(datetime.now(), self.CSV_FILE) + name + '.csv'
                    with open(self.DATA_PATH + filename, 'w') as csvfile:
                        csvfile.write(','.join(self.ALL_PARAMETERS) + '\n') # Write headers
                        for sample in self.mongo_db[name].find({'type':'sample', 'time':{'$gt': time_range, '$lt':datetime.now()}}):
                            sample['time'] = datetime.strftime(sample['time'], self.TIME_FORMAT)
                            sample_as_list = []
                            for param in self.ALL_PARAMETERS:
                                try:
                                    sample_as_list.append(str(sample[param]))
                                except Exception as error:
                                    sample_as_list.append('NaN') # Not-a-Number if missing
                            sample_as_list.append('\n')
                            try:
                                csvfile.write(','.join(sample_as_list))
                            except Exception as error:
                                pretty_print('ERROR', str(error))
        except Exception as error:
            pretty_print('ERROR', str(error))
       
    ## Receive Sample
    def receive_message(self):
        pretty_print('ZMQ', 'Receiving Message')
        try:
            packet = self.socket.recv()
            message = json.loads(packet)
            pretty_print('RECEIVE', 'OKAY: %s' % str(message))
            return message
        except Exception as error:
            pretty_print('ERROR', str(error))
            
    ## Store Sample
    def store_sample(self, sample):
        pretty_print('MONGO', 'Storing Sample')
        try:
            sample['time'] = datetime.now()
            mongo_db = self.mongo_client[datetime.strftime(datetime.now(), self.MONGO_DB)]
            hive = mongo_db[sample['hive_id']]
            sample_id = hive.insert(sample)
            pretty_print('MONGO', 'OKAY: %s' % str(sample_id))
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
            pretty_print('MONGO', 'OKAY: %s' % str(log_id))
            return str(log_id)
        except Exception as error:
            pretty_print('ERROR', str(error))
            
    ## Classify
    def classify_sample(self, sample):
        pretty_print('SKLEARN', 'Classifying Sample')
        try:
            try:
                health_data = [sample[v] for v in self.HEALTH_PARAMETERS]
                health = self.svc_health.predict(health_data)
            except Exception as error:
                health = None
                pretty_print('ERROR', str(error))
            try:
                activity_data = [sample[v] for v in self.ACTIVITY_PARAMETERS]
                activity = self.svc_activity.predict(actvity_data)
            except Exception as error:
                activity = None
                pretty_print('ERROR', str(error))
            try:
                environment_data = [sample[v] for v in self.ENVIRONMENT_PARAMETERS]
                environment = self.svc_environment.predict(environment_data)
            except Exception as error:
                environment = None
                pretty_print('ERROR', str(error))
            estimators = {
                'health' : health,
                'environment' : environment,
                'activity' : activity
            }
            pretty_print('SKLEARN', str(estimators))
        except Exception as error:
            estimators = {}
            pretty_print('ERROR', str(error))
        return estimators
            
    ### Send Response
    def send_response(self, estimators, sample_id):
        pretty_print('ZMQ', 'Sending Response to Hive')
        try:
            response = {
                'id' : sample_id,
                'type' : 'response',
                'time' : datetime.strftime(datetime.now(), self.TIME_FORMAT),
                'estimators' : estimators
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
            estimators = self.classify_sample(message)
            self.send_response(estimators, sample_id)
        
    ## Backup
    def backup(self):
        pretty_print('CHERRYPY', 'Backing up data')
    
    ## Check Database
    def check(self):
        pretty_print('CHERRYPY', 'Checking Database')
    
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
            print('\tPOST: %s' % str(kwargs))
            if kwargs['type'] == 'log':
                self.store_sample(kwargs)
                self.train_estimators(kwargs)
            elif kwargs['type'] == 'graph':
                self.query_samples(int(kwargs['range_select']))
            elif kwargs['type'] == 'save':
                self.dump_csv(int(kwargs['range_select']))
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
