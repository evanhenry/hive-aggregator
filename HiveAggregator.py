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
import numpy
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
    CONFIG_FILE = 'settings.json'

# HiveAggregator CherryPy server
class HiveAggregator:

    ## Initialize
    def __init__(self, config_file):
        self.init_config(config_file)
        self.init_zmq()
        self.init_cherrypy()
        self.init_mongo()
        self.init_sklearn()
    
    ## Load Configuration
    def init_config(self, config_file):
        print('\n[Loading Config File]')
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
        print('\t[Initializing ZMQ]' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
        try:
            self.context = zmq.Context()
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.ZMQ_SERVER)
            print('\t\tOKAY')
        except Exception as error:
            print('\t\tERROR: ' + str(error))
    
    ## Initialize CherryPy
    def init_cherrypy(self):   
        print('\n[Enabling Monitors]' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
        try:
            Monitor(cherrypy.engine, self.listen, frequency=self.CHERRYPY_INTERVAL).subscribe()
            print('\tOKAY')
        except Exception as error:
            print('\tERROR: ' + str(error))     
    
    ## Initialize MongoDB
    def init_mongo(self):
        print('\n[Initializing Mongo] ' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
        try:    
            self.mongo_client = MongoClient(self.MONGO_ADDR, self.MONGO_PORT)
            self.mongo_db = self.mongo_client[self.MONGO_DB]
            print('\tOKAY')
        except Exception as error:
            print('\tERROR: ' + str(error))
    
    ## Initialize SKlearn
    def init_sklearn(self):     
        print('\n[Initializing SKlearn] ' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
        try:
            self.svc_health = svm.SVC(kernel='rbf')
            print('\tOKAY')
        except Exception as error:
            print('\tERROR: ' + str(error))
    
    ## Train
    def train(self, log):
        print('\n[Training SVM] ' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
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
            print('\tOKAY')
        except Exception as error:
            print('\tERROR: ' + str(error))
    
    ## Classify
    def classify(self, sample):
        print('\n[Classifying State] ' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
        try:
            data = [sample['int_t'], sample['ext_t']]
            prediction = self.svc_health.predict(data)
            print('\tCLASS: ' + str(int(prediction)))
            return prediction
        except Exception as error:
            print('\tERROR: ' + str(error))
        
    ## Query Samples in Range to JSON-file
    def query_samples(self, hours):
        print('\n[Querying Samples in Range] ' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
        print('\tRange: ' + str(hours))
        time_range = datetime.now() - timedelta(hours = hours) # get datetime
        with open('data/samples.json', 'w') as jsonfile:
            result = []
            for name in self.mongo_db.collection_names():
                if not name == 'system.indexes':
                    for sample in self.mongo_db[name].find({'type':'sample', 'time':{'$gt': time_range, '$lt':datetime.now()}}):
                        sample['time'] = datetime.strftime(sample['time'], self.TIME_FORMAT)
                        result.append(sample)
            dump = json_util.dumps(result, indent=4)
            jsonfile.write(dump)
            
    ## Query Logs in Range to JSON-file
    def query_logs(self, hours):
        print('[Querying from Mongo]')
        print('\tRange: ' + str(hours))
        time_range = datetime.now() - timedelta(hours = hours) # get datetime
        with open('data/logs.json', 'w') as jsonfile:
            result = []
            for name in self.mongo_db.collection_names():
                if not name == 'system.indexes':
                    for log in self.mongo_db[name].find({'type':'log', 'time':{'$gt': time_range, '$lt':datetime.now()}}):
                        log['time'] = datetime.strftime(log['time'], self.TIME_FORMAT)
                        result.append(log)
            dump = json_util.dumps(result, indent=4)
            jsonfile.write(dump)
    
    ## Dump to CSV
    def dump_csv(self, hours):
        print('\n[Dumping to CSV] '+ datetime.strftime(datetime.now(), self.TIME_FORMAT))
        print('\tRange: ' + str(hours))
        time_range = datetime.now() - timedelta(hours = hours) # get datetime
        with open('data/samples.csv', 'w') as csvfile:
            for name in self.mongo_db.collection_names():
                    if not name == 'system.indexes':
                        for sample in self.mongo_db[name].find({'type':'sample', 'time':{'$gt': time_range, '$lt':datetime.now()}}):
                            sample['time'] = datetime.strftime(sample['time'], self.TIME_FORMAT)
                            sample_as_list = []
                            for param in self.PARAMS:
                                try:
                                    sample_as_list.append(str(sample[param]))
                                except Exception as err:
                                    print str(err)
                                    sample_as_list.append('NaN')
                            sample_as_list.append('\n')
                            try:
                                csvfile.write(','.join(sample_as_list))
                            except Exception as err:
                                print str(err)
                
    ## Store to Mongo
    def store(self, doc):
        print('\n[Storing to Mongo] ' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
        try:
            doc['time'] = datetime.now()
            hive = self.mongo_db[doc['hive_id']]
            doc_id = hive.insert(doc)
            print('\tDOC_ID: ' + str(doc_id))
            return doc_id
        except Exception as error:
            print('\tERROR: ' + str(error))
       
    ## Receive Sample
    def receive(self):
        print('\n[Receiving Sample from Hive] ' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
        try:
            packet = self.socket.recv()
            sample = json.loads(packet)
            print('\tOKAY: ' + str(sample))
            return sample
        except Exception as error:
            print('\tERROR: ' + str(error))
    
    ### Send Response
    def send(self):
        print('\n[Sending Response to Hive] ' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
        try:
            response = {'status':'okay'}
            dump = json.dumps(response)
            self.socket.send(dump)
            print('\tRESPONSE: ' + str(response))
        except Exception as error:
            print('\tERROR: ' + str(error))   
                       
    ## Listen for Next Sample
    def listen(self):
        sample = self.receive()
        response = self.classify(sample)
        self.send()
        mongo_id = self.store(sample)
    
    ## Render Index
    @cherrypy.expose
    def index(self):
        print('\n[Loading Index Page] ' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
        html = open('static/index.html').read()
        return html
    
    ## Handle Posts
    @cherrypy.expose
    def default(self,*args,**kwargs):
        print('\n[Received POST Request] ' + datetime.strftime(datetime.now(), self.TIME_FORMAT))
        try:
            print('\t POST: ' + str(kwargs))
            if kwargs['type'] == 'log':
                self.store(kwargs)
                self.train(kwargs)
            elif kwargs['type'] == 'graph':
                self.query_samples(int(kwargs['range_select']))
            elif kwargs['type'] == 'save':
                self.dump_csv(int(kwargs['range_select']))
        except Exception as err:
            print('\tERROR: ' + str(err))
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
