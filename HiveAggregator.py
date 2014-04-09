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
    def __init__(self):
        ### Load Configuration
        print('[Loading Config File]')
        with open(CONFIG_FILE) as config:
            settings = json.loads(config.read())
            for key in settings:
                try:
                    getattr(self, key)
                except AttributeError as error:
                    print('\t' + key + ' : ' + str(settings[key]))
                    setattr(self, key, settings[key])
        ### ZMQ
        print('[Initializing ZMQ]')
        try:
            self.context = zmq.Context()
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.ZMQ_SERVER)
            print('\tOKAY')
        except Exception as error:
            print('\tERROR: ' + str(error))
        ### CherryPy Monitors
        print('[Enabling Monitors]')
        try:
            Monitor(cherrypy.engine, self.listen, frequency=self.CHERRYPY_INTERVAL).subscribe()
            print('\tOKAY')
        except Exception as error:
            print('\tERROR: ' + str(error))     
        ### MongoDB
        print('[Initializing Mongo]')
        try:    
            self.mongo_client = MongoClient(self.MONGO_ADDR, self.MONGO_PORT)
            self.mongo_db = self.mongo_client[self.MONGO_DB]
            print('\tOKAY')
        except Exception as error:
            print('\tERROR: ' + str(error))
        ### SKLearn
        print('[Initializing SKlearn]')
        try:
            self.svc_health = svm.SVC(kernel='rbf')
            print('\tOKAY')
        except Exception as error:
            print('\tERROR: ' + str(error))
    
    ## Train
    def train(self, log):
        pass
    
    ## Classify
    def classify(self, sample):
        prediction = 1
        return prediction
        
    ## Query Param in Range to JSON-file
    def query_range(self, start_hours, end_hours):
        print('[Querying Data Range]')
        print start_hours
        print end_hours
        with open('data/samples.json', 'w') as jsonfile:
            result = []
            start = datetime.now() - timedelta(hours = start_hours) # get datetime
            end = datetime.now() - timedelta(hours = end_hours) # get datetime
            for name in self.mongo_db.collection_names():
                if not name == 'system.indexes':
                    for sample in self.mongo_db[name].find({'time':{'$gt': end, '$lt':start}}):
                        sample['time'] = datetime.strftime(sample['time'], self.TIME_FORMAT)
                        result.append(sample)
            dump = json_util.dumps(result, indent=4)
            jsonfile.write(dump)
                
    ## Store to Mongo
    def store(self, doc):
        print('[Storing to Mongo]')
        try:
            doc['time'] = datetime.now()
            hive = self.mongo_db[doc['hive_id']]
            doc_id = hive.insert(doc)
            print('\tOKAY: ' + str(doc_id))
            return doc_id
        except Exception as error:
            print('--> ERROR: ' + str(error))
       
    ## Receive Sample
    def receive(self):
        print('[Receiving Sample from Hive]')
        try:
            packet = self.socket.recv()
            sample = json.loads(packet)
            print('\tOKAY: ' + str(sample))
            return sample
        except Exception as error:
            print('\tERROR: ' + str(error))
    
    ### Send Response
    def send(self):
        print('[Sending Response to Hive]')
        try:
            response = {'status':'okay'}
            dump = json.dumps(response)
            self.socket.send(dump)
            print('\tOKAY: ' + str(response))
        except Exception as error:
            print('\tERROR: ' + str(error))   
                       
    ## Listen for Next Sample
    def listen(self):
        print('\n')
        sample = self.receive()
        response = self.classify(sample)
        self.send()
        mongo_id = self.store(sample)
    
    ## Render Index
    @cherrypy.expose
    def index(self):
        html = open('static/index.html').read()
        return html
    
    ## Handle Posts
    @cherrypy.expose
    def default(self,*args,**kwargs): 
        print kwargs
        if kwargs['type'] == 'log':
            print 'log'
        elif kwargs['type'] == 'graph':
            if kwargs['range_select'] == 'hour':
                self.query_range(0, 1)
            elif kwargs['range_select'] == 'day':
                self.query_range(0, 24)
            elif kwargs['range_select'] == 'week':
                self.query_range(0, 168)
            elif kwargs['range_select'] == 'month':
                self.query_range(0, 744)
        else:
            pass
        return 't'
    
# Main
if __name__ == '__main__':
    aggregator = HiveAggregator()
    cherrypy.server.socket_host = aggregator.CHERRYPY_ADDR
    cherrypy.server.socket_port = aggregator.CHERRYPY_PORT
    currdir = os.path.dirname(os.path.abspath(__file__))
    conf = {
        '/': {'tools.staticdir.on':True, 'tools.staticdir.dir':os.path.join(currdir,'static')},
        '/data': {'tools.staticdir.on':True, 'tools.staticdir.dir':os.path.join(currdir,'data')}, # NEED the '/' before the folder name
    }
    cherrypy.quickstart(aggregator, '/', config=conf)
