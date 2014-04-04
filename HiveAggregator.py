#!/usr/bin/env python
"""
HiveAggregator
Developed by Trevor Stanhope

HiveAggregator is a minimal CherryPy instance

TODO:
- Scatter plot, not line graph

"""

# Libraries
import json
import ast
import cherrypy
import os
import urllib2
import sys
import numpy
from datetime import datetime, timedelta
from cherrypy.process.plugins import Monitor
from cherrypy import tools
from pymongo import MongoClient
import zmq

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
    
    ## Train with User Log
    def train(self, log):
        hive_id = log['hive_id']
        collection = self.mongo_db[hive_id]
        period = datetime.now()
        event = {'type':'event'}
        for sample in collection.find({'time':{'$lt':period}, 'type':'sample'}).sort('time'):
            for param in params:
                event[param] = sample[param]    
        return event

    ## Classify Sample
    def classify(self, sample):
        hive_id = sample['hive_id']
        collection = self.mongo_db[hive_id]
        period = datetime.now()
        for event in collection.find({'time':{'$lt':period}, 'type':'event'}).sort('time'):
            pass
        return {'none'}

    ## Query Last 24 hours to CSV
    def query_range(self, param1, param2, hours, filename):
        print('[Querying Last 24 Hours]')
        with open('data/' + filename + '.csv', 'w') as datafile:
            datafile.write(','.join(['hive_id','time',param1, param2,'\n']))
            one_day_ago = datetime.today() - timedelta(hours = hours) # get datetime of 1 day ago
            for name in self.mongo_db.collection_names():
                if name == 'system.indexes':
                    pass
                else:
                    hive = self.mongo_db[name]
                    for sample in hive.find({'time':{'$gt':one_day_ago}}):
                        try:
                            hive_id = str(sample['hive_id'])
                            val1 = str(sample[param1])
                            val2 = str(sample[param2])
                            time = sample['time'].strftime('%H:%M')
                            datafile.write(','.join([hive_id,time,val1,val2,'\n']))
                        except Exception:
                            pass
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
        self.send()
        state = self.classify(sample)
        mongo_id = self.store(sample)
    
    ## Render Index
    @cherrypy.expose
    def index(self):
        self.learner.query_range('int_t', 'ext_t', 24, 'temp')                    
        html = open('static/index.html').read()
        return html
    
# Main
if __name__ == '__main__':
    aggregator = HiveAggregator()
    cherrypy.server.socket_host = aggregator.CHERRYPY_ADDR
    cherrypy.server.socket_port = aggregator.CHERRYPY_PORT
    currdir = os.path.dirname(os.path.abspath(__file__))
    cherrypy.config.update({ "environment": "embedded" })
    conf = {
        '/': {'tools.staticdir.on':True, 'tools.staticdir.dir':os.path.join(currdir,'static')},
        '/data': {'tools.staticdir.on':True, 'tools.staticdir.dir':os.path.join(currdir,'data')}, # NEED the '/' before the folder name
    }
    cherrypy.quickstart(aggregator, '/', config=conf)
