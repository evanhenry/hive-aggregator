#!/usr/bin/env python
"""
HiveMind-Plus Aggregator
Developed by Trevor Stanhope

TODO:
- Use MongoDB instead of CouchDB
- Use Flask instead of CherryPy
- Validate received data
- use JSON settings file
"""

# Libraries
import zmq
import serial
import json
import time
import ast
import cherrypy
from pymongo import MongoClient
from cherrypy.process.plugins import Monitor
from cherrypy import tools
from firebase import firebase

# Constants
CONFIG_FILE = 'settings.json'

# HiveAggregator
class HiveAggregator:

    ## Initialize
    def __init__(self):

        ### Load Configuration
        print('[Loading Config File]')
        with open(CONFIG_FILE) as config:
            settings = ast.literal_eval(config.read())
            for key in settings:
                try:
                    getattr(self, key)
                except AttributeError as error:
                    print(key + ' : ' + str(settings[key]))
                    setattr(self, key, settings[key])

        ### ZMQ Server
        print('[Initializing ZMQ]')
        try:
            self.context = zmq.Context()
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.ZMQ_SERVER)
        except Exception as error:
            print('--> ERROR: ' + str(error))

        ### Firebase
        print('[Initializing Firebase]')  
        try:    
            self.firebase = firebase.FirebaseApplication(self.FIREBASE_URL, None)
        except Exception as error:
            print('--> ERROR: ' + str(error))

        ### Mongo
        print('[Initializing Mongo]')
        try:    
            self.mongo_client = MongoClient(self.MONGO_ADDR, self.MONGO_PORT)
            self.mongo_db = self.mongo_client[self.MONGO_DB]
        except Exception as error:
            print('--> ERROR: ' + str(error))        

        ### Schedulers
        print('[Enabling Update Monitor]')
        try:
            Monitor(cherrypy.engine, self.update, frequency=self.CHERRYPY_INTERVAL).subscribe()
        except Exception as error:
            print('--> ERROR: ' + str(error))

    ## Update
    def update(self):
        print('\n')

        ### Receive Data
        print('[Receiving Sample from Node]')
        try:
            packet = self.socket.recv()
            sample = json.loads(packet)
            print('--> SAMPLE: ' + str(sample))
        except Exception as error:
            print('--> ERROR: ' + str(error))

        ### Store to Mongo
        print('[Storing to Mongo]')
        try:
            collection = self.mongo_db[sample['node']]
            sample_id = collection.insert(sample)
            print('--> SAMPLE ID: ' + str(sample_id))
        except Exception as error:
            print('--> ERROR: ' + str(error))  

        ### Send Response
        print('[Sending Response to Node]')
        try:
            response = {'status':'okay'}
            dump = json.dumps(response)
            self.socket.send(dump)
        except Exception as error:
            print('--> ERROR: ' + str(error))
    
    ## Render Index
    @cherrypy.expose
    def index(self):
        return ''
    
# Main
if __name__ == '__main__':
    aggregator = HiveAggregator()
    cherrypy.server.socket_host = aggregator.CHERRYPY_ADDR
    cherrypy.server.socket_port = aggregator.CHERRYPY_PORT
    cherrypy.quickstart(aggregator, '/')
