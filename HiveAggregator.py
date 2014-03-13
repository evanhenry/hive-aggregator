#!/usr/bin/env python
"""
HiveMind-Plus Aggregator
Developed by Trevor Stanhope

TODO:
- Scatter plot, not line graph
- convert to Flask?
- Validate received data
- POST to HiveServer
"""

# Libraries
import zmq
import json
import ast
import cherrypy
import os
import urllib2
import sys
from datetime import datetime, timedelta
from pymongo import MongoClient
from cherrypy.process.plugins import Monitor
from cherrypy import tools

# Constants
try:
    CONFIG_FILE = sys.argv[1]
except Exception as err:
    CONFIG_FILE = 'settings.json'

# HiveAggregator
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
                    print(key + ' : ' + str(settings[key]))
                    setattr(self, key, settings[key])

        ### ZMQ
        print('[Initializing ZMQ]')
        try:
            self.context = zmq.Context()
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.ZMQ_SERVER)
        except Exception as error:
            print('--> ERROR: ' + str(error))

        ### Mongo
        print('[Initializing Mongo]')
        try:    
            self.mongo_client = MongoClient(self.MONGO_ADDR, self.MONGO_PORT)
            self.mongo_db = self.mongo_client[self.MONGO_DB]
        except Exception as error:
            print('--> ERROR: ' + str(error))        

        ### CherryPy Monitors
        print('[Enabling Monitors]')
        try:
            Monitor(cherrypy.engine, self.listen, frequency=self.CHERRYPY_LISTEN_INTERVAL).subscribe()
        except Exception as error:
            print('--> ERROR: ' + str(error))
            
    ## Receive Sample
    def receive(self):
        print('[Receiving Sample from Hive]')
        try:
            packet = self.socket.recv()
            sample = json.loads(packet)
            return sample
        except Exception as error:
            print('--> ERROR: ' + str(error))
            
    ## Check Sample
    def check(self, sample):
        sample['aggregator_id'] = self.AGGREGATOR_ID
        return sample
    
    ## Post to Server
    def post(self, sample):
        print('[Posting Sample to Server]')
        try:
            data = json.dumps(sample)
            req = urllib2.Request(self.POST_URL)
            req.add_header('Content-Type','application/json')
            response = urllib2.urlopen(req, data)
            return response
        except Exception as error:
            print('--> ERROR: ' + str(error))
            
    ## Store to Mongo
    def store(self, sample):
        print('[Storing to Mongo]')
        try:
            sample['time'] = datetime.now()
            hive = self.mongo_db[sample['hive_id']]
            mongo_id = hive.insert(sample)
            return mongo_id
        except Exception as error:
            print('--> ERROR: ' + str(error))
    
    ### Send Response
    def send(self):
        print('[Sending Response to Hive]')
        try:
            response = {'status':'okay'}
            dump = json.dumps(response)
            self.socket.send(dump)
        except Exception as error:
            print('--> ERROR: ' + str(error))  
               
    ## Listen for Next Sample
    def listen(self):
        print('\n')
        sample = self.receive() 
        sample = self.check(sample)
        response = self.post(sample)
        mongo_id = self.store(sample)
        self.send()
        
    ## Query Last 24 hours to CSV
    def query_today(self):
        print('[Querying Last 24 Hours]')
        with open('data/data.csv', 'w') as datafile:
            datafile.write('hive_id,time,temperature,humidity\n')
            one_day_ago = datetime.today() - timedelta(hours = 24) # get datetime of 1 day ago
            for name in self.mongo_db.collection_names():
                if name == 'system.indexes':
                    pass
                else:
                    hive = self.mongo_db[name]
                    for sample in hive.find({'time':{'$gt':one_day_ago}}):
                        hive_id = str(sample['hive_id'])
                        temperature = str(sample['temperature'])
                        humidity = str(sample['humidity'])
                        time = sample['time'].strftime('%H:%M')
                        datafile.write(','.join([hive_id,time,temperature,humidity,'\n']))
    
    ## Render Index
    @cherrypy.expose
    def index(self):
        self.query_today()                    
        html = open('static/index.html').read()
        return html
    
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
