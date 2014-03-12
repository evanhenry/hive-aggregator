#!/usr/bin/env python
"""
HiveMind-Plus Aggregator
Developed by Trevor Stanhope

TODO:
- Use MongoDB instead of CouchDB for storing
- Implement graphing
- Validate received data
"""

# Libraries
import zmq
import json
import ast
import cherrypy
import os
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import json_util
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

        ### ZMQ
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

        ### CherryPy Monitors
        print('[Enabling Monitors]')
        try:
            Monitor(cherrypy.engine, self.listen, frequency=self.CHERRYPY_LISTEN_INTERVAL).subscribe()
            Monitor(cherrypy.engine, self.update, frequency=self.CHERRYPY_UPDATE_INTERVAL).subscribe()
        except Exception as error:
            print('--> ERROR: ' + str(error))
    
    ## Listen for Next Sample
    def listen(self):
        print('\n')

        ### Receive Sample
        print('[Receiving Sample from Hive]')
        try:
            packet = self.socket.recv()
            sample = json.loads(packet)
            sample['time'] = datetime.now() # add datetime object for when sample was received
            sample['date'] = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            print('--> SAMPLE: ' + str(sample))
        except Exception as error:
            print('--> ERROR: ' + str(error))

        ### Store to Firebase 
        print('[Storing to Firebase]')
        try:
            firebase_sample_id = self.firebase.post('/samples', sample) # add sample
            firebase_sample_key = self.firebase.post('/hives/' + sample['hive_id'] + '/samples', firebase_sample_id) # associate sample with hive
            print('--> FIREBASE_SAMPLE_ID: ' + str(firebase_sample_id))
            print('--> FIREBASE_SAMPLE_KEY: ' + str(firebase_sample_key))
        except Exception as error:
            print('--> ERROR: ' + str(error))

        ### Store to Mongo
        print('[Storing to Mongo]')
        try:
            hive = self.mongo_db[sample['hive_id']]
            mongo_id = hive.insert(sample)
            print('--> MONGO_SAMPLE_ID: ' + str(mongo_id))
        except Exception as error:
            print('--> ERROR: ' + str(error))

        ### Send Response
        print('[Sending Response to Hive]')
        try:
            response = {'status':'okay'}
            dump = json.dumps(response)
            self.socket.send(dump)
        except Exception as error:
            print('--> ERROR: ' + str(error))

    ## Update Hives
    def update(self):
        for estimator in ['temperature', 'humidity']:
            ### Query Last 24 Hours
            print('[Querying Last 24 Hours]')
            hives = {}
            sample_times = {} 
            headers = ['date']
            one_day_ago = datetime.today() - timedelta(hours = 24) # get datetime of 1 day ago
            print('--> RANGE: ' + str(one_day_ago))
            for collection in self.mongo_db.collection_names():
                if collection == 'system.indexes':
                    pass
                else:
                    hive_samples = {}
                    headers.append(collection) # build hive names
                    hive = self.mongo_db[collection] # get hive
                    for sample in hive.find({'time':{'$gt':one_day_ago}}):  
                        sample['unix_time'] = datetime.strftime(sample['time'], '%s')
                        sample['sample_time'] = datetime.strftime(sample['time'], '%Y-%m-%d-%H-%M')  
                        sample_times[sample['unix_time']] = sample['sample_time'] # remember sample time
                        hive_samples[sample['unix_time']] = sample # remember sample data
                    hives[collection] = hive_samples

            ### Flatten to List
            print('[Flattening JSON to List]')
            data_list = []
            for unix_time in sample_times: # all times sampled
                date_time = sample_times[unix_time] # i.e. sample['date']
                data_point = [date_time]
                for hive_id in hives:
                    try:
                        hive = hives[hive_id]
                        matching_sample = hive[unix_time]
                        if matching_sample['sample_time'] == sample_times[unix_time]:
                            estimator_value = str(matching_sample[estimator])
                            data_point.append(estimator_value)
                    except Exception as err:
                        pass # no time match
                if len(data_point) == len(hives) + 1:
                    data_list.append(data_point)
            
            ## Write to file
            print('[Writing to CSV]')
            with open('static/' + estimator + '.csv', 'w') as datafile:
                datafile.write(','.join(headers) + '\n')
                for data_point in sorted(data_list):
                    datafile.write(','.join(data_point) + '\n')
    
    ## Render Index
    @cherrypy.expose
    def index(self):
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
        'd3.v3.js': {'tools.staticfile.on': True, 'tools.staticfile.filename': os.path.join(currdir,'static') + 'd3.v3.js'},
        'index.css': {'tools.staticfile.on': True, 'tools.staticfile.filename': os.path.join(currdir,'static') + 'index.css'},
        '/favicon.ico': {'tools.staticfile.on': True, 'tools.staticfile.filename': os.path.join(currdir,'static') + 'favicon.ico'}
    }
    cherrypy.quickstart(aggregator, '/', config=conf)
