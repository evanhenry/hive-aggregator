#!/usr/bin/env python
"""
HiveMind-Plus Aggregator
Developed by Trevor Stanhope
"""

# Libraries
import zmq
import serial
import json
import time
import couchdb
from firebase import firebase
from firebase import jsonutil
import sys
import ast
import cherrypy
from cherrypy.process.plugins import Monitor
from cherrypy import tools
import os

# HiveAggregator
class HiveAggregator:

  ## Initialize
  def __init__(self):
    
    print('[Reloading Config File]')
    try:
      self.CONFIG_FILE = sys.argv[1]
    except Exception as error:
      print('--> Defaulting to Custom Configuration: aggregator.conf')
      self.CONFIG_FILE = 'aggregator.conf'
    with open(self.CONFIG_FILE) as config:
      settings = ast.literal_eval(config.read())
      for key in settings:
        try:
          getattr(self, key)
        except AttributeError as error:
          setattr(self, key, settings[key])
          
    print('[Initializing ZMQ]')
    try:
      self.context = zmq.Context()
      self.socket = self.context.socket(zmq.REP)
      self.socket.bind(self.ZMQ_SERVER)
    except Exception as error:
      print('-->' + str(error))
      
    print('[Initializing CouchDB]')
    try:
      server = couchdb.Server()
      try:
        self.couch = server[self.COUCHDB_DATABASE]
      except Exception as error:
        self.couch = server.create(self.COUCHDB_DATABASE)
    except Exception as error:
      print('--> ' + str(error))
      
    print('[Initializing Firebase]')  
    try:    
      self.firebase = firebase.FirebaseApplication(self.FIREBASE_URL, None)
    except Exception as error:
      print('-->' + str(error))
      
    print('[Enabling Update Monitor]')
    try:
      Monitor(cherrypy.engine, self.update, frequency=self.UPDATE_INTERVAL).subscribe()
    except Exception as error:
      print('-->' + str(error))
    
  ## Update
  def update(self):
    
    print('\n')
    
    print('[Receiving Data from Node]')
    try:
      packet = self.socket.recv()
      log = json.loads(packet)
      for key in log:
        print('--> ' + str(key) + ': ' + str(log[key]))
    except Exception as error:
      print('--> ' + str(error))
      
    print('[Storing Data to Local Database]')
    try:
      result = self.couch.save(log)
      print('--> ' + '_id' + ' : ' + str(log['_id']))
      print('--> ' + '_rev' + ' : ' + str(log['_rev']))
      local = 'ok'
    except Exception as error:
      print('--> ' + str(error))
      local = 'bad'
      
    print('[Storing Data to Remote Database]')
    try:
      result = self.firebase.post('/' + self.FIREBASE_USER + '/' + self.AGGREGATOR_ID + '/' + log['node'], log)
      for key in result:
        print('--> ' + key + ' : ' + str(result[key]))
      remote = 'ok'
    except Exception as error:
      print('--> ' + str(error))
      remote = 'bad'
      
    print('[Sending Response to Node]')
    try:
      response = {'remote_status':remote, 'local_status':local}
      dump = json.dumps(response)
      self.socket.send(dump)
      for key in response:
        print('--> ' + key + ' : ' + str(response[key]))
    except Exception as error:
      print('--> ' + str(error))
    
  ## Render Index
  @cherrypy.expose
  def index(self):
    
    print('[Loading Index.html]')
    html = open('www/index.html').read()
    
    print('[Mapping Query]')
    current_time = time.time()
    GRAPH_INTERVAL = 100000
    keys = ['time', 'internal_C', 'external_C']
    values = dict(zip(keys,[[], [], []]))
    map_nodes = "function(doc) { if (doc.time >= " + str(current_time - self.GRAPH_INTERVAL) + ") emit(doc); }"
    matches = self.couch.query(map_nodes)
    for row in matches:
      for key in keys:
        try:
          values[key].append([row.key['time'], row.key[key]])
        except Exception:
          pass
        
    print('[Writing Time to TSV-File]')
    with open('www/time.tsv','w') as tsv_file:
      tsv_file.write('date' + '\t' + 'close' + '\n')
      for point in sorted(values['time']):
        x = time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime(point[0]))
        y = str(point[1])
        tsv_file.write(x + '\t' + y + '\n')
    return html
    
# Main
if __name__ == '__main__':
  aggregator = HiveAggregator()
  currdir = os.path.dirname(os.path.abspath(__file__))
  cherrypy.server.socket_host = aggregator.BIND_ADDR
  cherrypy.server.socket_port = aggregator.PORT
  conf = {
    '/': {'tools.staticdir.on':True, 'tools.staticdir.dir':os.path.join(currdir,'www')},
    'd3.v3.js': {'tools.staticfile.on': True, 'tools.staticfile.filename': os.path.join(currdir,'www')+'d3.v3.js'},
    'data.tsv': {'tools.staticfile.on': True, 'tools.staticfile.filename': os.path.join(currdir,'www')+'data.tsv'}
  }
  cherrypy.quickstart(aggregator, '/', config=conf)
