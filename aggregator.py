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
    Monitor(cherrypy.engine, self.update, frequency=1).subscribe()
    self.reload_config()
    self.zmq_host()
    self.couchdb_connect()
    self.firebase_connect()
    
  ## Update
  def update(self):
    print('\n')
    print('[Receiving Data from Node]')
    try:
      log = json.loads(self.socket.recv())
      for key in log:
        print('--> ' + str(key) + ': ' + str(log[key]))
    except Exception as error:
      print('--> ' + str(error))
    print('[Storing Data to Local Database]')
    try:
      result = self.couch.save(log)
      print('--> ' + str(result))
    except Exception as error:
      print('--> ' + str(error))
    print('[Storing Data to Remote Database]')
    try:
      result = self.firebase.post(self.FIREBASE_PATH + '/' + log['Node'], log)
      print('--> ' + str(result))
    except Exception as error:
      print('--> ' + str(error))
    print('[Sending Response to Node]')
    try:
      response = json.dumps({'status':'ok'})
      result = self.socket.send(response)
      print('--> ' + str(response))
    except Exception as error:
      print('--> ' + str(error))

  ## Load Config File
  def reload_config(self):
    print('[Reloading Config File]')
    try:
      self.CONFIG_FILE = sys.argv[1]
    except Exception as error:
      print('--> ' + str(error))
      self.CONFIG_FILE = 'aggregator.conf'
    print('--> ' + self.CONFIG_FILE)
    with open(self.CONFIG_FILE) as config:
      settings = ast.literal_eval(config.read())
      for key in settings:
        try:
          getattr(self, key)
        except AttributeError as error:
          setattr(self, key, settings[key])

  ## Host ZMQ Server
  def zmq_host(self):
    print('[Initializing ZMQ]')
    try:
      self.context = zmq.Context()
      self.socket = self.context.socket(zmq.REP)
      self.socket.bind(self.ZMQ_SERVER)
    except Exception as error:
      print('-->' + str(error))

  ## Connect to couchdb
  def couchdb_connect(self):
    print('[Initializing CouchDB]')
    try:
      server = couchdb.Server()
      try:
        self.couch = server[self.COUCHDB_DATABASE]
      except Exception as error:
        self.couch = server.create(self.COUCHDB_DATABASE)
    except Exception as error:
      print('--> ' + str(error))

  ## Connect to Firebase IO
  def firebase_connect(self):
    print('[Initializing Firebase]')  
    try:    
      self.firebase = firebase.FirebaseApplication(self.FIREBASE_URL, None)
    except Exception as error:
      print('-->' + str(error))

  ## Disconnect from ZMQ
  def zmq_disconnect(self):
    print('[Halting ZMQ]')
    try:
      self.socket.close()
    except Exception as error:
      print('--> ' + str(error))

  ## Render Index
  @cherrypy.expose
  def index(self):
    with open('www/index.html') as html:
      return html.read()
    
# Main
if __name__ == '__main__':
  aggregator = HiveAggregator()
  currdir = os.path.dirname(os.path.abspath(__file__))
  cherrypy.server.socket_host = '0.0.0.0'
  cherrypy.server.socket_port = 8080
  conf = {'/www': {'tools.staticdir.on':True, 'tools.staticdir.dir':os.path.join(currdir,'www')}}
  cherrypy.quickstart(aggregator, '/', config=conf)
