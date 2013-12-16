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

# Configuration
try:
  CONFIG_FILE = sys.argv[1]
except Exception as error:
  CONFIG_FILE = 'config/aggregator.conf'
with open(CONFIG_FILE) as config:
  settings = ast.literal_eval(config.read())
  ZMQ_SERVER = settings['ZMQ_SERVER']
  COUCHDB_DATABASE = settings['COUCHDB_DATABASE']
  FIREBASE_PATH = settings['FIREBASE_PATH']
  FIREBASE_URL = settings['FIREBASE_URL']

# HiveAggregator
class HiveAggregator:

  ## Initialize
  def __init__(self):
    print('[Initializing ZMQ]')
    try:
      self.context = zmq.Context()
      self.socket = self.context.socket(zmq.REP)
      self.socket.bind(ZMQ_SERVER)
    except Exception as error:
      print('-->' + str(error))
    print('[Initializing CouchDB]')
    try:
      server = couchdb.Server()
      try:
        self.couch = server[COUCHDB_DATABASE]
      except Exception as error:
        self.couch = server.create(COUCHDB_DATABASE)
    except Exception as error:
      print('--> ' + str(error))
    print('[Initializing Firebase]')  
    try:    
      self.firebase = firebase.FirebaseApplication(FIREBASE_URL, None)
    except Exception as error:
      print('-->' + str(error))  

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
      result = self.firebase.post(FIREBASE_PATH + '/' + log['Node'], log)
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
 
# Main
if __name__ == '__main__':
  aggregator = HiveAggregator()
  while True:
    try:
      aggregator.update()
    except KeyboardInterrupt as error:
      break
