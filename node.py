#!/usr/bin/env python
"""
HiveMind-Plus Node
Developed by Trevor Stanhope
"""

# Libraries
import zmq
import serial
from serial import SerialException
import ast
import json
import time
import os
import sys

# Configuration
try:
  CONFIG_FILE = sys.argv[1]
except Exception as error:
  CONFIG_FILE = 'config/node.conf'
with open(CONFIG_FILE) as config:
  settings = ast.literal_eval(config.read())
  ZMQ_SERVER = settings['ZMQ_SERVER']
  ZMQ_TIMEOUT = settings['ZMQ_TIMEOUT']
  ARDUINO_DEV = settings['ARDUINO_DEV']
  ARDUINO_BAUD = settings['ARDUINO_BAUD']
  ARDUINO_INTERVAL = settings['ARDUINO_INTERVAL']
  NODE_ID = settings['NODE_ID']

# Node
class HiveNode:

  ## Initialize
  def __init__(self):
    print('HiveMind')
    self.context = zmq.Context()
    self.zmq_connect()
    self.arduino_connect()

  ## Connect to Aggregator
  def zmq_connect(self):
    print('[Initializing ZMQ]')
    try:
      self.socket = self.context.socket(zmq.REQ)
      self.socket.connect(ZMQ_SERVER)
      self.poller = zmq.Poller()
      self.poller.register(self.socket, zmq.POLLIN)
    except Exception as error:
      print('--> ' + str(error))

  # Connect to Arduino
  def arduino_connect(self):
    print('[Initializing Arduino]')
    try:
      self.arduino = serial.Serial(ARDUINO_DEV, ARDUINO_BAUD, timeout=ZMQ_TIMEOUT)
    except Exception as error:
      print('--> ' + str(error))
      
  ## Update to Aggregator
  def update(self):
    print('\n')
    log = {'Time':time.time(), 'Node':NODE_ID}
    print('[Reading Arduino Sensors]')
    try:
      log.update(self.arduino.read())
      print('-->' + str(log))
    except Exception as error:
      print('--> ' + str(error))
    print('[Sending Message to Aggregator]')
    try:
      result = self.socket.send(json.dumps(log))
      print('--> ' + str(result))
    except zmq.core.error.ZMQError as error:
      print('--> ' + str(error))
      self.zmq_disconnect()
      self.zmq_connect()
    print('[Receiving Response from Aggregator]')
    try:
      socks = dict(self.poller.poll(ZMQ_TIMEOUT))
      if socks:
        if socks.get(self.socket) == zmq.POLLIN:
          response = self.socket.recv(zmq.NOBLOCK)
          print('--> ' + str(response))
        else:
          print('--> ' + 'Timeout: ' + ZMQ_TIMEOUT + 'ms')
      else:
         print('--> ' + 'Aggregator not found')
    except Exception as error:
      print('--> ' + str(error))
  
  ## Disconnect from Aggregator
  def zmq_disconnect(self):
    print('[Disconnecting]')
    try:
      self.socket.close()
    except Exception as error:
      print('--> ' + str(error))
      
# Main
if __name__ == '__main__':
  node = HiveNode()
  while True:
    try:
      node.update()
    except KeyboardInterrupt as error:
      break
