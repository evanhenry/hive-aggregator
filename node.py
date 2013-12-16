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
import pyaudio
from ctypes import *

# Error Handling
ERROR_HANDLER_FUNC = CFUNCTYPE(None, c_char_p, c_int, c_char_p, c_int, c_char_p)
def py_error_handler(filename, line, function, err, fmt):
  pass
C_ERROR_HANDLER = ERROR_HANDLER_FUNC(py_error_handler)

# Node
class HiveNode:

  ## Initialize
  def __init__(self):
    print('HiveMind')
    self.reload_config()
    self.zmq_connect()
    self.arduino_connect()

  ### MAIN LOOP - Update to Aggregator ###
  def update(self):
    print('\n')
    log = {'Time':time.time(), 'Node':self.NODE_ID}
    
    print('[Reading Arduino Sensors]')
    try:
      log.update(self.arduino.read())
      print('-->' + str(log))
    except Exception as error:
      print('--> ' + str(error))
      
    ## print('[Listening to Microphone]')
    ## try:
    ##   log.update(self.use_microphone())
    ##   print('-->' + str(log))
    ## except Exception as error:
    ##   print('--> ' + str(error))
    
    print('[Sending Message to Aggregator]')
    try:
      result = self.socket.send(json.dumps(log))
      print('--> ' + str(result))
    except zmq.core.error.ZMQError as error:
      print('--> ' + str(error))
      self.zmq_refresh_socket()
      
    print('[Receiving Response from Aggregator]')
    try:
      socks = dict(self.poller.poll(self.ZMQ_TIMEOUT))
      if socks:
        if socks.get(self.socket) == zmq.POLLIN:
          response = self.socket.recv(zmq.NOBLOCK)
          print('--> ' + str(response))
        else:
          print('--> ' + 'Timeout: ' + self.ZMQ_TIMEOUT + 'ms')
      else:
         print('--> ' + 'Aggregator not found')
    except Exception as error:
      print('--> ' + str(error))
      self.zmq_refresh_poll()
      
  ## Load Config File
  def reload_config(self):
    print('[Reloading Config File]')
    try:
      self.CONFIG_FILE = sys.argv[1]
    except Exception as error:
      print('--> ' + str(error))
      self.CONFIG_FILE = 'node.conf'
    print('--> ' + self.CONFIG_FILE)
    with open(self.CONFIG_FILE) as config:
      settings = ast.literal_eval(config.read())
      for key in settings:
        try:
          getattr(self, key)
        except AttributeError as error:
          setattr(self, key, settings[key])
          
  ## Connect to Aggregator
  def zmq_connect(self):
    print('[Initializing ZMQ]')
    try:
      self.context = zmq.Context()
      self.socket = self.context.socket(zmq.REQ)
      self.socket.connect(self.ZMQ_SERVER)
      self.poller = zmq.Poller()
      self.poller.register(self.socket, zmq.POLLIN)
    except Exception as error:
      print('--> ' + str(error))
      
  ## Disconnect from Aggregator
  def zmq_disconnect(self):
    print('[Disconnecting from Aggregator]')
    try:
      self.socket.close()
      self.poller.close()
    except Exception as error:
      print('--> ' + str(error))
      
  ## Refresh ZMQ Socket
  def zmq_refresh_socket(self):
    print('[Refreshing Socket to Aggregator]')
    try:
      self.socket.close()
      self.socket = self.context.socket(zmq.REQ)
      self.socket.connect(self.ZMQ_SERVER)
    except Exception as error:
      print('--> ' + str(error))

  ## Refresh ZMQ Poller
  def zmq_refresh_poll(self):
    print('[Refreshing Aggregator Polling]')
    try:
      self.poller = zmq.Poller()
      self.poller.register(self.socket, zmq.POLLIN)
    except Exception as error:
      print('--> ' + str(error))
    
  ## Connect to Arduino
  def arduino_connect(self):
    print('[Initializing Arduino]')
    try:
      self.arduino = serial.Serial(self.ARDUINO_DEV, self.ARDUINO_BAUD, timeout=self.ARDUINO_INTERVAL)
    except Exception as error:
      print('--> ' + str(error))

  ## Disconnect Arduino
  def arduino_disconnect(self):
    print('Disabling Arduino')
    try:
      self.arduino.close()
    except Exception as error:
      print('--> ' + str(error))

  ## Use microphone
  def use_microphone(self):
    print('[Capturing Audio]')
    try:
      asound = cdll.LoadLibrary('libasound.so')
      asound.snd_lib_error_set_handler(C_ERROR_HANDLER) # Set error handler
      mic = pyaudio.PyAudio()
      stream = mic.open(format=FORMAT,channels=CHANNELS,rate=RATE,input=True,frames_per_buffer=CHUNK)
      data = stream.read(CHUNK)
      wave_array = np.fromstring(data, dtype='int16')
      wave_fft = np.fft.fft(wave_array)
      wave_freqs = np.fft.fftfreq(len(wave_fft))
      frequency = RATE*abs(wave_freqs[np.argmax(np.abs(wave_fft)**2)])
      amplitude = np.sqrt(np.mean(np.abs(wave_fft)**2))
      decibels =  10*np.log10(amplitude)
      stream.stop_stream()
      return {'Decibels': decibels, 'Frequency': frequency}
    except Exception as error:
      print('--> ' + str(error))
      return None
    
# Main
if __name__ == '__main__':
  node = HiveNode()
  while True:
    try:
      node.update()
    except KeyboardInterrupt as error:
      node.zmq_disconnect()
      node.arduino_disconnect()
      break
