#!/bin/bash
apt-get update -y
apt-get upgrade -y
apt-get install python-sklearn python-scipy python-numpy python-zmq -y
apt-get install mongodb -y
apt-get install python-pip -y
apt-get install python-cherrypy3 -y
pip install pymongo
