#!/bin/bash

# Get dependencies
apt-get update -y
apt-get upgrade -y
apt-get install python-sklearn python-scipy python-numpy python-zmq -y
apt-get install mongodb -y
apt-get install python-pip -y
apt-get install python-cherrypy3 -y
pip install pymongo

# Install to path
cp -R ../hive-aggregator /usr/share
chmod +x /usr/share/hive-aggregator/configs/hive-aggregator
sudo ln -s /usr/share/hive-aggregator/configs/hive-aggregator /usr/bin/hive-aggregator
