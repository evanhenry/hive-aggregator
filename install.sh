#!/bin/bash
apt-get update -y
apt-get upgrade -y
apt-get install python-sklearn python-scipy python-numpy python-zmq -y
apt-get install mongodb -y
apt-get install python-pip -y
apt-get install python-cherrypy3 -y
pip install pymongo

cp -R ../hive-aggregator /usr/share
sudo ln -s /usr/share/hive-aggregator/bin/hive-aggregator /usr/bin/hive-aggregregator
