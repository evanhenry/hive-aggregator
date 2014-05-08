#!/bin/bash
apt-get update -y
apt-get upgrade -y
apt-get install python-sklearn python-scipy python-numpy python-zmq
apt-get install mongodb
apt-get install pip -y

pip install pymongo
