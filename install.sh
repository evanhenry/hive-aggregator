#!/bin/bash

# Get dependencies
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get install python-sklearn python-scipy python-numpy python-zmq -y
sudo apt-get install mongodb -y
sudo apt-get install python-pip -y
sudo apt-get install python-cherrypy3 -y
sudo pip install pymongo

# Install to path
echo "Install to /usr/bin/ [y/n]?"
read ans
if [ $ans = y -o $ans = Y -o $ans = yes -o $ans = Yes -o $ans = YES ]
then
echo "installing to /usr/bin ..."
sudo cp -R ../hive-aggregator /usr/share
sudo chmod +x /usr/share/hive-aggregator/configs/hive-aggregator
sudo ln -s /usr/share/hive-aggregator/configs/hive-aggregator /usr/bin/hive-aggregator
fi
if [ $ans = n -o $ans = N -o $ans = no -o $ans = No -o $ans = NO ]
then
echo "Skipping ..."
fi
