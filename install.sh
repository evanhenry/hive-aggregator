#!/bin/bash

# Get dependencies
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get install mongodb python-pip build-essential python-dev python-setuptools python-zmq python-sklearn -y
sudo pip install -r requirements.txt

# Install to path
read -p "Install to /usr/bin/ [y/n]?" ans
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

# Start on boot?
echo "Start on boot [y/n]?"
read ans
if [ $ans = y -o $ans = Y -o $ans = yes -o $ans = Yes -o $ans = YES ]
then
echo "Adding to rc.local ..."
fi
if [ $ans = n -o $ans = N -o $ans = no -o $ans = No -o $ans = NO ]
then
echo "Skipping ..."
fi
