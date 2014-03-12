#!/bin/bash
apt-get update -y
apt-get upgrade -y
apt-get install pip -y
pip install -r requirements.txt
