# HiveAggregator
Asynchronous server host for aggregating sample data for multiple beehives.

## Overview
### CherryPy
CherryPy acts as a hyper-minimal webserver for accessing aggregated data over
the local network. CherryPy's usefulness comes from its support for scheduled
tasks, known as a Monitor, which is used to automate collection of samples sent
to the aggregator.

### ZeroMQ
ZeroMQ is a highly efficient asynchronous socket server and is responsible for 
handling communication to the individual hives. All data exchange uses the JSON
convention.

### Firebase
Remote key-value store which allows realtime callbacks.

### MongoDB
Local key-value store which allows advanced queries on large datasets.

## Installation
To install all dependencies for the system, run the following:

    ./install.sh

## Running
To run the aggregator, from the git directory run the following:
    
    python HiveAggregator.py

By default, the HiveAggregator uses `settings.json`, alternatively you can specify the settings file to use:

    python HiveAggregator.py other_settings.json


    
    

