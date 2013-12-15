#!/usr/bin/env python
# Configure HiveMind on RaspberryPi

import string
import random
import shutil
import subprocess
import os

def key_generator(size=6, chars=string.ascii_uppercase + string.digits):
  return ''.join(random.choice(chars) for x in range(size))
  
def bash(script='ls'):
  print('BASH: ' + script)
  action = subprocess.Popen(script, shell=True, stdin=None, stdout=open(os.devnull,"wb"), stderr=None, executable="/bin/sh")
  action.wait()
