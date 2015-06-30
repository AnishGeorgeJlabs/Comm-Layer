# -*- encoding: utf-8 -*-

# Runner program to keep the application running in an event loop
# The application will return a bool telling whether it has worked or not

import time
from app import watcher

while True:
  while not watcher():
    pass
  time.sleep (30)       # This is the resolution we need
