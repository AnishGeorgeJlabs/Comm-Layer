# -*- encoding: utf-8 -*-

# Runner program to keep the application running in an event loop
# The application will return a bool telling whether it has worked or not
"""
import time
from app import watcher

while True:
  while not watcher():
    pass
  time.sleep (30)       # This is the resolution we need
"""

from scheduler import jobscheduler
from app import watcher
from conf_loader.config import config
import pika
import json

jobscheduler.register(watcher)

# ------- Rabbit MQ --------- #
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()
channel.queue_declare(queue=config['queue'])

def callback(ch, method, properties, body):
    payload = json.loads(body)
    jobscheduler.configure_jobs(payload)
    ch.basic_ack(delivery_tag=method.delivery_tag)      # Acknowledgment

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=config['queue'])
print "Ready"
channel.start_consuming()

while True:
    pass
