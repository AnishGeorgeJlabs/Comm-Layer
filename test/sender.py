import pika
import time
import json

# sample sender

messages = [{"msg": "message# "+str(n), "data": "yo"} for n in range(20)]

# --------- Now for the Rabbit --------- #
connection = pika.BlockingConnection(pika.ConnectionParameters(
  'localhost'))
channel = connection.channel()
channel.queue_declare(queue='testing')

for message in messages:
  time.sleep(1)
  channel.basic_publish(exchange='',
                        routing_key='testing',
                        body=json.dumps(message))

connection.close()
# -------------------------------------- #
