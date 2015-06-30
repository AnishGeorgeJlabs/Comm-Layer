import pika
import json
import time

print "Waiting...."
connection = pika.BlockingConnection(pika.ConnectionParameters(
  host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='testing')
def callback(ch, method, properties, body):
  time.sleep(2)
  payload = json.loads(body)
  print payload['msg'] + " " + payload['data']
  ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='testing')

channel.start_consuming()
