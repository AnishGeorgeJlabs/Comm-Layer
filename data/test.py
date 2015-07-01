import pika
import json
from configuration import config

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()
channel.queue_declare(queue=config['app_queue'])

def callback(ch, method, properties, body):
    payload = json.loads(body)
    print 'got data: '
    print str(payload)
    ch.basic_ack(delivery_tag=method.delivery_tag)      # Acknowledgment

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=config['app_queue'])
print "consuming"
channel.start_consuming()
