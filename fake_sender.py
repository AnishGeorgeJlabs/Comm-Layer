import pika
from data.sheet import updateAction
import json
from data.configuration import config


connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()
channel.queue_declare(queue=config['sms_queue'])

def callback(ch, method, prop, body):
    payload = json.loads(body)
    print "Got sms: "+str(payload)
    if 'sentinel' in payload:
        d = payload['sentinel']
        updateAction(d['ID'], d['Action'])
        print "Finished with job: ", d['ID']
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue=config['sms_queue'])
channel.start_consuming()
