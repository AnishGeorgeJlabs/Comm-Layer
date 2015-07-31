import pika
from data.sheet import updateAction
import json
from data.configuration import config


connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()
channel.queue_declare(queue=config['sms_queue'])
logChannel = connection.channel()
logChannel.exchange_declare(exchange='wadi:logs', type='fanout')

def log(job_id):
    def lg(status):
        msg = json.dumps({"sender": "sms_sender", "log": {'job_id': job_id, 'status': status}})
        logChannel.basic_publish(exchange='wadi:logs', routing_key='', body=msg)
    return lg

def callback(ch, method, prop, body):
    data = json.loads(body)
    payload = data[1]
    print "Got sms: "+str(payload)
    if 'sentinel' in payload:
        d = payload['sentinel']
        updateAction(d['ID'], d['Action'])
        print "Finished with job: ", d['ID']
    else:
        log(data[0])("success")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue=config['sms_queue'])
channel.start_consuming()
