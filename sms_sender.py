# Consumer for the rabbitMq. it will send the sms
import urllib, urllib2
import pika
import json
from data.configuration import config
from data.sheet import updateAction

def sendSms(payload, log):
    try:
        if 'sentinel' in payload:
            d = payload['sentinel']
            updateAction(d['ID'], d['Action'])
            print "Finished with job: ", d['ID']
        else:
            values = {'user' : 'WADI.COM', 'passwd' : 'SMS#WADI', 'DR':'Y','Sid':'Jlabs'}
            values.update(payload)
            url = config['sms_post_url']
            data = urllib.urlencode(values)
            request = urllib2.Request(url, data)
            print "Got payload: ", payload
            wp = urllib2.urlopen(request)
            if 'OK' in wp.read():
                log('success')
            else:
                log('failure')
        return True
    except AssertionError:
        log('error')
        return False
    except Exception, e:
        log('error')
        print " >>> Error: "+str(e)
        return False


# ------------ consumer ------------- #
if __name__ == "__main__":
    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue=config['sms_queue'])
    channel.exchange_declare(exchange='wadi:logs', type='fanout')

    def log(job_id):
        def lg(status):
            msg = json.dumps({"sender": "sms_sender", "log": {'job_id': job_id, 'status': status}})
            channel.basic_publish(exchange='wadi:logs', routing_key='', body=msg)
        return lg

    def callback(ch, method, properties, body):
        data = json.loads(body)
        payload = data[1]
        sendSms(payload, log(data[0]))
        ch.basic_ack(delivery_tag = method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback,
                          queue=config['sms_queue'])

    channel.start_consuming()
    print "Ready"
