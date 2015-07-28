# Consumer for the rabbitMq. it will send the sms
import urllib, urllib2
import pika
import json
from data.configuration import config

def sendSms(payload):
    try:
        assert {'message', 'cell_phone'} & payload.viewkeys(), "Invalid Data Packet"
        values = {'user' : 'wadiops', 'passwd' : 'w@d1Rock$', 'DR':'Y','Sid':'Jlabs'}
        values.update(payload)
        url = config['sms_post_url']
        data = urllib.urlencode(values)
        request = urllib2.Request(url, data)
        print "Got payload: ", payload
        wp = urllib2.urlopen(request)
        print wp.read()
        return True
    except AssertionError:
        return True
    except Exception, e:
        print " >>> Error: "+str(e)
        return False


# ------------ consumer ------------- #
if __name__ == "__main__":
    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=config['sms_queue'])

    def callback(ch, method, properties, body):
        payload = json.loads(body)
        sendSms(payload)
        #i = 1
        #while not sendSms(payload) and i > 0:       # 2 tries
        #    i -= 1
        ch.basic_ack(delivery_tag = method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback,
                          queue=config['sms_queue'])

    channel.start_consuming()
    print "Ready"
