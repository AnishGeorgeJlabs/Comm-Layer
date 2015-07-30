# Main application instance, which will be the producer for the rabbitMQ
from data_loader import load_data
import json
import pika
from configuration import config

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost'))

def watcher(event):
    print "Debug: Watcher started ..."
    try:
        ## Full code for main parent
        res, payloadArr = load_data(event)
        if not res:
          raise Exception

        # --------- Now for the Rabbit --------- #
        channel = connection.channel()
        channel.queue_declare(queue=config['sms_queue'])

        print "Payload array size", len(payloadArr)
        for payload in payloadArr:
          channel.basic_publish(exchange='',
                                routing_key=config['sms_queue'],
                                body=json.dumps(payload))

        #connection.close()
        # -------------------------------------- #
        return True
    except Exception:
        return False
    finally:
        pass     # cleanup

if __name__ == '__main__':
    print "Getting ready"
    pingChannel = connection.channel()
    pingChannel.queue_declare(queue=config['event_queue'])

    def callback(ch, method, properties, body):
        print "executing ping callback"
        event = json.loads(body)
        watcher(event)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    pingChannel.basic_qos(prefetch_count=1)
    pingChannel.basic_consume(callback, queue=config['event_queue'])
    pingChannel.start_consuming()
