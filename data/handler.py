# Main application instance, which will be the producer for the rabbitMQ
from data.data_loader import load_data
import json
import pika
from configuration import config

def watcher(sender, event):
    print "Debug: Watcher started ..."
    try:
        ## Full code for main parent
        res, payloadArr = load_data(event)
        if not res:
          raise Exception

        # --------- Now for the Rabbit --------- #
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue=config['sms_queue'])

        print "array of payloads ", payloadArr
        for payload in payloadArr:
          channel.basic_publish(exchange='',
                                routing_key=config['sms_queue'],
                                body=json.dumps(payload))

        connection.close()
        # -------------------------------------- #
        return True
    except Exception:
        raise
        return False
    finally:
        pass     # cleanup
