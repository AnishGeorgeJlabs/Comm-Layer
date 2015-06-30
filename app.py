# Main application instance, which will be the producer for the rabbitMQ
from dataLoader import load_data
import json
import pika
from config import config

def watcher():
    print "Debug: Watcher started ..."
    try:
        ## Full code for main parent
        res, payloadArr = load_data()
        if not res:
          raise Exception

        # --------- Now for the Rabbit --------- #
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue=config['queue'])

        print "array of payloads ", payloadArr
        for payload in payloadArr:
          channel.basic_publish(exchange='',
                                routing_key=config['queue'],
                                body=json.dumps(payload))

        connection.close()
        # -------------------------------------- #
        return True
    except Exception:
        return False
    finally:
        pass     # cleanup
