# Main application instance, which will be the producer for the rabbitMQ
from data_loader import load_data
from data.sheet import updateAction
import json
import pika
from configuration import config, createLogger
from external_setup import work_external_data

cLogger = createLogger("handler")

def watcher(event, send_sms, log):
    print "Debug: Watcher started ..."
    try:
        ## Full code for main parent
        if event['type'] == 'send_sms':
            data = event['data']
            res, payloadArr = load_data(data)
            if not res:
              raise Exception

            log(str(data['ID']), len(payloadArr) - 1)              # The last one is the sentinel
            print "Payload array size", len(payloadArr) - 1

            for payload in payloadArr:
                send_sms(payload)
        elif event['type'] == 'external_setup':
            data = event['data']
            result = work_external_data(data)
            print "DEBUG, external event, laoded = "+str(result)
        elif event['type'] == 'update_action':
            data = event['data']
            updateAction(data['ID'], event['Action'], event.get('External Job'))

        return True
    except Exception:
        cLogger.exception("handler recovered over the following")
        return False
    finally:
        pass     # cleanup

if __name__ == '__main__':
    print "Getting ready"
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=config['event_queue'])
    channel.exchange_declare(exchange='wadi:logs', type='fanout')

    def send_sms(payload):
        channel.basic_publish(exchange='',
                              routing_key=config['sms_queue'],
                              body=json.dumps(payload))

    def log(job_id, size):
        msg = {'sender': 'handler', 'log': {
            'job_id': job_id, 'payload_size': size
        }}
        channel.basic_publish(exchange='wadi:logs', routing_key='', body=json.dumps(msg))

    def callback(ch, method, properties, body):
        print "executing ping callback"
        event = json.loads(body)
        watcher(event, send_sms, log)
        ch.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue=config['event_queue'])
    channel.start_consuming()
