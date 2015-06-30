from config import get_worksheet, config
import pika
import json

worksheet = get_worksheet().get_all_records()
print "Got worksheet", str(worksheet)

# ---------- Rabbit ----------- #
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue=config['queue'])

channel.basic_publish(
    exchange='',
    routing_key=config['queue'],
    body=json.dumps(worksheet)
)
print "publishing"
## Send message on rabbit Q
#worksheet.update_acell('F2', 'Done')