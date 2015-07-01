from data.sheet import get_scheduler_sheet
from data.configuration import config
import pika
import json

worksheet = get_scheduler_sheet().get_all_records()
print "Got worksheet", str(worksheet)

# ---------- Rabbit ----------- #
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue=config['app_queue'])

channel.basic_publish(
    exchange='',
    routing_key=config['app_queue'],
    body=json.dumps(worksheet)
)
print "publishing"
## Send message on rabbit Q
#worksheet.update_acell('F2', 'Done')