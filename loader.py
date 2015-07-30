from data.sheet import get_scheduler_sheet
from data.configuration import config
import pika
import json

worksheet = get_scheduler_sheet().get_all_records()
#print "Got worksheet", str(worksheet)

def checkRow(row):
    mandatory = ['Repeat', 'Type', 'Campaign', 'Start Date', 'Hour', 'Minute', 'English', 'Arabic']
    for key in mandatory:
        if row[key] == '':
            print "invalid: "+key
            return False

    return True

check = all(
    checkRow(row) for row in worksheet
)

# ---------- Rabbit ----------- #
if check:
    print "publishing records: ", len(worksheet)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=config['app_queue'])

    channel.basic_publish(
        exchange='',
        routing_key=config['app_queue'],
        body=json.dumps(worksheet)
    )
else:
    print "invalid data, exiting"
## Send message on rabbit Q
#worksheet.update_acell('F2', 'Done')
