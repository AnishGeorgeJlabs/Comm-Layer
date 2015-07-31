"""
Logs system info
"""
import pika
from datetime import datetime
import json

runDict = {}

def get_time():
    return datetime.now().strftime("%d/%m/%Y %H:%M")

def process_job(job_id, payload_size):
    runDict[job_id] = {
        "process_start": get_time(),
        "size": payload_size,
        "counts": {
            "success": 0,
            "failure": 0,
            "error": 0
        }
    }

def log(job_id, data):
    data['process_end'] = get_time()
    filename = './logs/log_'+str(job_id)+".txt"
    with open(filename, 'a') as lFile:
        lFile.writelines([
            "\n",
            "\nProcess started on "+data['process_start'],
            "\nTotal # of messages: "+str(data['size']),
            "\n        Sent: "+str(data['counts']['success']),
            "\n      failed: "+str(data['counts']['failure']),
            "\n  exceptions: "+str(data['counts']['error']),
            "\nProcess ended on "+data['process_end']
        ])
    return None

def critical_log(data):
    with open('./logs/critical.txt') as lFile:
        lFile.write(json.dumps(data))

def inc(job_id, status):
    data = runDict[job_id]
    data['counts'][status] += 1
    if sum(data['counts'].values()) == data['size']:
        runDict.pop(job_id)
        log(job_id, data)

def callback(ch, method, properties, body):
    print "calback called"
    try:
        data = json.loads(body)
        sender = data['sender']
        log = data['log']
        print "LOG: ", data

        if sender == 'sms_sender':
            inc(log['job_id'], log['status'])
        elif sender == 'handler':
            process_job(log['job_id'], log['payload_size'])
        elif sender == "critical":
            critical_log(log)
    except Exception, e:
        raise
        print "ERROR: LOGGER: ", e

if __name__ == "__main__":
    print "Starting logger"
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

    channel = connection.channel()

    channel.exchange_declare(exchange='wadi:logs', type='fanout')

    result = channel.queue_declare(exclusive=True)

    queue_name = result.method.queue

    channel.queue_bind(exchange='wadi:logs',
                       queue=queue_name)

    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=True)
    channel.start_consuming()
    print "Logger started"

