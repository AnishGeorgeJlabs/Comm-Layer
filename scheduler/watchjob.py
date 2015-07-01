# main event scheduling system

from pydispatch import dispatcher
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from datetime import datetime, time, date
from time import sleep

import logging
logging.basicConfig()

# Scheduler
scheduler = BackgroundScheduler()
scheduler.start()

# Dispatcher 
SIG = 'WatchJob'
_format = "%m/%d/%Y %H:%M:%S"
#

def register(handler):
    print "Registering handler for WatchJob"
    dispatcher.connect(handler, signal=SIG, sender=dispatcher.Any)


class WatchJob(object):
    # global SIG
    # global dispatcher
    # global scheduler
    global _format

    def __init__(self, conf):
        print "Created WatchJob"
        self.conf = conf
        # Short date
        self.sDate = datetime.strptime(self.conf['Start Date'], '%m/%d/%Y')

        # Full Date
        self.fDate = datetime.combine(
            self.sDate.date(),
            time(int(self.conf['Hour']), int(self.conf['Minute']))
        )
        self._set_trigger()
        self._set_delay()

    def _set_delay(self):
        # TODO: have to set up crash recovery using last execute in action
        if self.conf['Repeat'] != 'Once':
            if self.conf['Repeat'] != 'Test' and self.sDate.date() > date.today():
                scheduler.add_job(self._schedule, 'date', run_date=self.sDate)
            else:
                scheduler.add_job(self._schedule, 'date', run_date=datetime.now())

    def _set_trigger(self):
        self.triggerObj = {
            'Campaign': self.conf['Campaign'],
            'ID': self.conf['ID'],
            'Type': self.conf['Type']
        }
        if self.conf['Type'] == 'SMS':
            self.triggerObj.update({
                'Arabic': self.conf['Arabic'],
                'English': self.conf['English']
            })
        self.trigger = {}           # Actual trigger object for the apscheduler

        if self.conf['Repeat'] == 'Once':
            self.trigger = DateTrigger(self.fDate)
        elif self.conf['Repeat'] == 'Hourly':
            self.trigger = CronTrigger(hour='*/'+str(self.conf['Hour']), minute=str(self.conf['Minute']))
        elif self.conf['Repeat'] == 'Daily': # Daily
            self.trigger = CronTrigger(hour=self.conf['Hour'], minute=self.conf['Minute'])
        else:
            print "Correct repeat"
            self.trigger = CronTrigger(second='*/15')

        # DEBUG
        print "Created trigger", self.trigger
        print "curent", str(datetime.now().time())

    def _schedule(self):
        print "executing _schedule"
        self.job = scheduler.add_job(self._emit, self.trigger)

    def _emit(self):
        print "emitting ", self.conf['ID']
        if self.conf['Repeat'] == "Once":
            self.triggerObj.update({"Action": "Done"})
        else:
            self.triggerObj.update({"Action": datetime.now().strftime(_format)})
        
        dispatcher.send(signal=SIG, event=self.triggerObj, sender=self)

    def cancelJob(self):
        print "Remove called for campaign ", self.conf['Campaign']
        if hasattr(self, 'job'):
            self.job.remove()


## _-------------------- Testing -------------------------------_ ##

def logFunc(sender, event):
    print("Event: "+str(event)+"  at "+str(datetime.now()))

if __name__ == "__main__":
    register(logFunc)
    # dispatcher.connect(logFunc, signal=SIG, sender=dispatcher.Any)
    wj = WatchJob({
        'Campaign': 'TestCampaign',
        'Type': 'SMS',
        'Arabic': "Blah blah blah",
        'English': "Hello, howr you",
        'Repeat': 'Test',
        'Hour': '1',                        # TODO: these are str
        'Minute': '30',
        'Start Date': '6/30/2015',
        'ID': '1',
        'Action': ''
    })

    while True:
        sleep(1)
    #for i in range(10):
    #    sleep(1)

    wj.cancelJob()
    print "DONE"
