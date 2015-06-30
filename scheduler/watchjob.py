from pydispatch import dispatcher
from time import sleep
## main event scheduling system

from pydispatch import dispatcher
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from datetime import datetime, time, date
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
    dispatcher.connect(handler, signal=SIG, sender=dispatcher.Any)


class WatchJob(object):
    #global SIG
    #global dispatcher
    #global scheduler
    global _format

    def __init__(self, conf):
        self.conf = conf
        self.triggerObj = {
            'Campaign': self.conf['Campaign'], 
            'ID': self.conf['ID'],
            'Type': self.conf['Type']
        }
        if conf['Type'] == 'SMS':
            self.triggerObj.update({
                'Arabic': self.conf['Arabic'],
                'English': self.conf['English']
            })

        self.trigger = {}           # Actual trigger object for the apscheduler

        sDate = datetime.strptime(self.conf['Start Date'], '%m/%d/%Y')      # Short date given in Start Date

        fDate = datetime.combine(                                           # Full implied date
            sDate.date(),
            time(int(self.conf['Hour']), int(self.conf['Minute']))
        )

        ## setting up the trigger
        if self.conf['Repeat'] == 'Once':
            self.trigger = DateTrigger(fDate)
        elif self.conf['Repeat'] == 'Hourly':
            self.trigger = CronTrigger(hour='*/'+self.conf['Hour'], minute=self.conf['Minute'])
        elif self.conf['Repeat'] == 'Daily': # Daily
            self.trigger = CronTrigger(hour=self.conf['Hour'], minute=self.conf['Minute'])
        else:
            self.trigger = CronTrigger(second='*/5')
        
        ## setting the delay
        if self.conf['Repeat'] != 'Once':
            if sDate.date() > date.today():
                scheduler.add_job(self._schedule,'date', run_date=sDate)
            else:
                scheduler.add_job(self._schedule,'date', run_date=datetime.now())
        # TODO: have to set up crash recovery using last execute in action

    def _schedule(self):
        print "executing _schedule"
        self.job = scheduler.add_job(self._emit, self.trigger)

    def _emit(self):
        #print "emitting"
        if self.conf['Repeat'] == "Once":
            self.triggerObj.update({"Action": "Done"})
        else:
            self.triggerObj.update({"Action": datetime.now().strftime(_format)})
        
        dispatcher.send(signal=SIG, event=self.triggerObj, sender=self)

    def cancelJob(self):
        #print "Remove called"
        if hasattr(self, 'job'):
            self.job.remove()


## _-------------------- Testing -------------------------------_ ##

def logFunc(sender, event):
    print("Event: "+str(event)+"  at "+str(datetime.now()))

if __name__ == "__main__":
    register(logFunc)
    #dispatcher.connect(logFunc, signal=SIG, sender=dispatcher.Any)
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

    #del wj
    print "DONE"
