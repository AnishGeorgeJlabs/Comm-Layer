# main event scheduling system

# Conf['id'] will always be an int here

from pydispatch import dispatcher
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from datetime import datetime, time, date, timedelta, tzinfo
from time import sleep
from tzlocal import get_localzone
import pytz

import logging

logging.basicConfig()

# Scheduler
scheduler = BackgroundScheduler()
scheduler.start()

# Dispatcher 
SIG = 'WatchJob'
_format = "%m/%d/%Y %H:%M:%S"

# Some constants
riyadhZone = pytz.timezone('Asia/Riyadh')
localZone = get_localzone()

_repeated_types = ['Hourly', 'Daily', 'Fortnightly', 'Monthly']
_done_action_types = ['Once', 'Immediately']


def _correct_in_time(dt):
    return riyadhZone.localize(dt).astimezone(localZone)


def _correct_out_time(dt):
    return localZone.localize(dt).astimezone(riyadhZone)


def register(handler):
    print "Registering handler for WatchJob"
    dispatcher.connect(handler, signal=SIG, sender=dispatcher.Any)


class WatchJob(object):
    global _format
    global _repeated_types
    global _done_action_types

    def __init__(self, conf):
        self.conf = conf
        self._create_event_obj()

        if self.conf['campaign'].lower() in "external":
            event = {
                'type': 'external_setup',
                'data': self.conf
            }
            print "watch job got External"
            dispatcher.send(signal=SIG, event=event, sender=self)
            if self.conf['repeat'] == 'No Send':
                return      # No further processing

        if self.conf['repeat'] == 'Immediately':
            if self.conf['campaign'].lower() in 'external':  # TODO, its an ugly hack, needs to be changed
                scheduler.add_job(self._emit, 'date', run_date=(datetime.now() + timedelta(minutes=1)))
            self._emit()
            return

        try:
            self.fDate = _correct_in_time(
                datetime.combine(
                    datetime.strptime(self.conf['start_date'], '%m/%d/%Y'),
                    time(self.conf['hour'], self.conf['minute'])
                )
            )
        except:
            self.fDate = localZone.localize(datetime.now())

        # Short circuit if we missed our time
        if self.conf['repeat'] == 'Once' and self.fDate <= localZone.localize(datetime.now()):
            self.valid = False
            print "Missed one: ", self.fDate.strftime("%d/%m/%Y, %H:%M:%S")
        else:
            self.valid = True

            self.sDate = self.fDate.replace(hour=0, minute=0, second=0)
            print "time: ", self.fDate.time()

            self._set_trigger()
            if not self._crash_recovery():
                self._set_delay()

    def _create_event_obj(self):
        self.eventObj = {
            'campaign': self.conf['campaign'],
            'id': self.conf['id'],
            'arabic': self.conf['arabic'],
            'english': self.conf['english']
        }
        oid = self.conf.get('oid', '')
        if oid != '':
            self.eventObj['oid'] = oid

    def _crash_recovery(self):
        """ Duh!

        Todo: Implement for all repeat types
        """
        if self.conf['repeat'] in _repeated_types:
            print 'inside crash recovery'
            now = datetime.now()
            tommorow = datetime.combine(
                now.date() + timedelta(days=1),
                datetime.min.time()
            )
            # nexthour = (now + timedelta(hours=1)).replace(minute=0, second=0)
            try:
                lastUsed = _correct_in_time(datetime.strptime(self.conf['action'], _format))
                if self.conf['repeat'] == 'Daily':
                    if lastUsed.date() < now.date():
                        print "Scheduling for current day"
                        self._schedule()
                    else:
                        # Add a delay of a day
                        print "Scheduling for next day"
                        scheduler.add_job(self._schedule, 'date', run_date=tommorow)
                elif self.conf['repeat'] == 'Hourly':
                    chour = self.conf['hour']
                    diff = now.hour - lastUsed.hour
                    if diff >= chour:
                        print "scheduling for current hour"
                        self._schedule()
                    else:
                        thour = (now + timedelta(hours=(chour - diff))).replace(minute=0, second=0)
                        print "Scheduling for next hour ", thour
                        scheduler.add_job(self._schedule, 'date', run_date=thour)
                return True
            except:
                return False
        else:
            return False

    def _set_delay(self):  # Todo: setup for all repeat types
        """ Either schedule the emission right now or delay that """
        if (self.conf['repeat'] in _repeated_types) and \
                        self.fDate.date() > datetime.now().date():
            scheduler.add_job(self._schedule, 'date', run_date=self.sDate)
        else:
            self._schedule()

    def _set_trigger(self):
        """ Create the cron or date trigger as required
        Todo: Implement all repeat types
        """
        self.trigger = {}  # Actual trigger object for the apscheduler

        hour = self.fDate.hour
        minute = self.fDate.minute

        if self.conf['repeat'] == 'Once':
            self.trigger = DateTrigger(self.fDate)
        elif self.conf['repeat'] == 'Hourly':
            self.trigger = CronTrigger(hour='*/' + str(self.conf['hour']),
                                       minute=minute)  # hour is absolute
        elif self.conf['repeat'] == 'Daily':  # Daily
            self.trigger = CronTrigger(hour=hour, minute=minute)
        elif self.conf['repeat'] == 'Weekly':
            self.trigger = CronTrigger(day_of_week=self.fDate.weekday(), hour=hour, minute=minute)
        elif self.conf['repeat'] == 'Fortnightly':  # This one is totally fucking up
            self.trigger = DateTrigger(self.fDate)
        elif self.conf['repeat'] == 'Monthly':
            self.trigger = CronTrigger(day=self.fDate.day, hour=hour, minute=minute)
        else:
            self.trigger = DateTrigger(self.fDate)

        # DEBUG
        print "Created trigger", self.trigger
        print "curent", str(datetime.now().time())

    def _schedule(self, create_cancel=True):
        """ Schedule the emission for a future time
        Todo: somehow add the action update (DONE) after being ended
        """
        print "executing _schedule"
        self.job = scheduler.add_job(self._emit, self.trigger)
        if 'end_date' in self.conf and self.conf['end_date'] != '' and create_cancel:
            cancel_date = _correct_in_time(datetime.strptime(self.conf['end_date'], "%m/%d/%Y")) \
                          + timedelta(hours=23, minutes=58)

            # cancel_date = datetime.now() + timedelta(minutes=3)  # ONLY FOR TESTING

            print "Have an end_date: " + str(cancel_date.strftime("%d/%m/%Y %H:%M:%S"))

            def finish():
                self.cancel_job()
                self.eventObj['action'] = "Done"
                event = {
                    "type": "update_action",
                    "data": self.eventObj
                }
                dispatcher.send(signal=SIG, event=event, sender=self)

            scheduler.add_job(finish, DateTrigger(cancel_date))

    def _emit(self):
        """ Emit the event to start sending messages """
        print "emitting ", self.conf['id']
        if self.conf['repeat'] in _done_action_types:
            self.eventObj.update({"action": "Done"})
        else:
            self.eventObj.update({"action": _correct_out_time(datetime.now()).strftime(_format)})

        event = {
            "type": "send_sms",
            "data": self.eventObj
        }

        if self.conf['repeat'] == 'Fortnightly':
            print "Additional emit condition"
            self.trigger = DateTrigger(datetime.now() + timedelta(days=14))
            self._schedule(create_cancel=False)

        dispatcher.send(signal=SIG, event=event, sender=self)

    def cancel_job(self):
        print "Remove called for campaign %s with id %i" % (self.conf['campaign'], self.conf['id'])
        if hasattr(self, 'job'):
            self.job.remove()

    def next_run(self):
        if hasattr(self, 'job'):
            return self.job.next_run_time
        else:
            return None

# -------------------- Testing ------------------------------- #

import pprint

pp = pprint.PrettyPrinter(indent=2)
now = datetime.now()
riyadhNow = _correct_out_time(datetime.now())


def logFunc(sender, event):
    # print("Event: " + str(event) + "  at " + str(datetime.now().strftime("%d/%m/%Y")))
    print "Got Event at: " + str(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    pp.pprint(event)
    print ">> Next Run: ", sender.next_run().strftime("%a %d/%m/%Y %H:%M")


if __name__ == "__main__":
    print "Now: " + str(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    register(logFunc)
    # dispatcher.connect(logFunc, signal=SIG, sender=dispatcher.Any)
    wj = WatchJob({
        'campaign': 'Testcampaign',
        'arabic': "Blah blah blah",
        'english': "Hello, howr you",
        'repeat': 'Fortnightly',
        'hour': str(riyadhNow.hour),
        'minute': str((riyadhNow + timedelta(minutes=1)).minute),
        'oid': '5420ces5d013ddat510321cd',
        'start_date': _correct_out_time(datetime.now()).strftime("%m/%d/%Y"),
        'end_date': '12/1/2015',
        'id': 1,
        'action': 'Registered'
    })
    print "First Run: ", wj.next_run().strftime("%a %d/%m/%Y %H:%M")

    while True:
        sleep(1)
    # for i in range(10):
    #    sleep(1)

    wj.cancel_job()
    print "DONE"
