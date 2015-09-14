# Full Scheduler for waid

# Okay, we use ID to be ints only

"""
csv object
{
    "Campaign": <name>
    "Start Date": 'm/d/yyyy'
    "Hour": '<num>'
    "Minute": '<num>'
    "Arabic": 'str'
    "English": 'str'
    "Type": 'SMS'
    "Repeat": 'str'


    "Action": Done | last date
    "ID": id
}
"""

import watchjob


def register(handler):
    watchjob.register(handler)


external = {}


def set_id_update(func):
    external['update_id'] = func  # (id, index) -> {}


def set_action_update(func):
    external['update_action'] = func  # (id, action) -> {}


_idSet = set([])
_currentJobs = {}
_newJobs = {}
_cid = 1

""" Add a single Job
Returns the id """


def _addJob(conf):
    print "Inside addJob"
    global _currentJobs
    global _newJobs
    global _cid
    global _idSet

    def helper():
        jb = watchjob.WatchJob(conf)
        if jb.valid:
            _newJobs[conf['id']] = jb
        return conf['id'], jb.valid

    if conf['id'] == 0:
        print " c1. No id, new one"
        while _cid in _idSet:
            _cid += 1
        conf['id'] = _cid
        _cid += 1

        # Validity for job, only if the case is repeat = 'once' else always valid
        return helper()

    elif conf['action'].strip() == "" and conf['id'] in _currentJobs:  # _currentJobs.has_key(conf['id']):
        print " c2. Cleared Action"
        # restart job
        _currentJobs[conf['id']].cancel_job()  # Cancel the job
        return helper()

    elif conf['id'] not in _currentJobs:  # not _currentJobs.has_key(conf['id']):           # Event of a crash
        print " c3. App crash or tampering"
        return helper()  # Problematic
    elif conf['id'] in _currentJobs:  # _currentJobs.has_key(conf['id']):               # same, transfere
        print " c4. Action not cleared, same job, ignore: ", conf['id']
        _newJobs[conf['id']] = _currentJobs.pop(conf['id'])
        return None
    else:  # Dont think we will reach this
        print " c5. case FUCKED"
        return None


""" Actual method to use """


def configure_jobs(csvlist):
    print " Configuring jobs"
    global _currentJobs
    global _newJobs
    global _idSet
    if csvlist:
        for conf in csvlist:
            if conf['ID'] != "":
                _idSet.add(int(conf['ID']))
        for i, config in enumerate(csvlist):
            conf, valid = _data_map(config)
            if conf['action'].lower() in ['done', 'processing', 'missed', 'bad link', 'cancel', 'data load failed'] or \
                    not valid:
                continue
            res = _addJob(conf)
            if res is not None:

                oid = conf.get('oid')  # For API notification

                if res[1]:
                    external['update_id'](res[0], i, 'Registered', oid=oid)
                else:
                    external['update_id'](res[0], i, 'Missed', oid=oid)

        for k, remaining in _currentJobs.iteritems():
            remaining.cancel_job()
        _currentJobs = _newJobs
        _newJobs = {}
    else:
        _currentJobs = {}


def is_data_empty(v):
    return str(v).strip() in ['', '_']


def _data_map(conf):
    res = {
        "repeat": conf['Repeat'],
        "campaign": conf['Campaign'],
        "start_date": conf['Start Date'],
        "hour": conf['Hour'],
        "minute": conf['Minute'],
        "english": conf['English'],
        "arabic": conf['Arabic'],
        "action": conf['Action'],
        "id": conf['ID'],
        "data_link": conf.get('Data Link', '')
    }
    if conf.get('External Job', '') != '':
        ext = conf['External Job']
        if res['campaign'] == 'segment':
            dt = [x.strip() for x in ext.split(',')]  # " OID, t_id, lower_limit, upper_limit
            if len(dt) < 4 or any([is_data_empty(res[key]) for key in ['start_date', 'hour', 'minute']]):
                return res, False
            res['repeat'] = 'Once'
            res['oid'] = dt[0]
            try:
                res['segment_data'] = {
                    "lower_limit": int(dt[2]),
                    "upper_limit": int(dt[3])
                }
                if dt[1].startswith('external_database_'):
                    res['ext_db'] = dt[1].replace('external_config_', '')
                else:
                    res['ref_id'] = int(dt[1])
            except Exception, e:
                print "Returning False from _data_map:: "+str(e)
                return res, False
        else:
            res['oid'] = ext
    if not is_data_empty(conf.get('End Date', '')):
        res['end_date'] = conf['End Date']

    for key in ['hour', 'minute']:
        if is_data_empty(res[key]):
            res[key] = 0
        else:
            try:
                res[key] = int(res[key])
            except:
                res[key] = 0
    if res['id'] != '':
        res['id'] = int(res['id'])
    else:
        res['id'] = 0

    return res, True

# ----------------- Tests ------------------ #
_t1 = [
    {
        "Campaign": "Test1",
        "Start Date": "6/30/2015",
        "Repeat": "Once",
        "Hour": '1',
        "Minute": '20',
        "Arabic": "Blah blah",
        "English": "Blu blue",
        "Type": "SMS",

        "Action": "",
        "ID": ""
    },
    {
        "Campaign": "Test2",
        "Start Date": "6/30/2015",
        "Repeat": "Hourly",
        "Hour": '1',
        "Minute": '20',
        "Arabic": "Blah blah",
        "English": "Blu blue",
        "Type": "SMS",

        "Action": "",
        "ID": ""
    },
]
from watchjob import _correct_out_time
from datetime import datetime, timedelta

riyadhNow = _correct_out_time(datetime.now())
_t2 = [
    {
        'Campaign': 'segment',
        'Arabic': "Blah blah blah",
        'English': "Hello, howr you",
        'Repeat': 'Once',
        # 'hour': 1,
        'Hour': (riyadhNow + timedelta(minutes=1)).hour,
        'Minute': (riyadhNow + timedelta(minutes=1)).minute,
        'External Job': '5420ces5d013ddat510321cd,56,0,1000',
        'Start Date': _correct_out_time(datetime.now()).strftime("%m/%d/%Y"),
        'End Date': '12/1/2015',
        'ID': 59,
        'Action': 'Registered'
    }
]


def _logPrint(i, id, *args, **kwargs):
    print "updating ", i, id
    print "Got arguments: " + str(args)
    print "Got kwargs: " + str(kwargs)


def test1():
    register(watchjob.logFunc)
    set_id_update(_logPrint)
    set_action_update(_logPrint)
    configure_jobs(_t2)


def test2():
    configure_jobs(_t2)


if __name__ == '__main__':
    from time import sleep

    test1()
    while True:
        sleep(1)
