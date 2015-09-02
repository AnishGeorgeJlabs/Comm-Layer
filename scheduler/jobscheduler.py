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
    external['update_id'] = func            # (id, index) -> {}


def set_action_update(func):
    external['update_action'] = func        # (id, action) -> {}

_idSet = set([])
_currentJobs = {}
_newJobs = {}
_cid = 1

""" Add a single Job
Returns the id """
def _addJob (conf):
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

    elif conf['Action'].strip() == "" and conf['id'] in _currentJobs:   #_currentJobs.has_key(conf['id']):
            print " c2. Cleared Action"
            # restart job
            _currentJobs[conf['id']].cancel_job()        # Cancel the job
            return helper()

    elif conf['id'] not in _currentJobs:   #not _currentJobs.has_key(conf['id']):           # Event of a crash
            print " c3. App crash or tampering"
            return helper()                     # Problematic
    elif conf['id'] in _currentJobs:    #_currentJobs.has_key(conf['id']):               # same, transfere
            print " c4. Action not cleared, same job, ignore: ", conf['id']
            _newJobs[conf['id']] = _currentJobs.pop(conf['id'])
            return None
    else:   # Dont think we will reach this
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
            conf = _data_map(config)
            if conf['action'].lower() in ['done', 'processing', 'missed', 'bad link', 'cancel']:
                continue
            res = _addJob (conf)
            if res is not None:

                oid = conf.get('oid') # For API notification

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


def _data_map (conf):
    res = {
        "repeat": conf['Repeat'],
        "campaign": conf['Campaign'],
        "start_date": conf['Start Date'],
        "hour": conf['Hour'],
        "minute": conf['Minute'],
        "english": conf['English'],
        "arabic": conf['Arabic'],
        "oid": conf['External Job'],
        "action": conf['Action'],
        "id": conf['ID'],
        "data_link": conf['Data Link']
    }
    for key in ['hour', 'minute']:
        if res[key] == '' or res[key] == '_':
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

    return res
# ----------------- Tests ------------------ #
_t1 = [
    {
        "Campaign": "Test1",
        "Start Date": "6/30/2015",
        "Repeat": "Test",
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
        "Repeat": "Test",
        "Hour": '1',
        "Minute": '20',
        "Arabic": "Blah blah",
        "English": "Blu blue",
        "Type": "SMS",

        "Action": "",
        "ID": ""
    },
]
_t2 = [
    {
        "Campaign": "Test3",
        "Start Date": "6/30/2015",
        "Repeat": "Test",
        "Hour": '1',
        "Minute": '20',
        "Arabic": "Blah blah",
        "English": "Blu blue",
        "Type": "SMS",

        "Action": "Done",
        "ID": "0"
    }
]

def _logPrint(i, id):
    pass
    # print "updating ", i, id

def test1():
    register(watchjob.logFunc)
    set_id_update(_logPrint)
    set_action_update(_logPrint)
    configure_jobs (_t1)

def test2():
    configure_jobs (_t2)
