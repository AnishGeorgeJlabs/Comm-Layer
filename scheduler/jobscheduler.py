# Full Scheduler for waid 

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
    print "jobscheduler.register"
    watchjob.register(handler)

external = {}
def set_id_update(func):
    external['update_id'] = func            # (id, index) -> {}


def set_action_update(func):
    external['update_action'] = func        # (id, action) -> {}

_idList = []
_currentJobs = {}
_newJobs = {}
_cid = 0

""" Add a single Job
Returns the id """
def _addJob (conf):
    print "Inside addJob"
    global _currentJobs
    global _newJobs
    global _cid
    global _idList

    def helper():
        jb = watchjob.WatchJob(conf)
        if jb.valid:
            _newJobs[conf['ID']] = jb
        return conf['ID'], jb.valid

    if str(conf['ID']).strip() == "":
        print " c1. No ID, new one"
        while _cid in _idList:
            _cid += 1
        conf['ID'] = str(_cid)
        _cid += 1

        # Validity for job, only if the case is repeat = 'once' else always valid
        return helper()

    elif conf['Action'].strip() == "" and conf['ID'] in _currentJobs:   #_currentJobs.has_key(conf['ID']):
            print " c2. Cleared Action"
            # restart job
            _currentJobs[conf['ID']].cancelJob()        # Cancel the job
            return helper()

    elif conf['ID'] not in _currentJobs:   #not _currentJobs.has_key(conf['ID']):           # Event of a crash
            print " c3. App crash or tampering"
            return helper()                     # Problematic
    elif conf['ID'] in _currentJobs: #_currentJobs.has_key(conf['ID']):               # same, transfere
            print " c4. Action not cleared, same job, ignore"
            _newJobs[conf['ID']] = _currentJobs.pop(conf['ID'])
            return None
    else:   # Dont think we will reach this
            print " c5. case FUCKED"
            return None

""" Actual method to use """
def configure_jobs(csvlist):
    print " Configuring jobs"
    global _currentJobs
    global _newJobs
    global _idList
    if csvlist:
        for conf in csvlist:
            if conf['ID'] != "":
                _idList.append(int(conf['ID']))
        for i, conf in enumerate(csvlist):
            if conf['Action'] in ['Done', 'Processing', 'Missed']:
                continue
            res = _addJob (conf)
            if res is not None:
                if res[1]:
                    external['update_id'](res[0], i, 'Registered')
                else:
                    external['update_id'](res[0], i, 'Missed')

        #print "Remaining ", _currentJobs
        for k, remaining in _currentJobs.iteritems():
            remaining.cancelJob()
        _currentJobs = _newJobs
        _newJobs = {}
    else:
        _currentJobs = {}


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
