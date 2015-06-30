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
    watchjob.register(handler)

_currentJobs = {}
_newJobs = {}
_cid = 0

""" Add a single Job
Returns the id """
def _addJob (conf):
    global _currentJobs
    global _newJobs
    global _cid 

    if conf['ID'] == "":
        conf['ID'] = str(_cid)
        _cid += 1
        
        _newJobs[conf['ID']] = watchjob.WatchJob(conf)
        return conf['ID']

    elif conf['Action'].strip() == "" and _currentJobs.has_key(conf['ID']):
            # restart job
            _currentJobs[conf['ID']].cancelJob()        # Cancel the job
            _newJobs[conf['ID']] = watchjob.WatchJob(conf)
            return conf['ID']
    elif not _currentJobs.has_key(conf['ID']):           # Event of a crash
            _newJobs[conf['ID']] = watchjob.WatchJob(conf)
            return None
    elif _currentJobs.has_key(conf['ID']):               # same, transfere
            _newJobs[conf['ID']] = _currentJobs.pop(conf['ID'])
            return None
    else:   # Dont think we will reach this
            return None

""" Actual method to use """
def configure_jobs (csvlist):
    global _currentJobs
    global _newJobs
    for conf in csvlist:
        res = _addJob (conf)
        if res is not None:
            print "update: "+res

    print "Remaining ", _currentJobs
    for k, remaining in _currentJobs.iteritems():
        remaining.cancelJob()
    #del _currentJobs
    _currentJobs = _newJobs
    _newJobs = {}


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
        "Campaign": "Test1",
        "Start Date": "6/30/2015",
        "Repeat": "Test",
        "Hour": '1',
        "Minute": '20',
        "Arabic": "Blah blah",
        "English": "Blu blue",
        "Type": "SMS",

        "Action": "",
        "ID": "0"
    }
]

def test1():
    watchjob.register(watchjob.logFunc)
    configure_jobs (_t1)

def test2():
    configure_jobs (_t2)
