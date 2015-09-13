import requests
import json

def _send(data):
    try:
        api_url = "http://45.55.72.208/wadi/job_update"
        headers = {'Content-type': 'application/json'}
        r = requests.post(api_url, data=json.dumps(data), headers=headers)
        return r.status_code == 200 and r.json().get('success', False)
    except:
        return False

def update_job_status(oid, **kwargs):
    if id is None or id == '':
        return True
    else:
        kwargs['id'] = oid
        return _send(kwargs)
