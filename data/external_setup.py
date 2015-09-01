import json
from job_update_api import update_job_status
from sql_data import db, connect_db
import csv
from sheet import updateAction, updateLink
import requests
import ftplib
from custom_events.main import execute_pipeline
from configuration import createLogger

cLogger = createLogger("external_setup")


def upload_file(filename, path):
    ftp = ftplib.FTP("jlabs.co")
    ftp.login("jlab", "coldplay")
    ftp.cwd("/jlabs_co/wadi/query_results")
    ftp.storlines("STOR "+filename, open(path))
    ftp.close()

def save_to_file(lst_obj, filename, headers):
    print "Query executed, writing file"
    with open(filename, 'w') as cfile:
        writer = csv.writer(cfile, quoting=csv.QUOTE_ALL)
        writer.writerow(headers)
        writer.writerows(lst_obj)


def work_external_data(event):
    try:
        oid = event['External Job']
        # url = event['External Link']
        url = 'http://45.55.72.208/wadi/query?id='+str(event['External Job'])
        r = requests.get(url)
        print "Inside work external"
        if r.status_code == 200:
            rdata = r.json()

            filename = "res_"+str(event['ID'])+".csv"
            filename_full = './data/temp/'+filename

            if 'query' in rdata:
                query = rdata['query']
                cursor = db.cursor()
                cursor.execute(query)
                print "Got query"+query
                save_to_file(cursor, filename_full, ['Phone', 'Language'])
            elif 'pipeline' in rdata and 'options' in rdata:
                res, headers = execute_pipeline(rdata['pipeline'], rdata['options'])
                update_job_status(oid, customer_count=len(res))     # Notify the API about the total number of customers
                save_to_file(res, filename_full, headers)
            else:
                raise Exception

            print "Updating action"
            upload_file(filename, filename_full)
            if event.get('Repeat', 'Once') == 'No Send':
                action = 'Done'
            else:
                action = 'Data Loaded'
            updateAction(event['ID'], action, oid=oid)
            print "Updating link"
            updateLink(event['ID'], 'http://jlabs.co/downloadcsv.php?file='+filename, oid=oid)
            return True
        else:
            updateAction(event['ID'], 'Bad Link', oid=oid)
            return False
    except Exception:
        cLogger.exception("crashed with event %s", str(event))
        return False
