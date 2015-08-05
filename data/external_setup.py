import json
from sql_data import db
import csv
from sheet import updateAction
import requests

def save_to_file(query, filename):
    cursor = db.cursor()
    cursor.execute(query)
    with open(filename, 'w') as cfile:
        writer = csv.writer(cfile)
        writer.writerow([i[0] for i in cursor.description])
        writer.writerows(cursor)

def work_external_data(event):
    url = event['External Link']
    r = requests.get(url)
    if r.status_code == 200:
        rdata = r.json()
        query = rdata['query']
        filename = './data/temp/'+event['ID']+'.csv'
        save_to_file(query, filename)
    else:
        updateAction(event['ID'], 'Bad Link')