import json
from sql_data import db
import csv
from sheet import updateAction, updateLink
import requests
import ftplib


ftp = ftplib.FTP("jlabs.co")
ftp.login("jlab", "coldplay")
ftp.cwd("/jlabs_co/wadi/query_results")

def upload_file(filename, path):
    ftp.storlines("STOR "+filename, open(path))

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
        filename = "res_"+event['ID']+".csv"
        filename_full = './data/temp/'+filename
        save_to_file(query, filename_full)
        updateAction(event['ID'], 'Data Loaded')
        upload_file(filename, filename_full)
        updateLink(event['ID'], 'http://jlabs.co/wadi/query_results/'+filename)
    else:
        updateAction(event['ID'], 'Bad Link')
