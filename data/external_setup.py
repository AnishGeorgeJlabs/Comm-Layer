import json
from sql_data import db, connect_db
import csv
from sheet import updateAction, updateLink
import requests
import ftplib


ftp = ftplib.FTP("jlabs.co")
ftp.login("jlab", "coldplay")
ftp.cwd("/jlabs_co/wadi/query_results")

def upload_file(filename, path):
    ftp.storlines("STOR "+filename, open(path))

def remove_duplicates(data):
    """Remove duplicates from the csv data list, prefer phones with arabic language attachment
    This algorithm assumes that phones are the first column and language is the second one"""

    sorting_dict = {}
    for row in data:
        phone = row.pop(0)
        if phone not in sorting_dict:
            sorting_dict[phone] = row
        else:
            olang = sorting_dict[phone][0].lower()
            clang = row[0].lower()
            if olang in 'english' and clang in 'arabic':
                sorting_dict[phone] = row

    return [[k] + row for k, row in sorting_dict.items()]

def complete_event(query_event):

    pass

def mega_query_save_to_file(queries, filename):
    """ The mega complicated process, doc pending

    :param queries:
    :param filename:
    :return:
    """

    # Super algorithm, God knows how much time it will take
    extra_data = []         # A list of dicts { id_customer: [array of data] }
    cid_set = set()
    for query_event in queries:
        k, res = complete_event(query_event)        # Following event specs

        cid_set.intersection(k)
        if len(cid_set) == 0:   # Short circuit to save calculation
            break

        extra_data.append(res)


    final = []
    for k in cid_set:
        data = []
        for ed in extra_data:
            data = ed[k] + data     # Prepend to get reverse pipeline
        final.append(data)          # if all is well, phone and language will be first

    # Note, at this point, we need a list[list] structure
    # final = remove_duplicates(result)
    with open(filename, 'w') as cfile:
        writer = csv.writer(cfile)
        writer.writerow(["Phone", "Language"])
        writer.writerows(final)

def save_to_file(query, filename):
    cursor = db.cursor()
    cursor.execute(query)
    print "Query executed, writing file"
    with open(filename, 'w') as cfile:
        writer = csv.writer(cfile)
        writer.writerow([i[0] for i in cursor.description])
        writer.writerows(cursor)


def work_external_data(event):
    url = event['External Link']
    r = requests.get(url)
    print "Inside work external"
    if r.status_code == 200:
        rdata = r.json()
        query = rdata['query']
        print "Got query"+query
        filename = "res_"+event['ID']+".csv"
        filename_full = './data/temp/'+filename

        if isinstance(query, list):
            mega_query_save_to_file(query, filename_full)
        else:
            save_to_file(query, filename_full)

        print "Updating action"
        upload_file(filename, filename_full)
        updateAction(event['ID'], 'Data Loaded')
        print "Updating link"
        updateLink(event['ID'], 'http://jlabs.co/wadi/query_results/'+filename)
    else:
        updateAction(event['ID'], 'Bad Link')
