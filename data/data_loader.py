# -*- encoding: utf-8 -*-

# This module does the data downloading part from the csv
from sheet import updateAction, get_testing_sheet, get_custom_sheet, get_block_sheet, getFileLink
from sql_data import db
import json
import string
import requests
import csv
from configuration import createLogger

QUERRY = {
    "all" : "select distinct b.number,if(a.fk_language=1,'English','Arabic') as language from customer a inner join customer_phone b on b.fk_customer = a.id_customer order by a.id_customer desc",
    "all_uae" : "select distinct b.number,if(a.fk_language=1,'English','Arabic') as language from customer a inner join customer_phone b on b.fk_customer = a.id_customer where a.fk_country = 3 order by a.id_customer desc",
    "all_ksa" : "select distinct b.number,if(a.fk_language=1,'English','Arabic') as language from customer a inner join customer_phone b on b.fk_customer = a.id_customer where a.fk_country = 193 order by a.id_customer desc",
    "other" : "select distinct phone,if(language_code='en','English','Arabic') from promotion_subscription WHERE promotion_type LIKE %s"
}

cLogger = createLogger("data_loader")

# ------------ Helper functions ------------------
def str_to_hex(text):
    text = text.strip("u").strip("'")
    arabic_hex = [hex(ord(b)).replace("x","").upper().zfill(4) for b in text]
    arabic_hex.append("000A")
    text_update = "".join(arabic_hex)
    return text_update

def clean_english(text):
    # Support for en and em dash which is a common english unicode
    repText = ''.join(map(
        lambda c: '-' if c == u'\u2013' or c == u'\u2014' else c,
        text
    ))
    filtered_txt = str(filter(lambda k: k in string.printable , repText))
    return filtered_txt
# ------------------------------------------------
#-----------------Get Campaign Data---------------
def get_external_data(id):
    url = "http://jlabs.co/wadi/query_results/res_"+str(id)+".csv"
    print "Got url: "+url
    if url == '':
        return []
    r = requests.get(url)
    if r.status_code == 200:
        raw = filter(lambda k: len(k) > 0, r.text.split("\n"))[1:]
        reader = csv.reader(raw)
        data = [                        # External file may have additional fields
            x[:3] for x in
            list(reader)
        ]
        return data
    else:
        return []

def getUserData(campaign, id):
    data = []
    clo = campaign.lower()
    print "inside getUserData, "+clo+", and id: "+id
    if clo in "external":
        data = get_external_data(id)
        print "Got external data: "+str(data)
    elif clo.startswith("all"):       # Actually, all in campaig.lower()
        print "Starts with all"
        #cx = pymysql.connect(user='maowadi', password='FjvQd3fvqxNhszcU',database='jerry_live', host="db02")
        cu = db.cursor()
        cu.execute(QUERRY[clo])
        for x in cu:
            data.append(x)
    elif clo in "testing":
        print "Testing campaign"
        # data = [["919818261929","Arabic"],["917838310825","English"],["971559052361","Arabic"]]
        data = get_testing_sheet().get_all_values()[1:]     # Gives data in list of list format, skipping the header row
    elif clo in "custom":
        print "Custom campaign"
        data = get_custom_sheet().get_all_values()[1:]     # Gives data in list of list format, skipping the header row
    elif clo.startswith("cust_"):
        print "starts with cust_"
        data = get_custom_sheet(clo).get_all_values()[1:]
    else:
        print "Different: "+clo
        #cx = pymysql.connect(user='maowadi', password='FjvQd3fvqxNhszcU',database='cerberus_live', host="db02")
        cu = db.cursor()
        cu.execute(QUERRY['other'],campaign)
        for x in cu:
            data.append(x)
    blocked_list = set([a[0]+','+a[1] for a in get_block_sheet().get_all_values()[1:]])
    data = [a for a in data if ','.join([a[0], a[1]]) not in blocked_list]
    return data
# --------------------------------------------------

# --------------------- Main method ------------------------
def load_data(event):
    try:
        print "Inside Data loader"
        campaign = event['Campaign']
        ar = event['Arabic']
        en = event['English']

        sms_dict_n = {'ar': str_to_hex(ar), 'en': clean_english(en)}       # Normal cases
        sms_dict_ae = {'ar': str_to_hex(ar+'\nOPTOUT@4782'), 'en': clean_english(en + '\nOPTOUT@4782')}   # for uae cust

        if 'uae' in campaign.lower():
            sms_dict = sms_dict_ae

        payloadArr = []
        data = getUserData(campaign, event['ID'])
        for d in data:
            if 'external' in campaign.lower() and len(d) > 2 and 'UAE' in d[2]:
                sms_dict = sms_dict_ae
            elif 'external' in campaign.lower():
                sms_dict = sms_dict_n

            payload = {}
            if d[1].strip() in "Arabic":
                message_text = sms_dict['ar']
                payload = {'message': message_text,'mobilenumber':d[0].strip("=").strip().replace('+','').replace('-',''), 'mtype': "OL"}
            elif d[1].strip() in "English":
                message_text = sms_dict['en']
                payload = {'message': message_text,'mobilenumber':d[0].strip("=").strip().replace('+','').replace('-',''), 'mtype': "N"}
            payloadArr.append([str(event['ID']), payload])

        # Now the sms_sender is responsible for doing the final
        # update Action saying things are done
        payloadArr.append(['sentinel', {
            'sentinel': {
                'ID': event['ID'],
                'Action': event['Action']
            }
        }])
        return True, payloadArr
    except Exception, e:
        cLogger.exception("with event as %s, data_loader crashed", str(event))
        return False, None
    finally:
        updateAction(event['ID'], 'Processing')
        pass
# -----------------------------------------------------------
if __name__ == '__main__':
    event = {"Campaign":"testing","English":"hahahaha","Arabic":"SubhanAllah"}
    a,b =  load_data(event)
    print a
    print b[9]
