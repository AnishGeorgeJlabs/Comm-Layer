# -*- encoding: utf-8 -*-

# This module does the data downloading part from the csv
from sheet import updateAction, get_testing_sheet, get_custom_sheet
from sql_data import db
import string
import requests
import csv
from configuration import createLogger
from custom_events.aux import get_block_set

QUERRY = {
    "all": """SELECT DISTINCT b.number,if(a.fk_language=1,'English','Arabic') AS language
              FROM customer a INNER JOIN customer_phone b ON b.fk_customer = a.id_customer
              ORDER BY a.id_customer DESC""",
    "all_uae": """SELECT DISTINCT b.number,if(a.fk_language=1,'English','Arabic') AS LANGUAGE
                  FROM customer a INNER JOIN customer_phone b ON b.fk_customer = a.id_customer
                  WHERE a.fk_country = 3
                  ORDER BY a.id_customer DESC""",
    "all_ksa": """SELECT DISTINCT b.number,if(a.fk_language=1,'English','Arabic') AS language
                  FROM customer a INNER JOIN customer_phone b ON b.fk_customer = a.id_customer
                  WHERE a.fk_country = 193
                  ORDER BY a.id_customer DESC""",
    "other": """SELECT DISTINCT phone,if(language_code='en','English','Arabic')
                FROM promotion_subscription
                WHERE promotion_type LIKE %s"""
}

cLogger = createLogger("data_loader")

# ---------------------------------------------------- Helper functions ------------------------------------------------
def str_to_hex(text):
    """ Converts a string to hex values, used for arabic messages """
    text = text.strip("u").strip("'")
    arabic_hex = [hex(ord(b)).replace("x", "").upper().zfill(4) for b in text]
    arabic_hex.append("000A")
    text_update = "".join(arabic_hex)
    return text_update


def clean_english(text):
    """ Converts any possible unicode to its ascii variant else removes unicode from english message """
    # Support for en and em dash which is a common english unicode
    repText = ''.join(map(
        lambda c: '-' if c == u'\u2013' or c == u'\u2014' else c,
        text
    ))
    filtered_txt = str(filter(lambda k: k in string.printable, repText))
    return filtered_txt


# ------------------------------------------------
# -----------------Get Campaign Data---------------
def get_external_data(event):
    """ Get the dataset from external csv file which was initially uploaded by the tool itself :param id: """
    if event['campaign'] == 'external':
        id = event['id']
    else:
        id = event.get('segment_data', {}).get('ref_id', 0)

    if id != 0:
        url = "http://jlabs.co/wadi/query_results/res_" + str(id) + ".csv"
    else:
        url = event.get('segment_data', {}).get('db_file', '')
        if url == '':
            return []
    print "Got url: " + url
    if url == '':
        return []
    r = requests.get(url)
    if r.status_code == 200:
        raw = filter(lambda k: len(k) > 0, r.text.split("\n"))[1:]
        reader = csv.reader(raw)
        data = [  # External file may have additional fields
                  x[:3] for x in
                  list(reader)
                  ]

        if event['campaign'] == 'segment':
            lower_limit = event.get('segment_data', {}).get('lower_limit', 0)
            upper_limit = event.get('segment_data', {}).get('upper_limit', len(data))
            return data[lower_limit: upper_limit]
        else:
            return data
    else:
        return []


def getUserData(event):
    """ Get the [phone, language] or [phone, language, country] for the customers """
    data = []
    campaign = event['campaign']
    id = event['id']
    clo = campaign.lower()
    print "inside getUserData, " + clo + ", and id: " + str(id)
    if clo in ["external", "segment"]:  # Main priority
        data = get_external_data(event)
        print "Got external data: " + str(data)
    elif clo.startswith("all"):  # Actually, all in campaig.lower()
        print "Starts with all"
        cu = db.cursor()
        cu.execute(QUERRY[clo])
        for x in cu:
            data.append(x)
    elif clo in "testing":
        print "Testing campaign"
        data = get_testing_sheet().get_all_values()[1:]  # Gives data in list of list format, skipping the header row
    elif clo in "custom":
        print "Custom campaign"
        data = get_custom_sheet().get_all_values()[1:]  # Gives data in list of list format, skipping the header row
    elif clo.startswith("cust_"):
        print "starts with cust_"
        data = get_custom_sheet(clo).get_all_values()[1:]
    else:
        print "Different: " + clo
        cu = db.cursor()
        cu.execute(QUERRY['other'], campaign)
        for x in cu:
            data.append(x)
    blocked_set = get_block_set()
    data = map(
        lambda k: [str(k[0]).strip('+ ').replace('-', '')] + list(k[1:]),
        data
    )
    data = [a for a in data if ','.join(a[0:2]) not in blocked_set]
    return data


def load_data(event):
    """ Main Method for loading the data """
    try:
        print "Inside Data loader"
        campaign = event['campaign']
        ar = event['arabic']
        en = event['english']

        sms_dict_n = {'ar': str_to_hex(ar), 'en': clean_english(en)}  # Normal cases
        sms_dict_ae = {'ar': str_to_hex(ar + '\nOPTOUT@4782'),
                       'en': clean_english(en + '\nOPTOUT@4782')}  # for uae cust

        if 'uae' in campaign.lower():
            sms_dict = sms_dict_ae
        else:
            sms_dict = sms_dict_n

        payloadArr = []
        data = getUserData(event)
        print "Got data: ", data
        for d in data:
            if 'external' in campaign.lower() and len(d) > 2 and 'UAE' in d[2]:
                sms_dict = sms_dict_ae
            elif 'external' in campaign.lower():
                sms_dict = sms_dict_n

            payload = {}
            if d[1].strip() in "Arabic":
                message_text = sms_dict['ar']
                payload = {'message': message_text,
                           'mobilenumber': d[0].strip("=").strip().replace('+', '').replace('-', ''), 'mtype': "OL"}
            elif d[1].strip() in "English":
                message_text = sms_dict['en']
                payload = {'message': message_text,
                           'mobilenumber': d[0].strip("=").strip().replace('+', '').replace('-', ''), 'mtype': "N"}
            payloadArr.append([event['id'], payload])

        # Now the sms_sender is responsible for doing the final
        # update Action saying things are done
        payloadArr.append(['sentinel', {
            'sentinel': {
                'id': event['id'],
                'action': event['action'],
                'oid': event.get('oid')
            }
        }])
        return True, payloadArr
    except Exception, e:
        cLogger.exception("with event as %s, data_loader crashed", str(event))
        return False, None
    finally:
        updateAction(event['id'], 'Processing', oid=event.get('oid'))
