# -*- encoding: utf-8 -*-

# This module does the data downloading part from the csv
import csv
import requests
import datetime
import zipfile
from bs4 import BeautifulSoup
from config import config
import pymysql

QUERRY = {
    "all" : "select distinct b.number,if(a.fk_language=1,'English','Arabic') as language from customer a inner join customer_phone b on b.fk_customer = a.id_customer order by a.id_customer desc"
    "other" : "select distinct phone,if(language_code='en','English','Arabic') from promotion_subscription WHERE promotion_type LIKE %s"
}

# ------------ Helper functions ------------------
def str_to_hex(text):
    text = text.strip("u").strip("'")
    arabic_hex = [hex(ord(b)).replace("x","").upper().zfill(4) for b in text]
    arabic_hex.append("000A")
    text_update = "".join(arabic_hex)
    return text_update
# ------------------------------------------------
#-----------------Get Campaign Data---------------

def getUserData(campaign):
    data = []
    if campaign.lower() in "all":
        cx = pymysql.connect(user='maowadi', password='FjvQd3fvqxNhszcU',database='jerry_live', host="db02")
        cu = cx.cursor()
        cu.execute(QUERRY['all'])
        for x in cu:
            data.append(x)
    else:
        cx = pymysql.connect(user='maowadi', password='FjvQd3fvqxNhszcU',database='jerry_live', host="db02")
        cu = cx.cursor()
        cu.execute(QUERRY['other'],campaign)
        for x in cu:
            data.append(x)
    return data
#--------------------------------------------------
# --------------------- Main method ------------------------
def load_data(event):
    try:
        campaign = event['Campaign']
        ar = event['Arabic']
        en = event['English']
        sms_dict = {'ar':ar,'en':en}
        payloadArr = []
        data = getUserData(campaign)
        for d in data:
            payload = {}
            if d[1].strip() in "Arabic":
                message_text = sms_dict['ar']
                payload = {'message': message_text,'mobilenumber':d[0].strip("=").strip(), 'mtype': "OL"}
            elif d[1].strip() in "English":
                message_text = sms_dict['en']
                payload = {'message': message_text,'mobilenumber':d[0].strip("=").strip(), 'mtype': "N"}
            payloadArr.append(payload)

        return (True, payloadArr)
    except Exception:
        return (False, None, None)
    finally:
        pass
# -----------------------------------------------------------
if __name__ == '__main__':
    event = {"Campaign":"midnight_campaign","English":"hahahaha","Arabic":"SubhanAllah"}
    a,b =  load_data(event)
    print a
    print b[9]