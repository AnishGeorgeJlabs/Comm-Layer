# -*- encoding: utf-8 -*-

# This module does the data downloading part from the csv
import pymysql
from sheet import updateAction, get_testing_sheet, get_custom_sheet
import pymongo
mdb = pymongo.MongoClient("mongodb://45.55.232.5:27017").wadi

QUERRY = {
    "all" : "select distinct b.number,if(a.fk_language=1,'English','Arabic') as language from customer a inner join customer_phone b on b.fk_customer = a.id_customer order by a.id_customer desc",
    "all_uae" : "select distinct b.number,if(a.fk_language=1,'English','Arabic') as language from customer a inner join customer_phone b on b.fk_customer = a.id_customer where a.fk_country = 3 order by a.id_customer desc",
    "all_ksa" : "select distinct b.number,if(a.fk_language=1,'English','Arabic') as language from customer a inner join customer_phone b on b.fk_customer = a.id_customer where a.fk_country = 193 order by a.id_customer desc",
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
    print "inside getUserData"
    data = []
    if campaign.lower().startswith("all"):       # Actually, all in campaig.lower()
        cx = pymysql.connect(user='maowadi', password='FjvQd3fvqxNhszcU',database='jerry_live', host="db02")
        cu = cx.cursor()
        cu.execute(QUERRY[campaign.lower()])
        for x in cu:
            data.append(x)
    elif campaign.lower() in "testing":
        # data = [["919818261929","Arabic"],["917838310825","English"],["971559052361","Arabic"]]
        data = get_testing_sheet().get_all_values()[1:]     # Gives data in list of list format, skipping the header row
    elif campaign.lower() in "custom":
        data = get_custom_sheet().get_all_values()[1:]     # Gives data in list of list format, skipping the header row
    else:
        cx = pymysql.connect(user='maowadi', password='FjvQd3fvqxNhszcU',database='cerberus_live', host="db02")
        cu = cx.cursor()
        cu.execute(QUERRY['other'],campaign)
        for x in cu:
            data.append(x)
    blocked_list = set([a['number']+','+a['language'] for a in mdb.blocked.find({}, {"_id": False})])
    data = [a for a in data if ','.join([a[0], a[1]]) not in blocked_list]
    return data
# --------------------------------------------------

# ------------------Update Action-------------------
# Has now been moved to sheet.py
"""
def updateAction(id,action):
    print "inside updateAction"
    storage = Storage("creds.dat")
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        flags = tools.argparser.parse_args(args=[])
        flow = flow_from_clientsecrets("client_secret.json", scope=["https://spreadsheets.google.com/feeds"])
        credentials = tools.run_flow(flow, storage, flags)
    if credentials.access_token_expired:
        credentials.refresh(httplib2.Http())
    gc = gspread.authorize(credentials)
    wks = gc.open_by_key('144fuYSOgi8md4n2Ezoj9yNMi6AigoXrkHA9rWIF0EDw')
    worksheet = wks.get_worksheet(1)
    val = worksheet.get_all_records()
    for x in val:
        if id is x['ID']:
            rowNum = val.index(x) + 2
            column = 'I'+str(rowNum)
            print column
            worksheet.update_acell(column, str(action))
"""


# --------------------- Main method ------------------------
def load_data(event):
    try:
        print "Inside Data loader"
       #print "Event: ", str(event)
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
                payload = {'message': str_to_hex(message_text),'mobilenumber':d[0].strip("=").strip().replace('+','').replace('-',''), 'mtype': "OL"}
            elif d[1].strip() in "English":
                message_text = sms_dict['en']
                payload = {'message': message_text,'mobilenumber':d[0].strip("=").strip().replace('+','').replace('-',''), 'mtype': "N"}
            payloadArr.append(payload)

        return (True, payloadArr)
    except Exception:
        return (False, None)
    finally:
        updateAction(event['ID'],event['Action'])
        pass
# -----------------------------------------------------------
if __name__ == '__main__':
    event = {"Campaign":"testing","English":"hahahaha","Arabic":"SubhanAllah"}
    a,b =  load_data(event)
    print a
    print b[9]
