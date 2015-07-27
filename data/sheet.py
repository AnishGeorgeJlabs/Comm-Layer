import httplib2
# Do OAuth2 stuff to create credentials object
from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets
from oauth2client import tools
import gspread


def get_worksheet(i):
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
    return wks.get_worksheet(i)

def get_scheduler_sheet():
    return get_worksheet(0)

def get_testing_sheet():
    return get_worksheet(3)
def get_custom_sheet():
    return get_worksheet(4)
def get_block_sheet():              ## NOTE: Only to be used by blockList.py
    return get_worksheet(5)

def updateId(id, row, *arg):
    print 'inside updateId, ', id, row
    cell = 'J'+str(row + 2)
    worksheet = get_scheduler_sheet()
    worksheet.update_acell(cell, id)
    if len(arg) > 0:
        worksheet.update_acell('I'+str(row+2), arg[0])

def updateAction(id, action):
    worksheet = get_scheduler_sheet()
    val = worksheet.get_all_records()
    for x in val:
        try:
            if int(id) == int(x['ID']):
                print "IF ke andar aa gaya"
                rowNum = val.index(x) + 2
                column = 'I'+str(rowNum)
                print column
                worksheet.update_acell(column, str(action))
        except:
            print "Some error came"
