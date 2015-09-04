import httplib2
# Do OAuth2 stuff to create credentials object
from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets
from oauth2client import tools
import gspread
import requests
from job_update_api import update_job_status

_cache = {}             # id against sheet row numbers

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


def get_custom_sheet(*args):
    try:  # Format = cust_<name>_i<index>
        if len(args) > 0:
            idx = int(args[0].split("_")[-1][1:])
            return get_worksheet(5 + idx)
        else:
            return get_worksheet(4)
    except:
        print " >> ERROR: malformed custom sheet parameter, returning base custom"
        return get_worksheet(4)


def get_block_sheet():  ## NOTE: Only to be used by blockList.py
    return get_worksheet(5)


actionAlpha = 'J'
idAlpha = 'K'
linkAlpha = 'L'


def updateId(id, row, *arg, **kwargs):
    """
    Updates the id
    :param id: The created id
    :param row: sheet row index - 2
    :param arg: May contain the starting action
    :param kwargs: May contain 'oid' in which case the api will be notified
    :return:
    """

    sheet_row = row + 2
    print 'inside updateId, ', id, row
    cell = idAlpha + str(sheet_row)

    _cache[id] = sheet_row

    worksheet = get_scheduler_sheet()
    worksheet.update_acell(cell, id)

    if len(arg) > 0:
        worksheet.update_acell(actionAlpha + str(sheet_row), arg[0])
    if 'oid' in kwargs and kwargs['oid'] is not None:
        if len(arg) > 0:
            update_job_status(kwargs['oid'], t_id=id, sheet_row=sheet_row, status=str(arg[0]))
        else:
            update_job_status(kwargs['oid'], t_id=id, sheet_row=sheet_row)


def updateLink(id, link, oid=None):
    updateAux(id, linkAlpha, link)
    if oid is not None:
        update_job_status(oid, file_link=link)


def updateAction(id, action, oid=None):
    """ Update the Action i.e. Status of the job
    :param id: The tool's id for the current job
    :param action: The current status
    :param oid: Present only for external jobs. If present, then we also put out a shout to the api,
                regarding the status
    """
    updateAux(id, actionAlpha, action)
    if oid is not None:
        update_job_status(oid, status=action)


def updateAux(id, col, data):
    worksheet = get_scheduler_sheet()
    if id in _cache and worksheet.acell(idAlpha+str(_cache[id])).value == str(id):
        cell_name = col + str(_cache[id])
        if worksheet.acell(cell_name).value != 'Cancel':
            worksheet.update_acell(cell_name)
    else:
        val = worksheet.get_all_records()
        for x in val:
            try:
                if str(id) == str(x['ID']):         # Here this is the sheet based ID
                    rowNum = val.index(x) + 2

                    _cache[id] = rowNum

                    cell = col + str(rowNum)
                    print cell
                    if x['Action'] != 'Cancel':
                        worksheet.update_acell(cell, str(data))
                    break
            except Exception, e:
                print "Some error came : "+str(e)


def getFileLink(id):
    worksheet = get_scheduler_sheet()
    val = worksheet.get_all_records()
    for x in val:
        if int(id) == int(x['ID']):
            return x['Data Link']
    return ''
