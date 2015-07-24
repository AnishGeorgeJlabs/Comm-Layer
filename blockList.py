from data.sheet import get_block_sheet
import pymongo

dbclient = pymongo.MongoClient("mongodb://45.55.232.5:27017")
col = dbclient.wadi.blocked

sheet = get_block_sheet()
values = sheet.get_all_values()[1:]

for i, value in enumerate(values):
    try:
        if len(value) == 2:
            col.insert_one({"number": value[0], "language": value[1]})
        else:
            continue    # low risk
    except:
        pass
    finally:
        sheet.update_cell(i+2, 1, '')       # clear the data
        sheet.update_cell(i+2, 2, '')
